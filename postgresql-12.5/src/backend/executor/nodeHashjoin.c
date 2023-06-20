/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in several ways.  A
 * parallel-oblivious hash join is one where the node is unaware that it is
 * part of a parallel plan.  In this case, a copy of the inner plan is used to
 * build a copy of the hash table in every backend, and the outer plan could
 * either be built from a partial or complete path, so that the results of the
 * hash join are correspondingly either partial or complete.  A parallel-aware
 * hash join is one that behaves differently, coordinating work between
 * backends, and appears as Parallel Hash Join in EXPLAIN output.  A Parallel
 * Hash Join always appears with a Parallel Hash node.
 *
 * Parallel-aware hash joins use the same per-backend state machine to track
 * progress through the hash join algorithm as parallel-oblivious hash joins.
 * In a parallel-aware hash join, there is also a shared state machine that
 * co-operating backends use to synchronize their local state machines and
 * program counters.  The shared state machine is managed with a Barrier IPC
 * primitive.  When all attached participants arrive at a barrier, the phase
 * advances and all waiting participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * One barrier called build_barrier is used to coordinate the hashing phases.
 * The phase is represented by an integer which begins at zero and increments
 * one by one, but in the code it is referred to by symbolic names as follows:
 *
 *   PHJ_BUILD_ELECTING              -- initial state
 *   PHJ_BUILD_ALLOCATING            -- one sets up the batches and table 0
 *   PHJ_BUILD_HASHING_INNER         -- all hash the inner rel
 *   PHJ_BUILD_HASHING_OUTER         -- (multi-batch only) all hash the outer
 *   PHJ_BUILD_DONE                  -- building done, probing can begin
 *
 * While in the phase PHJ_BUILD_HASHING_INNER a separate pair of barriers may
 * be used repeatedly as required to coordinate expansions in the number of
 * batches or buckets.  Their phases are as follows:
 *
 *   PHJ_GROW_BATCHES_ELECTING       -- initial state
 *   PHJ_GROW_BATCHES_ALLOCATING     -- one allocates new batches
 *   PHJ_GROW_BATCHES_REPARTITIONING -- all repartition
 *   PHJ_GROW_BATCHES_FINISHING      -- one cleans up, detects skew
 *
 *   PHJ_GROW_BUCKETS_ELECTING       -- initial state
 *   PHJ_GROW_BUCKETS_ALLOCATING     -- one allocates new buckets
 *   PHJ_GROW_BUCKETS_REINSERTING    -- all insert tuples
 *
 * If the planner got the number of batches and buckets right, those won't be
 * necessary, but on the other hand we might finish up needing to expand the
 * buckets or batches multiple times while hashing the inner relation to stay
 * within our memory budget and load factor target.  For that reason it's a
 * separate pair of barriers using circular phases.
 *
 * The PHJ_BUILD_HASHING_OUTER phase is required only for multi-batch joins,
 * because we need to divide the outer relation into batches up front in order
 * to be able to process batches entirely independently.  In contrast, the
 * parallel-oblivious algorithm simply throws tuples 'forward' to 'later'
 * batches whenever it encounters them while scanning and probing, which it
 * can do because it processes batches in serial order.
 *
 * Once PHJ_BUILD_DONE is reached, backends then split up and process
 * different batches, or gang up and work together on probing batches if there
 * aren't enough to go around.  For each batch there is a separate barrier
 * with the following phases:
 *
 *  PHJ_BATCH_ELECTING       -- initial state
 *  PHJ_BATCH_ALLOCATING     -- one allocates buckets
 *  PHJ_BATCH_LOADING        -- all load the hash table from disk
 *  PHJ_BATCH_PROBING        -- all probe
 *  PHJ_BATCH_DONE           -- end
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_PROBING; populating batch 0's hash table is done during
 * PHJ_BUILD_HASHING_INNER so we can skip loading.
 *
 * Initially we try to plan for a single-batch hash join using the combined
 * work_mem of all participants to create a large shared hash table.  If that
 * turns out either at planning or execution time to be impossible then we
 * fall back to regular work_mem sized hash tables.
 *
 * To avoid deadlocks, we never wait for any barrier unless it is known that
 * all other backends attached to it are actively executing the node or have
 * already arrived.  Practically, that means that we never return a tuple
 * while attached to a barrier, unless the barrier has reached its final
 * state.  In the slightly special case of the per-batch barrier, we return
 * tuples while in PHJ_BATCH_PROBING phase, but that's OK because we use
 * BarrierArriveAndDetach() to advance it to PHJ_BATCH_DONE without waiting.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"

/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE 1
#define HJ_NEED_NEW_INNER 2
#define HJ_SCAN_OUTER_BUCKET 3
#define HJ_NEED_NEW_OUTER 4
#define HJ_SCAN_INNER_BUCKET 5
#define HJ_FILL_TUPLES 6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate) ((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate) ((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
                                                 HashJoinState *hjstate,
                                                 uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinInnerGetTuple(PlanState *innerNode,
                                                 HashJoinState *hjstate,
                                                 uint32 *hashvalue);
static TupleTableSlot *ExecParallelHashJoinOuterGetTuple(PlanState *outerNode,
                                                         HashJoinState *hjstate,
                                                         uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
                                                 BufFile *file,
                                                 uint32 *hashvalue,
                                                 TupleTableSlot *tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState *hjstate);
static bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate);
static void ExecParallelHashJoinPartitionOuter(HashJoinState *node);

/* ----------------------------------------------------------------
 *		ExecHashJoinImpl
 *
 *		This function implements the Hybrid Hashjoin algorithm.  It is marked
 *		with an always-inline attribute so that ExecHashJoin() and
 *		ExecParallelHashJoin() can inline it.  Compilers that respect the
 *		attribute should create versions specialized for parallel == true and
 *		parallel == false with unnecessary branches removed.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
{
    elog(NOTICE, "impl start");
    HashJoinState *node = castNode(HashJoinState, pstate);
    HashState *hashNode_outer;
    HashState *hashNode_inner;
    ExprState *joinqual;
    ExprState *otherqual;
    ExprContext *econtext;
    HashJoinTable hashtable_inner;
    HashJoinTable hashtable_outer;
    TupleTableSlot *outerTupleSlot;
    TupleTableSlot *innerTupleSlot;
    uint32 hashvalue_inner;
    uint32 hashvalue_outer;
    int batchno;
    ParallelHashJoinState *parallel_state;

    /*
     * get information from HashJoin node
     */
    joinqual = node->js.joinqual;
    otherqual = node->js.ps.qual;
    hashNode_inner = (HashState *)innerPlanState(node);
    hashNode_outer = (HashState *)outerPlanState(node);
    hashtable_inner = node->hj_HashTable_inner;
    hashtable_outer = node->hj_HashTable_outer;

    econtext = node->js.ps.ps_ExprContext;
    parallel_state = hashNode_inner->parallel_state;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * run the hash join state machine
     */

    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        switch (node->hj_JoinState)
        {
        case HJ_BUILD_HASHTABLE:
            Assert(hashtable == NULL);

            hashtable_inner =
                ExecHashTableCreate(hashNode_inner, node->hj_HashOperators,
                                    node->hj_Collations, HJ_FILL_INNER(node));
            hashtable_outer =
                ExecHashTableCreate(hashNode_outer, node->hj_HashOperators,
                                    node->hj_Collations, HJ_FILL_OUTER(node));
            node->hj_HashTable_inner = hashtable_inner;
            node->hj_HashTable_outer = hashtable_outer;

            hashNode_inner->hashtable = hashtable_inner;
            hashNode_outer->hashtable = hashtable_outer;

            // (void)MultiExecHash(hashNode_inner);
            // (void)MultiExecHash(hashNode_outer);

            node->hj_JoinState = HJ_NEED_NEW_OUTER;
            continue;

        case HJ_NEED_NEW_INNER:
            elog(NOTICE, "try get inner tuple");
            if (node->hj_InnerNotEmpty)
                innerTupleSlot = ExecHashJoinInnerGetTuple(
                    (PlanState *)hashNode_inner, node, &hashvalue_inner);
            else
            {
                if (node->hj_OuterNotEmpty){
                    node->hj_JoinState = HJ_NEED_NEW_OUTER;
                    continue;
                }
                else
                    return NULL;
            }
            // elog(NOTICE, "get inner tuple");

            if (TupIsNull(innerTupleSlot))
            {
                //暂不考虑其他类型
                // elog(NOTICE, "inner tuple slot is null");
                // inner join end
                node->hj_InnerNotEmpty = false;
                if (node->hj_OuterNotEmpty)
                {
                    node->hj_JoinState = HJ_NEED_NEW_OUTER;
                    continue;
                }
                //忽略FILL return
                else
                    return NULL;
            }
            else
                elog(NOTICE, "inner tuple not null");
            econtext->ecxt_innertuple = innerTupleSlot;
            node->hj_MatchedInner = false;

            node->hj_CurHashValue_inner = hashvalue_inner;
            ExecHashGetBucketAndBatch(hashtable_inner, hashvalue_inner,
                                      &node->hj_CurBucketNo_inner, &batchno);
            node->hj_CurTuple_inner = NULL;
            elog(NOTICE, "get inner success");
            node->hj_JoinState = HJ_SCAN_OUTER_BUCKET;
            continue;

        case HJ_SCAN_OUTER_BUCKET:
            elog(NOTICE,"scan outer");
            if (!ExecScanHashBucket(node, econtext, 2))
            {
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                elog(NOTICE,"not found outer");
                continue;
            }

            if (joinqual == NULL || ExecQual(joinqual, econtext))
            {
                node->hj_MatchedInner = true;

                node->hj_JoinState = HJ_NEED_NEW_OUTER;

                if (otherqual == NULL || ExecQual(otherqual, econtext))
                {
                    // econtext->ecxt_innertuple = innerTupleSlot;
                    elog(NOTICE, "project inner");
                    return ExecProject(node->js.ps.ps_ProjInfo);
                }
                else
                    InstrCountFiltered2(node, 1);
            }
            else
                InstrCountFiltered1(node, 1);
            break;

        case HJ_NEED_NEW_OUTER:
            elog(NOTICE, "try get outer tuple");
            if (node->hj_OuterNotEmpty)
                outerTupleSlot = ExecHashJoinOuterGetTuple(
                    (PlanState *)hashNode_outer, node, &hashvalue_outer);
            else
            {
                if (node->hj_InnerNotEmpty){
                    node->hj_JoinState = HJ_NEED_NEW_INNER;
                    continue;
                }
                else
                    return NULL;
            }
            // elog(NOTICE, "get outer tuple");

            if (TupIsNull(outerTupleSlot))
            {
                //暂不考虑其他类型
                // elog(NOTICE, "outer tuple slot is null");
                node->hj_OuterNotEmpty = false;
                if (node->hj_InnerNotEmpty)
                {
                    node->hj_JoinState = HJ_NEED_NEW_INNER;
                    continue;
                }
                //忽略FILL return
                // inner join end
                else
                    return NULL;
            }
            else
                elog(NOTICE, "outer tuple  not null");
            econtext->ecxt_outertuple = outerTupleSlot;
            node->hj_MatchedOuter = false;

            node->hj_CurHashValue_outer = hashvalue_outer;
            ExecHashGetBucketAndBatch(hashtable_outer, hashvalue_outer,
                                      &node->hj_CurBucketNo_outer, &batchno);
            node->hj_CurTuple_outer = NULL;
            elog(NOTICE, "get outer success");
            node->hj_JoinState = HJ_SCAN_INNER_BUCKET;
            continue;

        case HJ_SCAN_INNER_BUCKET:
            elog(NOTICE, "scan inner");
            if (!ExecScanHashBucket(node, econtext, 1))
            {
                node->hj_JoinState = HJ_NEED_NEW_INNER;
                elog(NOTICE, "not found inner");
                continue;
            }

            if (joinqual == NULL || ExecQual(joinqual, econtext))
            {
                node->hj_MatchedOuter = true;

                node->hj_JoinState = HJ_NEED_NEW_INNER;

                if (otherqual == NULL || ExecQual(otherqual, econtext))
                {
                    // econtext->ecxt_outertuple = outerTupleSlot;
                    elog(NOTICE, "project outer");
                    return ExecProject(node->js.ps.ps_ProjInfo);
                }
                else
                    InstrCountFiltered2(node, 1);
            }
            else
                InstrCountFiltered1(node, 1);
            break;

        case HJ_FILL_TUPLES:
            return NULL;
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot * /* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
    /*
     * On sufficiently smart compilers this should be inlined with the
     * parallel-aware branches removed.
     */
    return ExecHashJoinImpl(pstate, false);
}

/* ----------------------------------------------------------------
 *		ExecParallelHashJoin
 *
 *		Parallel-aware version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot * /* return: a tuple or NULL */
ExecParallelHashJoin(PlanState *pstate)
{
    /*
     * On sufficiently smart compilers this should be inlined with the
     * parallel-oblivious branches removed.
     */
    return ExecHashJoinImpl(pstate, true);
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
    HashJoinState *hjstate;
    // Plan	   *outerNode;
    Hash *hashNodeOuter;
    Hash *hashNodeInner;
    TupleDesc outerDesc, innerDesc;
    const TupleTableSlotOps *ops_outer, *ops_inner;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    hjstate = makeNode(HashJoinState);
    hjstate->js.ps.plan = (Plan *)node;
    hjstate->js.ps.state = estate;

    /*
     * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
     * where this function may be replaced with a parallel version, if we
     * managed to launch a parallel query.
     */
    hjstate->js.ps.ExecProcNode = ExecHashJoin;
    // hjstate->js.jointype = node->join.jointype;
    hjstate->js.jointype = JOIN_INNER;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &hjstate->js.ps);

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    hashNodeOuter = (Hash *)outerPlan(node);
    hashNodeInner = (Hash *)innerPlan(node);

    outerPlanState(hjstate) =
        ExecInitNode((Plan *)hashNodeOuter, estate, eflags);
    outerDesc = ExecGetResultType(outerPlanState(hjstate));
    innerPlanState(hjstate) =
        ExecInitNode((Plan *)hashNodeInner, estate, eflags);
    innerDesc = ExecGetResultType(innerPlanState(hjstate));

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

    /*
     * tuple table initialization
     */
    ops_outer = ExecGetResultSlotOps(outerPlanState(hjstate), NULL);
    ops_inner = ExecGetResultSlotOps(innerPlanState(hjstate), NULL);

    hjstate->hj_OuterTupleSlot =
        ExecInitExtraTupleSlot(estate, outerDesc, ops_outer);
    hjstate->hj_InnerTupleSlot =
        ExecInitExtraTupleSlot(estate, innerDesc, ops_inner);

    /*
     * detect whether we need only consider the first matching inner tuple
     */
    hjstate->js.single_match =
        (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

    /* set up null tuples for outer joins, if needed */
    node->join.jointype = JOIN_INNER;
    switch (node->join.jointype)
    {
    case JOIN_INNER:
    case JOIN_SEMI:
        break;
    case JOIN_LEFT:
    case JOIN_ANTI:
        hjstate->hj_NullInnerTupleSlot =
            ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
        break;
    case JOIN_RIGHT:
        hjstate->hj_NullOuterTupleSlot =
            ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
        break;
    case JOIN_FULL:
        hjstate->hj_NullOuterTupleSlot =
            ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
        hjstate->hj_NullInnerTupleSlot =
            ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
        break;
    default:
        elog(ERROR, "unrecognized join type: %d", (int)node->join.jointype);
    }

    /*
     * now for some voodoo.  our temporary tuple slot is actually the result
     * tuple slot of the Hash node (which is our inner plan).  we can do this
     * because Hash nodes don't return tuples via ExecProcNode() -- instead
     * the hash join node uses ExecScanHashBucket() to get at the contents of
     * the hash table.  -cim 6/9/91
     */
    { //获取
        HashState *hashstate_inner = (HashState *)innerPlanState(hjstate);
        HashState *hashstate_outer = (HashState *)outerPlanState(hjstate);
        TupleTableSlot *slot_inner = hashstate_inner->ps.ps_ResultTupleSlot;
        TupleTableSlot *slot_outer = hashstate_outer->ps.ps_ResultTupleSlot;

        hjstate->hj_HashTupleSlot_inner = slot_inner;
        hjstate->hj_HashTupleSlot_outer = slot_outer;
    }

    /*
     * initialize child expressions
     */
    hjstate->js.ps.qual =
        ExecInitQual(node->join.plan.qual, (PlanState *)hjstate);
    hjstate->js.joinqual =
        ExecInitQual(node->join.joinqual, (PlanState *)hjstate);
    hjstate->hashclauses =
        ExecInitQual(node->hashclauses, (PlanState *)hjstate);

    /*
     * initialize hash-specific info
     */
    hjstate->hj_HashTable_inner = NULL;
    hjstate->hj_HashTable_outer = NULL;
    hjstate->hj_FirstOuterTupleSlot = NULL;
    hjstate->hj_FirstInnerTupleSlot = NULL;

    hjstate->hj_CurHashValue_inner = 0;
    hjstate->hj_CurHashValue_outer = 0;
    hjstate->hj_CurBucketNo_inner = 0;
    hjstate->hj_CurBucketNo_outer = 0;
    hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    hjstate->hj_CurTuple_inner = NULL;
    hjstate->hj_CurTuple_outer = NULL;

    hjstate->hj_OuterHashKeys =
        ExecInitExprList(node->hashkeys, (PlanState *)hjstate);
    hjstate->hj_InnerHashKeys =
        ExecInitExprList(node->hashkeys, (PlanState *)hjstate);
    hjstate->hj_HashOperators = node->hashoperators;
    hjstate->hj_Collations = node->hashcollations;

    hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
    hjstate->hj_MatchedOuter = false;
    hjstate->hj_MatchedInner = false;
    hjstate->hj_OuterNotEmpty = true;
    hjstate->hj_InnerNotEmpty = true;

    return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
    /*
     * Free hash table
     */
    if (node->hj_HashTable_inner)
    {
        ExecHashTableDestroy(node->hj_HashTable_inner);
        node->hj_HashTable_inner = NULL;
    }
    if (node->hj_HashTable_outer)
    {
        ExecHashTableDestroy(node->hj_HashTable_outer);
        node->hj_HashTable_outer = NULL;
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    ExecClearTuple(node->hj_OuterTupleSlot);
    if (node->hj_InnerTupleSlot)
        ExecClearTuple(node->hj_InnerTupleSlot);
    ExecClearTuple(node->hj_HashTupleSlot_inner);
    ExecClearTuple(node->hj_HashTupleSlot_outer);

    /*
     * clean up subtrees
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

static TupleTableSlot *
ExecHashJoinInnerGetTuple(PlanState *innerNode, HashJoinState *hjstate,
                          uint32 *hashvalue)
{
    elog(NOTICE, "get inner tuple");
    HashJoinTable hashtable = hjstate->hj_HashTable_inner;
    TupleTableSlot *slot;

    slot = hjstate->hj_FirstInnerTupleSlot;
    if (!TupIsNull(slot))
        hjstate->hj_FirstInnerTupleSlot = NULL;
    else
        slot = ExecProcNode(innerNode);

    elog(NOTICE, "before inner get tuple!!!");
    while (!TupIsNull(slot))
    {
        ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

        econtext->ecxt_innertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext, hjstate->hj_InnerHashKeys,
                                 false, HJ_FILL_INNER(hjstate), hashvalue))
        {
            hjstate->hj_InnerNotEmpty = true;
            elog(NOTICE, "get inner tuple return %p", slot);
            return slot;
        }
        slot = ExecProcNode(innerNode);
    }

    return NULL;
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for a parallel oblivious hashjoin: either by
 *		executing the outer plan node in the first pass, or from the temp
 *		files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode, HashJoinState *hjstate,
                          uint32 *hashvalue)
{
    elog(NOTICE, "get outer tuple");
    HashJoinTable hashtable = hjstate->hj_HashTable_outer;
    TupleTableSlot *slot;

    slot = hjstate->hj_FirstOuterTupleSlot;
    if (!TupIsNull(slot))
        hjstate->hj_FirstOuterTupleSlot = NULL;
    else
        slot = ExecProcNode(outerNode);

    elog(NOTICE,"outer get tuple!!!");
    while (!TupIsNull(slot))
    {
        ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

        econtext->ecxt_outertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext, hjstate->hj_OuterHashKeys,
                                 true, HJ_FILL_OUTER(hjstate), hashvalue))
        {
            hjstate->hj_OuterNotEmpty = true;
            elog(NOTICE, "get outer tuple return %p", slot);
            return slot;
        }
        slot = ExecProcNode(outerNode);
    }

    return NULL;
}

/*
 * ExecHashJoinOuterGetTuple variant for the parallel case.
 */
static TupleTableSlot *
ExecParallelHashJoinOuterGetTuple(PlanState *outerNode, HashJoinState *hjstate,
                                  uint32 *hashvalue)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int curbatch = hashtable->curbatch;
    TupleTableSlot *slot;

    /*
     * In the Parallel Hash case we only run the outer plan directly for
     * single-batch hash joins.  Otherwise we have to go to batch files, even
     * for batch 0.
     */
    if (curbatch == 0 && hashtable->nbatch == 1)
    {
        slot = ExecProcNode(outerNode);

        while (!TupIsNull(slot))
        {
            ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

            econtext->ecxt_outertuple = slot;
            if (ExecHashGetHashValue(hashtable, econtext,
                                     hjstate->hj_OuterHashKeys,
                                     true, /* outer tuple */
                                     HJ_FILL_OUTER(hjstate), hashvalue))
                return slot;

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    }
    else if (curbatch < hashtable->nbatch)
    {
        MinimalTuple tuple;

        tuple = sts_parallel_scan_next(
            hashtable->batches[curbatch].outer_tuples, hashvalue);
        if (tuple != NULL)
        {
            ExecForceStoreMinimalTuple(tuple, hjstate->hj_OuterTupleSlot,
                                       false);
            slot = hjstate->hj_OuterTupleSlot;
            return slot;
        }
        else
            ExecClearTuple(hjstate->hj_OuterTupleSlot);
    }

    /* End of this batch */
    return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int nbatch;
    int curbatch;
    BufFile *innerFile;
    TupleTableSlot *slot;
    uint32 hashvalue;

    nbatch = hashtable->nbatch;
    curbatch = hashtable->curbatch;

    if (curbatch > 0)
    {
        /*
         * We no longer need the previous outer batch file; close it right
         * away to free disk space.
         */
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
    }
    else /* we just finished the first batch */
    {
        /*
         * Reset some of the skew optimization state variables, since we no
         * longer need to consider skew tuples after the first batch. The
         * memory context reset we are about to do will release the skew
         * hashtable itself.
         */
        hashtable->skewEnabled = false;
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->nSkewBuckets = 0;
        hashtable->spaceUsedSkew = 0;
    }

    /*
     * We can always skip over any batches that are completely empty on both
     * sides.  We can sometimes skip over batches that are empty on only one
     * side, but there are exceptions:
     *
     * 1. In a left/full outer join, we have to process outer batches even if
     * the inner batch is empty.  Similarly, in a right/full outer join, we
     * have to process inner batches even if the outer batch is empty.
     *
     * 2. If we have increased nbatch since the initial estimate, we have to
     * scan inner batches since they might contain tuples that need to be
     * reassigned to later inner batches.
     *
     * 3. Similarly, if we have increased nbatch since starting the outer
     * scan, we have to rescan outer batches in case they contain tuples that
     * need to be reassigned.
     */
    curbatch++;
    while (curbatch < nbatch && (hashtable->outerBatchFile[curbatch] == NULL ||
                                 hashtable->innerBatchFile[curbatch] == NULL))
    {
        if (hashtable->outerBatchFile[curbatch] && HJ_FILL_OUTER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] && HJ_FILL_INNER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_original)
            break; /* must process due to rule 2 */
        if (hashtable->outerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_outstart)
            break; /* must process due to rule 3 */
        /* We can ignore this batch. */
        /* Release associated temp files right away. */
        if (hashtable->innerBatchFile[curbatch])
            BufFileClose(hashtable->innerBatchFile[curbatch]);
        hashtable->innerBatchFile[curbatch] = NULL;
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        curbatch++;
    }

    if (curbatch >= nbatch)
        return false; /* no more batches */

    hashtable->curbatch = curbatch;

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashtable);

    innerFile = hashtable->innerBatchFile[curbatch];

    if (innerFile != NULL)
    {
        if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not rewind hash-join temporary file")));

        while ((slot = ExecHashJoinGetSavedTuple(hjstate, innerFile, &hashvalue,
                                                 hjstate->hj_HashTupleSlot)))
        {
            /*
             * NOTE: some tuples may be sent to future batches.  Also, it is
             * possible for hashtable->nbatch to be increased here!
             */
            ExecHashTableInsert(hashtable, slot, hashvalue);
        }

        /*
         * after we build the hash table, the inner batch file is no longer
         * needed
         */
        BufFileClose(innerFile);
        hashtable->innerBatchFile[curbatch] = NULL;
    }

    /*
     * Rewind outer batch file (if present), so that we can start reading it.
     */
    if (hashtable->outerBatchFile[curbatch] != NULL)
    {
        if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not rewind hash-join temporary file")));
    }

    return true;
}

/*
 * Choose a batch to work on, and attach to it.  Returns true if successful,
 * false if there are no more batches.
 */
static bool
ExecParallelHashJoinNewBatch(HashJoinState *hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int start_batchno;
    int batchno;

    /*
     * If we started up so late that the batch tracking array has been freed
     * already by ExecHashTableDetach(), then we are finished.  See also
     * ExecParallelHashEnsureBatchAccessors().
     */
    if (hashtable->batches == NULL)
        return false;

    /*
     * If we were already attached to a batch, remember not to bother checking
     * it again, and detach from it (possibly freeing the hash table if we are
     * last to detach).
     */
    if (hashtable->curbatch >= 0)
    {
        hashtable->batches[hashtable->curbatch].done = true;
        ExecHashTableDetachBatch(hashtable);
    }

    /*
     * Search for a batch that isn't done.  We use an atomic counter to start
     * our search at a different batch in every participant when there are
     * more batches than participants.
     */
    batchno = start_batchno =
        pg_atomic_fetch_add_u32(&hashtable->parallel_state->distributor, 1) %
        hashtable->nbatch;
    do
    {
        uint32 hashvalue;
        MinimalTuple tuple;
        TupleTableSlot *slot;

        if (!hashtable->batches[batchno].done)
        {
            SharedTuplestoreAccessor *inner_tuples;
            Barrier *batch_barrier =
                &hashtable->batches[batchno].shared->batch_barrier;

            switch (BarrierAttach(batch_barrier))
            {
            case PHJ_BATCH_ELECTING:

                /* One backend allocates the hash table. */
                if (BarrierArriveAndWait(batch_barrier,
                                         WAIT_EVENT_HASH_BATCH_ELECTING))
                    ExecParallelHashTableAlloc(hashtable, batchno);
                /* Fall through. */

            case PHJ_BATCH_ALLOCATING:
                /* Wait for allocation to complete. */
                BarrierArriveAndWait(batch_barrier,
                                     WAIT_EVENT_HASH_BATCH_ALLOCATING);
                /* Fall through. */

            case PHJ_BATCH_LOADING:
                /* Start (or join in) loading tuples. */
                ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                inner_tuples = hashtable->batches[batchno].inner_tuples;
                sts_begin_parallel_scan(inner_tuples);
                while (
                    (tuple = sts_parallel_scan_next(inner_tuples, &hashvalue)))
                {
                    ExecForceStoreMinimalTuple(tuple, hjstate->hj_HashTupleSlot,
                                               false);
                    slot = hjstate->hj_HashTupleSlot;
                    ExecParallelHashTableInsertCurrentBatch(hashtable, slot,
                                                            hashvalue);
                }
                sts_end_parallel_scan(inner_tuples);
                BarrierArriveAndWait(batch_barrier,
                                     WAIT_EVENT_HASH_BATCH_LOADING);
                /* Fall through. */

            case PHJ_BATCH_PROBING:

                /*
                 * This batch is ready to probe.  Return control to
                 * caller. We stay attached to batch_barrier so that the
                 * hash table stays alive until everyone's finished
                 * probing it, but no participant is allowed to wait at
                 * this barrier again (or else a deadlock could occur).
                 * All attached participants must eventually call
                 * BarrierArriveAndDetach() so that the final phase
                 * PHJ_BATCH_DONE can be reached.
                 */
                ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                sts_begin_parallel_scan(
                    hashtable->batches[batchno].outer_tuples);
                return true;

            case PHJ_BATCH_DONE:

                /*
                 * Already done.  Detach and go around again (if any
                 * remain).
                 */
                BarrierDetach(batch_barrier);
                hashtable->batches[batchno].done = true;
                hashtable->curbatch = -1;
                break;

            default:
                elog(ERROR, "unexpected batch phase %d",
                     BarrierPhase(batch_barrier));
            }
        }
        batchno = (batchno + 1) % hashtable->nbatch;
    } while (batchno != start_batchno);

    return false;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile **fileptr)
{
    BufFile *file = *fileptr;

    if (file == NULL)
    {
        /* First write to this batch file, so open it. */
        file = BufFileCreateTemp(false);
        *fileptr = file;
    }

    BufFileWrite(file, (void *)&hashvalue, sizeof(uint32));
    BufFileWrite(file, (void *)tuple, tuple->t_len);
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate, BufFile *file,
                          uint32 *hashvalue, TupleTableSlot *tupleSlot)
{
    uint32 header[2];
    size_t nread;
    MinimalTuple tuple;

    /*
     * We check for interrupts here because this is typically taken as an
     * alternative code path to an ExecProcNode() call, which would include
     * such a check.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * Since both the hash value and the MinimalTuple length word are uint32,
     * we can read them both in one BufFileRead() call without any type
     * cheating.
     */
    nread = BufFileRead(file, (void *)header, sizeof(header));
    if (nread == 0) /* end of file */
    {
        ExecClearTuple(tupleSlot);
        return NULL;
    }
    if (nread != sizeof(header))
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not read from hash-join temporary file: "
                               "read only %zu of %zu bytes",
                               nread, sizeof(header))));
    *hashvalue = header[0];
    tuple = (MinimalTuple)palloc(header[1]);
    tuple->t_len = header[1];
    nread = BufFileRead(file, (void *)((char *)tuple + sizeof(uint32)),
                        header[1] - sizeof(uint32));
    if (nread != header[1] - sizeof(uint32))
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not read from hash-join temporary file: "
                               "read only %zu of %zu bytes",
                               nread, header[1] - sizeof(uint32))));
    ExecForceStoreMinimalTuple(tuple, tupleSlot, true);
    return tupleSlot;
}

void
ExecReScanHashJoin(HashJoinState *node)
{
    /*
     * In a multi-batch join, we currently have to do rescans the hard way,
     * primarily because batch temp files may have already been released. But
     * if it's a single-batch join, and there is no parameter change for the
     * inner subnode, then we can just re-use the existing hash table without
     * rebuilding it.
     */
    if (node->hj_HashTable != NULL)
    {
        if (node->hj_HashTable->nbatch == 1 &&
            node->js.ps.righttree->chgParam == NULL)
        {
            /*
             * Okay to reuse the hash table; needn't rescan inner, either.
             *
             * However, if it's a right/full join, we'd better reset the
             * inner-tuple match flags contained in the table.
             */
            if (HJ_FILL_INNER(node))
                ExecHashTableResetMatchFlags(node->hj_HashTable);

            /*
             * Also, we need to reset our state about the emptiness of the
             * outer relation, so that the new scan of the outer will update
             * it correctly if it turns out to be empty this time. (There's no
             * harm in clearing it now because ExecHashJoin won't need the
             * info.  In the other cases, where the hash table doesn't exist
             * or we are destroying it, we leave this state alone because
             * ExecHashJoin will need it the first time through.)
             */
            node->hj_OuterNotEmpty = false;

            /* ExecHashJoin can skip the BUILD_HASHTABLE step */
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        }
        else
        {
            /* must destroy and rebuild hash table */
            HashState *hashNode = castNode(HashState, innerPlanState(node));

            /* for safety, be sure to clear child plan node's pointer too */
            Assert(hashNode->hashtable == node->hj_HashTable);
            hashNode->hashtable = NULL;

            ExecHashTableDestroy(node->hj_HashTable);
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            /*
             * if chgParam of subnode is not null then plan will be re-scanned
             * by first ExecProcNode.
             */
            if (node->js.ps.righttree->chgParam == NULL)
                ExecReScan(node->js.ps.righttree);
        }
    }

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReScan(node->js.ps.lefttree);
}

void
ExecShutdownHashJoin(HashJoinState *node)
{
    if (node->hj_HashTable)
    {
        /*
         * Detach from shared state before DSM memory goes away.  This makes
         * sure that we don't have any pointers into DSM memory by the time
         * ExecEndHashJoin runs.
         */
        ExecHashTableDetachBatch(node->hj_HashTable);
        ExecHashTableDetach(node->hj_HashTable);
    }
}

static void
ExecParallelHashJoinPartitionOuter(HashJoinState *hjstate)
{
    PlanState *outerState = outerPlanState(hjstate);
    ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
    HashJoinTable hashtable = hjstate->hj_HashTable;
    TupleTableSlot *slot;
    uint32 hashvalue;
    int i;

    Assert(hjstate->hj_FirstOuterTupleSlot == NULL);

    /* Execute outer plan, writing all tuples to shared tuplestores. */
    for (;;)
    {
        slot = ExecProcNode(outerState);
        if (TupIsNull(slot))
            break;
        econtext->ecxt_outertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext, hjstate->hj_OuterHashKeys,
                                 true, /* outer tuple */
                                 HJ_FILL_OUTER(hjstate), &hashvalue))
        {
            int batchno;
            int bucketno;
            bool shouldFree;
            MinimalTuple mintup = ExecFetchSlotMinimalTuple(slot, &shouldFree);

            ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno,
                                      &batchno);
            sts_puttuple(hashtable->batches[batchno].outer_tuples, &hashvalue,
                         mintup);

            if (shouldFree)
                heap_free_minimal_tuple(mintup);
        }
        CHECK_FOR_INTERRUPTS();
    }

    /* Make sure all outer partitions are readable by any backend. */
    for (i = 0; i < hashtable->nbatch; ++i)
        sts_end_write(hashtable->batches[i].outer_tuples);
}

void
ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
{
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
    shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)
{
    int plan_node_id = state->js.ps.plan->plan_node_id;
    HashState *hashNode;
    ParallelHashJoinState *pstate;

    /*
     * Disable shared hash table mode if we failed to create a real DSM
     * segment, because that means that we don't have a DSA area to work with.
     */
    if (pcxt->seg == NULL)
        return;

    ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);

    /*
     * Set up the state needed to coordinate access to the shared hash
     * table(s), using the plan node ID as the toc key.
     */
    pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
    shm_toc_insert(pcxt->toc, plan_node_id, pstate);

    /*
     * Set up the shared hash join state with no batches initially.
     * ExecHashTableCreate() will prepare at least one later and set nbatch
     * and space_allowed.
     */
    pstate->nbatch = 0;
    pstate->space_allowed = 0;
    pstate->batches = InvalidDsaPointer;
    pstate->old_batches = InvalidDsaPointer;
    pstate->nbuckets = 0;
    pstate->growth = PHJ_GROWTH_OK;
    pstate->chunk_work_queue = InvalidDsaPointer;
    pg_atomic_init_u32(&pstate->distributor, 0);
    pstate->nparticipants = pcxt->nworkers + 1;
    pstate->total_tuples = 0;
    LWLockInitialize(&pstate->lock, LWTRANCHE_PARALLEL_HASH_JOIN);
    BarrierInit(&pstate->build_barrier, 0);
    BarrierInit(&pstate->grow_batches_barrier, 0);
    BarrierInit(&pstate->grow_buckets_barrier, 0);

    /* Set up the space we'll use for shared temporary files. */
    SharedFileSetInit(&pstate->fileset, pcxt->seg);

    /* Initialize the shared state in the hash node. */
    hashNode = (HashState *)innerPlanState(state);
    hashNode->parallel_state = pstate;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *cxt)
{
    int plan_node_id = state->js.ps.plan->plan_node_id;
    ParallelHashJoinState *pstate =
        shm_toc_lookup(cxt->toc, plan_node_id, false);

    /*
     * It would be possible to reuse the shared hash table in single-batch
     * cases by resetting and then fast-forwarding build_barrier to
     * PHJ_BUILD_DONE and batch 0's batch_barrier to PHJ_BATCH_PROBING, but
     * currently shared hash tables are already freed by now (by the last
     * participant to detach from the batch).  We could consider keeping it
     * around for single-batch joins.  We'd also need to adjust
     * finalize_plan() so that it doesn't record a dummy dependency for
     * Parallel Hash nodes, preventing the rescan optimization.  For now we
     * don't try.
     */

    /* Detach, freeing any remaining shared memory. */
    if (state->hj_HashTable != NULL)
    {
        ExecHashTableDetachBatch(state->hj_HashTable);
        ExecHashTableDetach(state->hj_HashTable);
    }

    /* Clear any shared batch files. */
    SharedFileSetDeleteAll(&pstate->fileset);

    /* Reset build_barrier to PHJ_BUILD_ELECTING so we can go around again. */
    BarrierInit(&pstate->build_barrier, 0);
}

void
ExecHashJoinInitializeWorker(HashJoinState *state, ParallelWorkerContext *pwcxt)
{
    HashState *hashNode;
    int plan_node_id = state->js.ps.plan->plan_node_id;
    ParallelHashJoinState *pstate =
        shm_toc_lookup(pwcxt->toc, plan_node_id, false);

    /* Attach to the space for shared temporary files. */
    SharedFileSetAttach(&pstate->fileset, pwcxt->seg);

    /* Attach to the shared state in the hash node. */
    hashNode = (HashState *)innerPlanState(state);
    hashNode->parallel_state = pstate;

    ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
}

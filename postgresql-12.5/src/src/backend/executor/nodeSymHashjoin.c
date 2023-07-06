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

//可以自定义自己的执行逻辑，以下仅供参考
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_NEED_NEW_INNER       3
#define HJ_FILL_TUPLES          4
#define HJ_SCAN_OUTER_BUCKET    5
#define HJ_SCAN_INNER_BUCKET    6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)


static TupleTableSlot *			/* return: a tuple or NULL */
ExecSymHashJoin(PlanState *pstate)	//在此处添加你的实现，可以参照ExecHashJoin
{
	HashJoinState *node = castNode(HashJoinState, pstate);  
	PlanState  *outerNode;
	PlanState  *innerNode;
	HashState  *innerHashNode;
	HashState  *outerHashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ExprContext *outerNode_econtext;
	
	HashJoinTable innerHashtable; //内hash表
	HashJoinTable outerHashtable; //外hash表
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	uint32		hashvalue;
	uint32 		outerHashValue;
	int         bucketno;
	int 		batchno;
	int         fakeBatchno;
	ParallelHashJoinState *parallel_state;
 
	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	innerHashNode = (HashState *) innerPlanState(node);
	outerHashNode = (HashState *) outerPlanState(node);
	// innerNode = innerPlanState(innerHashNode);
	// outerNode = outerPlanState(outerHashNode);
	innerHashtable = node->hj_HashTable;
	outerHashtable = node->hj_outerHashTable;
	econtext = node->js.ps.ps_ExprContext;
	parallel_state = innerHashNode->parallel_state;
 
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
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();
 
		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:
 
 
				Assert(innerhashtable == NULL);
				Assert(outerhashtable == NULL);
				/*
				 * Create the hash table.  If using Parallel Hash, then
				 * whoever gets here first will create the hash table and any
				 * later arrivals will merely attach to it.
				 */
				innerHashtable = ExecHashTableCreate(innerHashNode,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_INNER(node));
				outerHashtable = ExecHashTableCreate(outerHashNode,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_OUTER(node));
				node->hj_HashTable = innerHashtable;
				node->hj_outerHashTable = outerHashtable;
 
				innerHashNode->hashtable = innerHashtable;
				outerHashNode->hashtable = outerHashtable;
 
 
				innerHashtable->nbatch_outstart = innerHashtable->nbatch;
				outerHashtable->nbatch_outstart = outerHashtable->nbatch;
 
				node->hj_JoinState = HJ_NEED_NEW_INNER;
				break;
 
				/* FALL THRU */
			
			case HJ_NEED_NEW_INNER:
				if(node->innerTupleNull){
					if(node->outerTupleNull){
						node->hj_JoinState = HJ_FILL_TUPLES;
						continue;
					}else{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}
				}
				innerTupleSlot = ExecProcNode((PlanState *)innerHashNode); //从内部表中取出一个元组，探测hash值后压入内部hash表对应的位置
				if (TupIsNull(innerTupleSlot)) //如果取出的元组为空，由于不考虑多批次，因此代表内部表取完了
				{
					node->innerTupleNull = true;
					if (!node->outerTupleNull){
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}else if (HJ_FILL_INNER(node)||HJ_FILL_OUTER(node)){
						node->hj_JoinState = HJ_FILL_TUPLES;
						continue;
					}else {
						return NULL;
					}
				}
				if	(innerHashNode->insertTupleValueEqulNull){ //取出的元组不为空，但是值为null，并且由于不是右外连接，该元组没有被插入innerhash表中
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					continue;
				}
				econtext->ecxt_outertuple = innerTupleSlot; 
				if(ExecHashGetHashValue(outerHashtable, econtext,innerHashNode->hashkeys,true,false,&hashvalue)){ //求出当前取出的内部元组在外部hash表中的hash值，因为保留为null的元组没有意义，所以参数设置为false
					econtext->ecxt_innertuple = innerTupleSlot;//应该是保存一下内部元组之后用来投影
					node->hj_CurOutHashValue = hashvalue;
					ExecHashGetBucketAndBatch(outerHashtable, hashvalue,
            	                               &node->hj_CurBucketNo, &batchno);
					node->hj_CurOutTuple = NULL; //用于scan_bucket时外部hash表中hash槽的探测，如果为null，说明该hash值对应的hash槽刚开始探测，如果不为null，说明之前探测到了一个匹配项，需要继续查找剩余的匹配项
					node->hj_JoinState = HJ_SCAN_OUTER_BUCKET;
				}else{
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					econtext->ecxt_outertuple = NULL;
					continue;
				}
				break;
				
 
 
			case HJ_NEED_NEW_OUTER:
				if(node->outerTupleNull){
					if(node->innerTupleNull){
						node->hj_JoinState = HJ_FILL_TUPLES;
						continue;
					}else{
						node->hj_JoinState = HJ_NEED_NEW_INNER;
						continue;
					}
				}
				outerTupleSlot = ExecProcNode((PlanState *)outerHashNode); //从外部表中取出一个元组，探测hash值后压入外部hash表对应的位置
				if (TupIsNull(outerTupleSlot)) //如果取出的元组为空，由于不考虑多批次，因此代表外部表取完了
				{
					node->outerTupleNull = true;
					if (!node->innerTupleNull){
						node->hj_JoinState = HJ_NEED_NEW_INNER;
						continue;
					}else if (HJ_FILL_INNER(node)||HJ_FILL_OUTER(node)){
						node->hj_JoinState = HJ_FILL_TUPLES;
						continue;
					}else {
						return NULL;
					}
				}
				if (outerHashNode->insertTupleValueEqulNull){ //取出的元组不为空，但是值为null，并且由于不是左外连接，该元组没有被插入outerhash表中
					node->hj_JoinState = HJ_NEED_NEW_INNER;
					continue;
				}
				econtext->ecxt_outertuple = outerTupleSlot;
				if(ExecHashGetHashValue(innerHashtable, econtext,outerHashNode->hashkeys,true,false,&hashvalue)){ //求出当前取出的外部元组在内部hash表中的hash值，因为保留为null的元组没有意义，所以参数设置为false
					node->hj_CurHashValue = hashvalue;
					ExecHashGetBucketAndBatch(innerHashtable, hashvalue,
            	                               &node->hj_CurBucketNo, &batchno);
					node->hj_CurTuple = NULL; //用于scan_bucket时内部hash表中hash槽的探测，如果为null，说明该hash值对应的hash槽刚开始探测，如果不为null，说明之前探测到了一个匹配项，需要继续查找剩余的匹配项
					node->hj_JoinState = HJ_SCAN_INNER_BUCKET;
				}else{
					node->hj_JoinState = HJ_NEED_NEW_INNER;
					econtext->ecxt_outertuple = NULL;
					continue;
				}
				break;
 
			case HJ_SCAN_OUTER_BUCKET:
				node->scanBucket = false;
				if (!ExecScanHashBucket(node, econtext)) // false为探测外部表，true为探测内部hash表
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_NEED_NEW_OUTER; //当前内部元组在外部hash表中未找到匹配项，于是开始取外部元组探测内部hash表
					continue;
				}
 
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurOutTuple)); //将当前匹配到的外部hash表的元组设置为已匹配
     				ExecHashGetBucketAndBatch(innerHashtable, innerHashNode->curInsertHashValue,&bucketno, &fakeBatchno);
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(innerHashtable->buckets.unshared[bucketno]));//将当前匹配到的内部hash表的元组设置为已匹配
 
					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					// if (node->js.single_match) //这里注意一下: hj_curTuple, hj_curHashValue不能共用? 也不一定
					// 	node->hj_JoinState = HJ_NEED_NEW_OUTER;
 
					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;
 
			case HJ_SCAN_INNER_BUCKET:
				node->scanBucket = true;
				if (!ExecScanHashBucket(node, econtext)) // false为探测外部hash表，true为探测内部hash表
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_NEED_NEW_INNER; //当前外部元组在内部hash表中未找到匹配项，于是开始取内部元组探测外部hash表
					continue;
				}
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple)); //将当前匹配到的外部hash表的元组设置为已匹配
     				ExecHashGetBucketAndBatch(outerHashtable, outerHashNode->curInsertHashValue,&bucketno, &fakeBatchno);
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(outerHashtable->buckets.unshared[bucketno]));//将当前匹配到的内部hash表的元组设置为已匹配
 
					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					// if (node->js.single_match) 
					// 	node->hj_JoinState = HJ_NEED_NEW_INNER;
 
					if (otherqual == NULL || ExecQual(otherqual, econtext))
						return ExecProject(node->js.ps.ps_ProjInfo);
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;
 
			case HJ_FILL_TUPLES:
				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				if(node->firstFill){ //第一次填充hash表时需要将hash表元组序号和元组内部指针置为初始状态
					node->firstFill = false;
					ExecPrepHashTableForUnmatched(node);
				}
				if(!node->fillInnerTableFinished&&HJ_FILL_INNER(node)){
					if (!ExecScanHashTableForUnmatched(node, econtext)){
						node->fillInnerTableFinished = true;
						node->firstFill = true; //下一轮循环时将hash表元组序号和元组内部指针置为初始状态
						break;
					}else{
						econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;
						if (otherqual == NULL || ExecQual(otherqual, econtext)){
							return ExecProject(node->js.ps.ps_ProjInfo);
						}else{
							InstrCountFiltered2(node, 1);
						}
						break;
					}
				}
				if(!node->fillOuterTableFinished&&HJ_FILL_OUTER(node)){
					if (!ExecScanOutHashTableForUnmatched(node, econtext)){
						node->fillOuterTableFinished = true;
					}else{
						econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;
						if (otherqual == NULL || ExecQual(otherqual, econtext)){
							return ExecProject(node->js.ps.ps_ProjInfo);
						}else{
							InstrCountFiltered2(node, 1);
						}
						break;
					}
				}
				return NULL;
 
			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

HashJoinState *
ExecInitSymHashJoin(HashJoin *node, EState *estate, int eflags) //在此处添加你的实现
{
	HashJoinState *hjstate;
	Hash	   *outerNode;
	Hash	   *hashNode;
	TupleDesc	outerDesc,
				innerDesc;
	const TupleTableSlotOps *ops;
 
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
 
	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;
	hjstate->isSymHashJoin = node->isSymHashJoin; //表明这是一个symhashjoin
	//在此处添加你的实现
 
	/*
	 * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
	 * where this function may be replaced with a parallel version, if we
	 * managed to launch a parallel query.
	 */
	hjstate->js.ps.ExecProcNode = ExecSymHashJoin;
	hjstate->js.jointype = node->join.jointype;
 
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
	outerNode = (Hash *) outerPlan(node);
	hashNode = (Hash *) innerPlan(node);
 
	outerPlanState(hjstate) = ExecInitNode((Plan *) outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hjstate));
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	innerDesc = ExecGetResultType(innerPlanState(hjstate));
 
	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);
 
	/*
	 * tuple table initialization
	 */
	ops = ExecGetResultSlotOps(outerPlanState(hjstate), NULL);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
														ops);
	//hjstate->hj_HashTupleSlot = ExecInitExtraTupleSlot(estate, innerDesc,
														//ops);
 
	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);
 
	/* set up null tuples for outer joins, if needed */
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
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}
 
	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{  //这一部分代码对正确性非常重要 值得研究
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;
 
		hjstate->hj_HashTupleSlot = slot;
	}
 
	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);
 
	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_outerHashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;
 
	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurOutHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;
	hjstate->hj_CurOutTuple = NULL;
 
	// hjstate->hj_OuterHashKeys = ((HashState*) outerPlanState(hjstate))->hashkeys;
	// hjstate->
	hjstate->hj_HashOperators = node->hashoperators;
	hjstate->hj_Collations = node->hashcollations;
 
	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;
	hjstate->fillInnerTableFinished = false;
	hjstate->fillOuterTableFinished = false;
	hjstate->firstFill = true;
	hjstate->innerTupleNull = false;
	hjstate->outerTupleNull = false;
 
	return hjstate;
}
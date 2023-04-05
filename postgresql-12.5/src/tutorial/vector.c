#include "c.h"
#include "postgres.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

PG_MODULE_MAGIC;

typedef struct Vector
{
    int32 size;
    float4 data[FLEXIBLE_ARRAY_MEMBER];
} Vector;

PG_FUNCTION_INFO_V1(vector_in);

Datum vector_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    if (str[0] != '{' || str[strlen(str) - 1] != '}') {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("vector must begin with \'{\' and end with \'}\'")));
    }
    float4 data[1024];
    int32 size = 0;
    const char s[] = "{,}";
    char* token;
    token = strtok(str, s);
    if (token == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("dimension of vector must be greater than one")));
    }
    while (token != NULL) {
        // elog(LOG, "token:%s", token);
        // int spaceAllow = true;
        // for (int i = 0; i < strlen(token); i++) {
        //     if (!isdigit(token[i])) {
        //         if (token[i] == ' ' && !spaceAllow) {
        //             ereport(ERROR,
        //                     (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
        //                      errmsg("vector format error: error space")));
        //         }
        //         else if (token[i] != ' ' &&
        //                  (!i && token[i] != '-' && token[i] != '+' ||
        //                   i && token[i] != '.')) {
        //             ereport(ERROR,
        //                     (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
        //                      errmsg("vector format error")));
        //         }
        //     }
        //     if (isdigit(token[i]) || token[i] == '+' || token[i] == '-') {
        //         spaceAllow = false;
        //     }
        //     if (token[i] == ',') {
        //         spaceAllow = true;
        //     }
        // }
        data[size++] = strtof(token, NULL);
        token = strtok(NULL, s);
    }
    Vector* result = (Vector*)palloc(sizeof(Vector) + size * sizeof(float4));
    // memcpy(result->data, data, (size) * sizeof(float4));
    for (int32 i = 0; i < size; i++) {
        result->data[i] = data[i];
        // elog(LOG, "resultdata:%f data: %f", result->data[i], data[i]);
    }
    result->size = size;
    SET_VARSIZE(result, size * sizeof(float4) + VARHDRSZ);
    int32 sizeTemp = (VARSIZE_ANY(result) - VARHDRSZ) / sizeof(float4);
    float4* temp = (float4*)VARDATA_ANY(result);
    // elog(LOG, "resultSize:%d", sizeTemp);
    // for (int i = 0; i < size; i++) {
    //     elog(LOG, "data: %f ", result->data[i]);
    // }
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_out);

Datum vector_out(PG_FUNCTION_ARGS)
{
    Vector* v = (Vector*)PG_GETARG_POINTER(0);
    int32 size = 0;
    int dimension = (VARSIZE_ANY(v) - VARHDRSZ) / sizeof(float4);
    char* result = (char*)palloc((dimension * 50) * sizeof(char));
    result[size++] = '{';
    // elog(LOG, "dimension:%d", dimension);
    for (int32 i = 0; i < dimension; i++) {
        // elog(LOG, "i:%d", i);
        float4 x = v->data[i];
        char temp[64];
        int32 len = float_to_shortest_decimal_bufn(x, temp);
        // elog(LOG, "len:%d", len);
        for (int j = 0; j < len; j++) {
            result[size++] = temp[j];
            // elog(LOG,
            //      "result[%d]:%c temp:%c",
            //      size - 1,
            //      result[size - 1],
            //      temp[j]);
        }
        if (i == dimension - 1) {
            result[size++] = '}';
            result[size++] = '\0';
        }
        else
            result[size++] = ',';
    }
    result[size++] = 't';
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(vector_size);

Datum vector_size(PG_FUNCTION_ARGS)
{
    Vector* v = (Vector*)PG_GETARG_POINTER(0);
    int32 size = (VARSIZE_ANY(v) - VARHDRSZ) / sizeof(float4);
    PG_RETURN_INT32(size);
}

PG_FUNCTION_INFO_V1(vector_distance);

Datum vector_distance(PG_FUNCTION_ARGS)
{
    float4 dis = 0;
    Vector* left = (Vector*)PG_GETARG_POINTER(0);
    Vector* right = (Vector*)PG_GETARG_POINTER(1);
    int32 leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float4);
    int32 rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float4);
    float4* varLeft = (float4*)VARDATA(left);
    float4* varRight = (float4*)VARDATA(right);
    if (leftSize != rightSize) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("vectors must have same dimension")));
    }
    // elog(LOG, "left->size %d", leftSize);
    // elog(LOG, "right->size %d", rightSize);
    // elog(LOG, "data[VAR]:%f", varLeft[0]);
    for (int32 i = 0; i < leftSize; i++) {
        // elog(LOG, "left%f,right%f", varLeft[i], varRight[i]);
        dis += pow(varLeft[i] - varRight[i], 2);
        // elog(LOG, "dis:%f", dis);
    }
    dis = (float)sqrt(dis);
    // elog(LOG, "dis:%f", dis);
    PG_RETURN_FLOAT4(dis);
}

PG_FUNCTION_INFO_V1(vector_add);

Datum vector_add(PG_FUNCTION_ARGS)
{
    Vector* left = (Vector*)PG_GETARG_POINTER(0);
    Vector* right = (Vector*)PG_GETARG_POINTER(1);
    int32 leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float4);
    int32 rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float4);
    float4* varLeft = (float4*)VARDATA(left);
    float4* varRight = (float4*)VARDATA(right);
    if (leftSize != rightSize) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("vectors must have same dimension")));
    }
    Vector* result =
        (Vector*)palloc(sizeof(Vector) + leftSize * sizeof(float4));
    result->size = leftSize;
    for (int32 i = 0; i < leftSize; i++) {
        result->data[i] = varLeft[i] + varRight[i];
    }
    SET_VARSIZE(result, leftSize * sizeof(float4) + VARHDRSZ);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_sub);

Datum vector_sub(PG_FUNCTION_ARGS)
{
    Vector* left = (Vector*)PG_GETARG_POINTER(0);
    Vector* right = (Vector*)PG_GETARG_POINTER(1);
    int32 leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float4);
    int32 rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float4);
    float4* varLeft = (float4*)VARDATA(left);
    float4* varRight = (float4*)VARDATA(right);
    if (leftSize != rightSize) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("vectors must have same dimension")));
    }
    Vector* result =
        (Vector*)palloc(sizeof(Vector) + leftSize * sizeof(float4));
    result->size = leftSize;
    for (int32 i = 0; i < result->size; i++) {
        result->data[i] = varLeft[i] - varRight[i];
    }
    SET_VARSIZE(result, leftSize * sizeof(float4) + VARHDRSZ);
    PG_RETURN_POINTER(result);
}

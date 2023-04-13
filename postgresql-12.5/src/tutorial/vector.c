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

#define THROW_ERROR(info)                                                      \
    ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),              \
                    errmsg("vector format error: " info)))

typedef struct Vector
{
    int size;
    float data[FLEXIBLE_ARRAY_MEMBER];
} Vector;

PG_FUNCTION_INFO_V1(vector_in);

Datum vector_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    float data[1024];
    int size = 0;
    const char s[] = "{,}";
    char *token;
    bool spaceAllow;
    bool dotAllow;
    bool addOrSubAllow;
    bool digitAppearance;
    Vector *result;
    if (str[0] != '{' || str[strlen(str) - 1] != '}')
    {
        THROW_ERROR("Vector must begin with \'{\' and end with \'}\'");
    }
    token = strtok(str, s);
    if (token == NULL)
    {
        THROW_ERROR("Dimension of vector must be greater than one");
    }
    while (token != NULL)
    {
        elog(NOTICE, "token: %s", token);
        spaceAllow = true;
        dotAllow = false;
        addOrSubAllow = true;
        digitAppearance = false;
        for (int i = 0; i < strlen(token); i++)
        {
            if (isdigit(token[i]))
            {
                dotAllow = true;
                spaceAllow = false;
                addOrSubAllow = false;
                digitAppearance = true;
            }
            else
            {
                if (token[i] == '+' || token[i] == '-')
                {
                    if (addOrSubAllow)
                    {
                        addOrSubAllow = false;
                        spaceAllow = false;
                    }
                    else
                    {
                        THROW_ERROR("Error in addition or subtraction");
                    }
                }
                if (token[i] == '.')
                {
                    if (dotAllow)
                    {
                        dotAllow = false;
                        spaceAllow = false;
                    }
                    else
                    {
                        THROW_ERROR("Error in dot");
                    }
                }
                if (token[i] == ' ')
                {
                    if (!spaceAllow)
                    {
                        THROW_ERROR("Error in space");
                    }
                }
            }
        }
        if (!digitAppearance)
        {
            THROW_ERROR("Error in no number");
        }
        data[size++] = strtof(token, NULL);
        token = strtok(NULL, s);
    }
    result = (Vector *)palloc(sizeof(Vector) + size * sizeof(float));
    memcpy(result->data, data, (size) * sizeof(float));
    result->size = size;
    SET_VARSIZE(result, size * sizeof(float) + VARHDRSZ);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_out);

Datum vector_out(PG_FUNCTION_ARGS)
{
    Vector *v = (Vector *)PG_GETARG_POINTER(0);

    int dimension = (VARSIZE_ANY(v) - VARHDRSZ) / sizeof(float);
    char *result = (char *)palloc((dimension * 50) * sizeof(char));
    int size = 0;
    float x;
    char temp[64];
    int len;
    result[size++] = '{';
    for (int i = 0; i < dimension; i++)
    {
        x = v->data[i];
        len = float_to_shortest_decimal_bufn(x, temp);
        for (int j = 0; j < len; j++)
        {
            result[size++] = temp[j];
        }
        if (i == dimension - 1)
        {
            result[size++] = '}';
            result[size++] = '\0';
        }
        else
            result[size++] = ',';
    }
    PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(vector_size);

Datum vector_size(PG_FUNCTION_ARGS)
{
    Vector *v = (Vector *)PG_GETARG_POINTER(0);
    int size = (VARSIZE_ANY(v) - VARHDRSZ) / sizeof(float);
    PG_RETURN_INT32(size);
}

PG_FUNCTION_INFO_V1(vector_distance);

Datum vector_distance(PG_FUNCTION_ARGS)
{
    Vector *left = (Vector *)PG_GETARG_POINTER(0);
    Vector *right = (Vector *)PG_GETARG_POINTER(1);
    int leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float);
    int rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float);
    float *varLeft = (float *)VARDATA(left);
    float *varRight = (float *)VARDATA(right);
    float dis = 0;
    if (leftSize != rightSize)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vectors must have same dimension")));
    }
    for (int i = 0; i < leftSize; i++)
    {
        dis += pow(varLeft[i] - varRight[i], 2);
    }
    dis = (float)sqrt(dis);
    PG_RETURN_FLOAT4(dis);
}

PG_FUNCTION_INFO_V1(vector_add);

Datum vector_add(PG_FUNCTION_ARGS)
{
    Vector *left = (Vector *)PG_GETARG_POINTER(0);
    Vector *right = (Vector *)PG_GETARG_POINTER(1);
    int leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float);
    int rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float);
    float *varLeft = (float *)VARDATA(left);
    float *varRight = (float *)VARDATA(right);
    Vector *result;
    if (leftSize != rightSize)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vectors must have same dimension")));
    }
    result = (Vector *)palloc(sizeof(Vector) + leftSize * sizeof(float));
    result->size = leftSize;
    for (int i = 0; i < leftSize; i++)
    {
        result->data[i] = varLeft[i] + varRight[i];
    }
    SET_VARSIZE(result, leftSize * sizeof(float) + VARHDRSZ);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_sub);

Datum vector_sub(PG_FUNCTION_ARGS)
{
    Vector *left = (Vector *)PG_GETARG_POINTER(0);
    Vector *right = (Vector *)PG_GETARG_POINTER(1);
    int leftSize = (VARSIZE_ANY(left) - VARHDRSZ) / sizeof(float);
    int rightSize = (VARSIZE_ANY(right) - VARHDRSZ) / sizeof(float);
    float *varLeft = (float *)VARDATA(left);
    float *varRight = (float *)VARDATA(right);
    Vector *result;
    if (leftSize != rightSize)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vectors must have same dimension")));
    }
    result = (Vector *)palloc(sizeof(Vector) + leftSize * sizeof(float));
    result->size = leftSize;
    for (int i = 0; i < result->size; i++)
    {
        result->data[i] = varLeft[i] - varRight[i];
    }
    SET_VARSIZE(result, leftSize * sizeof(float) + VARHDRSZ);
    PG_RETURN_POINTER(result);
}

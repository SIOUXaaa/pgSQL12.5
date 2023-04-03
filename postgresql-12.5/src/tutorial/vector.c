#include "postgres.h"
#include "fmgr.h"
#include "common/shortest_dec.h"
#include "errcodes.h"
#include <string.h>
#include <math.h>

PG_MODULE_MAGIC;

typedef struct Vector
{
    float data[FLEXIBLE_ARRAY_MEMBER];
    int size;
}Vector;

PG_FUNCTION_INFO_V1(vector_in);

Datum
vector_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    if(str[0]!='{'||str[strlen(str) - 1]!='}'){
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("vector must begin with \'{\' and end with \'}\'")));
    }
    float data[1024];
    int size=0;
    const char s[2]=",";
    char *token;
    token = strtok(str, s);
    if(token == NULL){
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("dimension of vector must be greater than one")));
    }
    while(token != NULL){   
        data[size++] = strtof(token, NULL); 
        token = strtok(NULL, s);       
    }
    Vector *result = (Vector *)palloc(sizeof(Vector) + size * sizeof(float));
    result->data = (float *) palloc((size + VARHDRSZ) * sizeof(float));
    SET_VARSIZE(result->data, size + VARHDRSZ);
    for(int i = 0; i < size; i++){
        result->data[i]=data[i];
    }
    result->size = size;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_out);

Datum
vector_out(PG_FUNCTION_ARGS)
{
    Vector *v = PG_GETARG_POINTER(0);
    int size=0;
    char *result = (char *)palloc((v->size * 20 + 2) * sizeof(char));
    result[size++] = '{';
    for (int i = 0; i < v->size; i++) {    
        float x = v->data[i];
        int len;
        char *temp = float_to_shortest_decimal_bufn(x, 0, 6, false, &len, NULL, NULL);  
        memcpy(result->data + size, temp, len);
        size += len;
        if(i == v->size - 1 ){
            result[size++] = '}';
            result[size++] = '\0';
        }
        else
            result[size++] = ',';
    }
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_size);

Datum
vector_size(PG_FUNCTION_ARGS)
{
    Vector *v = PG_GETARG_POINTER(0);
    int size = v->size;
    PG_RETURN_INT(size);
}

#define pow(c) ((c)*(c))

PG_FUNCTION_INFO_V1(vector_distance);

Datum
vector_distance(PG_FUNCTION_ARGS)
{
    float dis = 0;
    Vector *left = PG_GETARG_POINTER(0);
    Vector *right = PG_GETARG_POINTER(1);
    if(left->size != right->size){
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vectors must have same dimension")));
    }
    for(int i = 0; i < left->size; i++){
        dis += pow(left->data[i]-right->data[i]);
    }
    dis = sqrt(dis);
    PG_RETURN_FLOAT4(dis);
}

PG_FUNCTION_INFO_V1(vector_add);

Datum
vector_add(PG_FUNCTION_ARGS)
{
    Vector *left = PG_GETARG_POINTER(0);
    Vector *right = PG_GETARG_POINTER(1);
    if(left->size != right->size){
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vectors must have same dimension")));
    }
    Vector *result = (Vector *)palloc(sizeof(Vector));
    result->data = (float *)palloc(left->size + VARHDRSZ);
    result->size = left->size;
    SET_VARSIZE(result->data, left->size + VARHDRSZ);
    for(int i = 0; i < result->size; i++){
        result->data[i] = left->data[i] + right->data[i];
    }
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(vector_sub);

Datum
vector_sub(PG_FUNCTION_ARGS)
{
    Vector *left = PG_GETARG_POINTER(0);
    Vector *right = PG_GETARG_POINTER(1);
    if(left->size != right->size){
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vectors must have same dimension"));)
    }
    Vector *result = (Vector *)palloc(sizeof(Vector));
    result->data = (float *)palloc(left->size + VARHDRSZ);
    result->size = left->size;
    SET_VARSIZE(result->data, left->size + VARHDRSZ);
    for(int i = 0; i < result->size; i++){
        result->data[i] = left->data[i] - right->data[i];
    }
    PG_RETURN_POINTER(result);
}


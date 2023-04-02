#include "postgres.h"
#include "fmgr.h"
#include "common/shortest_dec.h"
#include <string.h>

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
    float data[1024];
    int size=0;
    const char s[2]=",";
    char *token;
    token = strtok(str, s);
    while(token != NULL){   
        data[size++] = strtof(token, NULL); 
        token = strtok(NULL, s);       
    }
    Vector *result = (Vector *)palloc(sizeof(Vector));
    result->data = (float *) palloc(size + VARHDRSZ);
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
    result[size++] = '{';
    for (int i = 0; i < v->size; i++) {
        char temp[1024];
        float x = v->data[i];
        sprintf(temp, "%f", x);
        int pos=0;
        while(temp[pos]!='\0'){
            pos++;
        }
        pos--;
        while(temp[pos]=='0'){
            temp[pos--]='\0';
        }
        for(int i=0;i<=pos;i++){
            result[size++]=temp[i];
        }
        if(i == v->size - 1 ){
            result[size++] = '}';
            result[size++] = '\0';
        }
        else
            result[size++] = ',';       
    }
    return result;
}






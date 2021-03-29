#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<semaphore.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>

#define THREAD_NUM 2
typedef struct
{
    char * buf;
    int _nThreadId;
    size_t start;
    size_t end;
}IDD_THREAD_PARAM;

typedef struct{
    int b;
    int a;
    int count_a;
}DataType;

typedef struct{
    DataType data;
    struct  HashNode *next;  //key冲突时，通过next指针进行连接
}HashNode;

typedef struct{
    int size;
    HashNode *table;
}HashMap,*hashmap;


HashMap *CreateHashMap(int size){
    HashMap *hashmap=(HashMap*)malloc(sizeof(HashMap));
    hashmap->size=2*size;
    hashmap->table=(HashNode *)malloc(sizeof(HashNode)*hashmap->size);
    int j=0;
    for(j=0;j<hashmap->size;j++){
        hashmap->table[j].next=NULL;
        hashmap->table[j].data.a = INT32_MIN;
    }
    return hashmap;
}

int Put(HashMap hashmap, char* key,char* value){

    int idx = atoi(key);
    int val = atoi(value);
    int pos=abs(idx)%hashmap.size;
    HashNode *pointer=&(hashmap.table[pos]);
    if(pointer->data.a == INT32_MIN)
    {
        pointer->data.a=idx;
        pointer->data.b=val;
        pointer->data.count_a = 1;
    }else{
        while(pointer->next!=NULL){
            if (pointer->data.a == idx){
                printf("aaa1  %d %d %d \n", pointer->data.a, pointer->data.b, pointer->data.count_a);
                pointer->data.count_a ++;
                pointer->data.b += val;
                printf("aaa2  %d %d %d \n", pointer->data.a, pointer->data.b, pointer->data.count_a);
                return 1;
            } else {
                pointer=pointer->next;
            }
        }
        if (pointer->data.a == idx){
            printf("bbb1  %d %d %d \n", pointer->data.a, pointer->data.b, pointer->data.count_a);
            pointer->data.count_a ++;
            pointer->data.b += val;
            printf("bbb2  %d %d %d \n", pointer->data.a, pointer->data.b, pointer->data.count_a);
            return 1;
        }

        HashNode *hnode=(HashNode *)malloc(sizeof(HashNode));

        hnode->data.a=idx;
        hnode->data.b=val;
        hnode->data.count_a = 1;
        hnode->next=NULL;
        pointer->next=hnode;

    }
    return 1;
}

int Put_i(HashMap hashmap, int idx, int val, int count){
    int pos=abs(idx)%hashmap.size;
    HashNode *pointer=&(hashmap.table[pos]);
    if(pointer->data.a == INT32_MIN)
    {
        pointer->data.a=idx;
        pointer->data.b=val;
        pointer->data.count_a = count;
    }else{
        while(pointer->next!=NULL){
            if (pointer->data.a == idx){
                pointer->data.count_a += count;
                pointer->data.b += val;
                return 1;
            } else {
                pointer=pointer->next;
            }
        }
        if (pointer->data.a == idx){
            pointer->data.count_a += count;
            pointer->data.b += val;
            return 1;
        }

        HashNode *hnode=(HashNode *)malloc(sizeof(HashNode));

        hnode->data.a=idx;
        hnode->data.b=val;
        hnode->data.count_a = count;
        hnode->next=NULL;
        pointer->next=hnode;

    }
    return 1;
}


int Get(HashMap *hashmap ,int key){

    int pos=abs(key)/hashmap->size;
    HashNode *pointer=&(hashmap->table[pos]);

    while(pointer!=NULL){
        if(pointer->data.a==key)
            return pointer->data.b;
        else
            pointer=pointer->next;
    }

    return -1;
}

void c_split(char *src, const char *separator, int maxlen, char **dest, int *num)
{
    char *pNext;
    int count = 0;
    if (src == NULL || strlen(src) == 0)
        return;
    if (separator == NULL || strlen(separator) == 0)
        return;
    pNext = strtok(src, separator);
    while (pNext != NULL && count < maxlen) {
        *dest++ = pNext;
        ++count;
        pNext = strtok(NULL, separator);
    }
    *num = count;
}


void *ThreadFunc(void *args)
{
    IDD_THREAD_PARAM *block = ((IDD_THREAD_PARAM *)args);

    size_t s = block->start;
    int nId = block->_nThreadId;

    int i = 0;
    char *split_bufs[10] = {0};
    int num =0;
    char src[1000] ;
    strncpy(src, block->buf + block->start, block->end - block->start);

    c_split(src, "\n", 10 , split_bufs, &num);


    HashMap *hashmap=CreateHashMap(num);
    for(i=0;i< num;i++){
        char *split_buf[10] = {0};
        int nn =0;
        c_split(split_bufs[i], " ", 10 , split_buf, &nn);
        Put(*hashmap, split_buf[1], split_buf[0]);
    }
    i=0;
    HashNode *pointer;
    while(i<hashmap->size){
        pointer=&(hashmap->table[i]);
        while(pointer!=NULL){
            if(pointer->data.a!=INT32_MIN)
                printf("thread %10d %10d\n",pointer->data.a, pointer->data.b);
            pointer=pointer->next;
        }
        i++;
    }
    printf("\n#####Thread exit %d#####\n",nId);

    pthread_exit((void*)hashmap);
}



size_t get_end(int start, int percent, char *buffer)
{
    int s = start +  percent;
    if (s >= strlen(buffer)-1){
        return strlen(buffer) -1;
    }
    while(buffer[s] != '\n' && buffer[s] !='\0'){
        s++;
    }
    if (buffer[s] =='\0'){
        return s-1;
    }
    return s;
}


int main()
{
    FILE *fp = fopen("data.txt","r");
    long lSize;
    char * buffer;
    size_t result;
    if (fp==NULL)
    {
        fputs ("File error",stderr);
        exit (1);
    }
    fseek (fp , 0 , SEEK_END);
    lSize = ftell (fp);
    rewind (fp);

    buffer = (char*) malloc (sizeof(char)*lSize);
    if (buffer == NULL)
    {
        fputs ("Memory error",stderr);
        exit (2);
    }

    result = fread (buffer,1,lSize,fp);
    if (result != lSize)
    {
        fputs ("Reading error",stderr);
        exit (3);
    }

    pthread_t pThreads[THREAD_NUM];


    size_t percent = strlen(buffer) / THREAD_NUM;

    IDD_THREAD_PARAM *param =  (IDD_THREAD_PARAM *)malloc(THREAD_NUM*sizeof( IDD_THREAD_PARAM));
    int i=0;
    int lastEnd = -1;
    for(;i<THREAD_NUM;i++)
    {
        param[i].buf = buffer;
        param[i]._nThreadId = i;

        param[i].start = lastEnd+1;
        param[i].end = get_end(lastEnd, percent, buffer);
        lastEnd =  param[i].end;

    }
    param[i].end = buffer[strlen(buffer)-1];
    for( i=0;i<THREAD_NUM;i++)
    {
        pthread_create(&pThreads[i],NULL,ThreadFunc,&(param[i]));
    }
    HashMap *allmap = CreateHashMap(strlen(buffer)/4);

    for( i=0;i<THREAD_NUM;i++){
        void *hashmaps;
        pthread_join(pThreads[i],&hashmaps);
        int j=0;
        while(j< ((HashMap*)hashmaps)->size){
            HashNode *pointer;
            pointer=&(((HashMap*)hashmaps)->table[j]);
            while(pointer != NULL){
                if(pointer->data.a!=INT32_MIN){
//                    printf("%10d %10d\n",pointer->data.a, pointer->data.b);
                    Put_i(*allmap, pointer->data.a, pointer->data.b, pointer->data.count_a);
                }
                pointer=pointer->next;
            }
            j++;
        }

    }
    int j = 0;
    while(j< allmap->size){
        HashNode *pointer;
        pointer=&(allmap->table[j]);
        while(pointer != NULL){
            if(pointer->data.a!=INT32_MIN){
                    printf("%10d  %f\n",pointer->data.a, (float)(pointer->data.b)/(pointer->data.count_a));
            }
            pointer=pointer->next;
        }
        j++;
    }

    free(allmap);
    free(param);
    fclose(fp);
    return 0;
}

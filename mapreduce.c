#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "mapreduce.h"
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sysinfo.h>

Mapper map_func;
Partitioner partitioner;
Reducer reduce_func;
char **queue;
int cursize = 0,fillptr=0, outptr = 0,max;
int tablesize=5001;
int numofpartitions;

pthread_mutex_t queuelock = PTHREAD_MUTEX_INITIALIZER;

struct node{
  char *key;
  struct values *val;
  struct node *next; 
};

struct values{
  char *value;
  struct values *nextvalue;
};

struct hashtable{
  int size;
  int SIZE;
  int keycount;
  struct node **table;
  pthread_mutex_t *lock;
  char **sortedkeys;
  int get_next_flag;
  struct values *valtemp;
}**bucket; 

void enqueue(char *ch){
  queue[fillptr] = ch;
  fillptr = (fillptr + 1) % max;
  cursize++;
}

char* dequeue(){
  char *ch= queue[outptr];
  outptr = (outptr + 1) % max;
  cursize--;
  return ch;
}

struct hashtable *ht_create( int size ){
  struct hashtable *hasht = NULL;
  int i;
  if( size < 1 ) return NULL;
  if( ( hasht = malloc( sizeof(struct hashtable) ) ) == NULL ){
    return NULL;
  }
  if( ( hasht->table = malloc( sizeof(struct node*) * size ) ) == NULL ){
    return NULL;
  }
  if( ( hasht->lock = malloc( sizeof(pthread_mutex_t) * size ) ) == NULL ){
    return NULL;
  }
  hasht->SIZE=50000;
  if((hasht->sortedkeys =(char**)malloc(sizeof(char*)*hasht->SIZE)) == NULL){
    return NULL;
  }
  for( i = 0; i < size; i++ ){
    hasht->table[i]=NULL;
    pthread_mutex_init(&(hasht->lock[i]),NULL);
  }
  hasht->keycount=0;
  hasht->size = size;
  hasht->get_next_flag=0;
  hasht->valtemp=NULL;
  return hasht;
}

unsigned long
hash(unsigned char *str){
  unsigned long hash = 5381;
  int c;
  while ((c = *str++)){
    hash = ((hash << 5) + hash) + c;
  }
  hash=hash%tablesize;
  return hash;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

void insert(struct hashtable *hasht,char *key,char* value){
  long index=hash((unsigned char*)key);
  pthread_mutex_t *lck=hasht->lock+index;
  pthread_mutex_lock(lck);
  if(hasht->table[index]==NULL){
    hasht->table[index]=malloc(sizeof(struct node));
    hasht->table[index]->key=strdup(key);
    hasht->table[index]->next=NULL;
    hasht->table[index]->val=malloc(sizeof(struct values));
    hasht->table[index]->val->value=strdup(value);
    hasht->table[index]->val->nextvalue=NULL;
    hasht->sortedkeys[hasht->keycount]=hasht->table[index]->key;
    hasht->keycount++;
    if(hasht->keycount>=hasht->SIZE){
      hasht->SIZE=2*hasht->keycount;
      hasht->sortedkeys=(char**)realloc(hasht->sortedkeys,hasht->SIZE*sizeof(char*));
    }
    pthread_mutex_unlock(lck);
    return;
  }
  else{
    struct node *curr=hasht->table[index];
    struct node *temp=curr;
    while(curr!=NULL){
      if(strcmp(key,curr->key)==0){
	struct values *v=malloc(sizeof(struct values));
	v->value=strdup(value);
	v->nextvalue=curr->val;
	curr->val=v;
	pthread_mutex_unlock(lck);
        return;
      }
      curr=curr->next;
    }
    struct node *new=malloc(sizeof(struct node));
    new->key=strdup(key);
    new->val=malloc(sizeof(struct values));
    new->val->value=strdup(value);
    new->val->nextvalue=NULL;
    new->next=temp;
    hasht->table[index]=new;
    hasht->sortedkeys[hasht->keycount]=new->key;
    hasht->keycount++;
    if(hasht->keycount>=hasht->SIZE){
      hasht->SIZE=2*hasht->keycount;
      hasht->sortedkeys=(char**)realloc(hasht->sortedkeys,hasht->SIZE*sizeof(char*));
    }
    pthread_mutex_unlock(lck);
    return;
  }
}

char *get_next(char *key, int partition_number){
  struct hashtable *t=bucket[partition_number];
  long pos=hash((unsigned char*)key);
  struct node *curr=t->table[pos];
  if(t->get_next_flag==0 && curr!=NULL){
    while(curr!=NULL){
      if(strcmp(curr->key,key)==0){
        break;
      }
      curr=curr->next;
    }
    t->valtemp=curr->val;
    t->get_next_flag=1;
  }
  if(curr==NULL)
    return NULL;
  else{
    char *ch;
    if(t->valtemp==NULL){ 
      t->get_next_flag=0;
      return NULL;
    }
    ch=t->valtemp->value;
    t->valtemp=t->valtemp->nextvalue;
    return ch;
  }
} 

void MR_Emit(char *key,char *value){
  long pNo;
  if(partitioner!=MR_DefaultHashPartition)
    pNo=partitioner(key,numofpartitions);
  else
    pNo=MR_DefaultHashPartition(key,numofpartitions);
  insert(bucket[pNo],key,value);
}

void *mapper(void *arg){
  pthread_mutex_lock(&queuelock);
  while(cursize!=0){
    char *ch=dequeue();
    map_func(ch);
  }
  pthread_mutex_unlock(&queuelock);
  return NULL;
}
 
void *reducer(void *arg){
  int p=*(int*)arg;
  for(int i=0;i<bucket[p]->keycount;i++){
    reduce_func(bucket[p]->sortedkeys[i],get_next,p);
  }
  return NULL;
}

void swap_str_ptrs(char **arg1, char **arg2){
  char *tmp = *arg1;
  *arg1 = *arg2;
  *arg2 = tmp;
}

void quicksort(char *args[], unsigned int len){
  unsigned int i, pvt=0;
  if (len <= 1)
      return;
  swap_str_ptrs(args+((unsigned int)rand() % len), args+len-1);
  for (i=0;i<len-1;++i){
    if (strcmp(args[i], args[len-1]) < 0)
      swap_str_ptrs(args+i, args+pvt++);
  }
  swap_str_ptrs(args+pvt, args+len-1);
  quicksort(args, pvt++);
  quicksort(args+pvt, len - pvt);
}

void sortpartition(void *arg){
  int p=*(int*)arg;
  quicksort(bucket[p]->sortedkeys,bucket[p]->keycount);
}

void freememory(struct hashtable *t){
  for(int j=0;j<tablesize;j++){
    struct node *curr=t->table[j];
    while(curr!=NULL){
      struct node *prev=curr;
      struct values *x=curr->val;
      while(x!=NULL){
        struct values *y=x;
        free(x->value);
        x=x->nextvalue;
        free(y);
      }
      free(curr->key);
      curr=curr->next;
      free(prev);
    }
    pthread_mutex_destroy(&(t->lock[j]));
  }
  free(t->sortedkeys);
  free(t->table);
  free(t->lock);
  free(t);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers,Partitioner partition){
  cursize = 0;fillptr=0;outptr = 0;
  tablesize=5001;
  max=argc-1;
  map_func=map;
  partitioner=partition;
  reduce_func=reduce;
  max=argc-1;
  numofpartitions=num_reducers;
  bucket=malloc(sizeof(struct hashtable*)*num_reducers);
  for(int i=0;i<num_reducers;i++){
    struct hashtable *table = ht_create(tablesize);
    bucket[i]=table;
  }
  if(argv[1]==NULL)
    {
      printf("mapreduce: file1 [file2...]\n");
      exit(1);
    }
  queue=(char**)malloc((argc-1)*sizeof(char*));
  if(argc-1>1){
    int filesizearray[argc-1];
    char **sortedfiles=(char **)(argv+1);
    struct stat filestat;
    for(int i=0;i<argc-1;i++){
      if(stat(argv[i+1],&filestat)==0){
        filesizearray[i] = filestat.st_size;
      }
    }
    int n=argc-1;
    for (int i = 0; i < n-1; i++){         
      for (int j = 0; j < n-i-1; j++){ 
        if (filesizearray[j] > filesizearray[j+1]){
          int temp=filesizearray[j];
          filesizearray[j]=filesizearray[j+1];
          filesizearray[j+1]=temp;
          char *ch=strdup(sortedfiles[j]);
          sortedfiles[j]=sortedfiles[j+1];
          sortedfiles[j+1]=ch;
        }
      }
    }
    for(int i=0;i<n;i++){
      enqueue(sortedfiles[i]);
    }
  }
  else
    enqueue(argv[1]);
  
  int a[num_reducers];
  for(int i=0;i<num_reducers;i++)
    a[i]=i;
  
  pthread_t mapthread[num_mappers],reducethread[num_reducers];
  
  for (int i = 0; i < num_mappers; i++)
    pthread_create(&mapthread[i], NULL, mapper, NULL);
  for (int i = 0; i < num_mappers; i++)
    pthread_join(mapthread[i], NULL);
   
  free(queue);
  
  for(int k=0;k<num_reducers;k++){
    sortpartition(&a[k]);
  }
  for(int k=0;k<num_reducers;k++){
    pthread_create(&reducethread[k], NULL, reducer,&a[k]);
  }
  for (int i = 0; i < num_reducers; i++)
    pthread_join(reducethread[i], NULL);
  
  for(int i=0;i<num_reducers;i++)
    freememory(bucket[i]);
  
  free(bucket);
}


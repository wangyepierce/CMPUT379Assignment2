#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include "threadpool.h"
#include "mapreduce.h"

#include <map>
#include <string>
#include <bits/stdc++.h> 
#include <string>
#include <iostream>

int  num_partitions_g;

std::map<int, std::map<std::string,std::vector<std::string> > > data_pool; 

pthread_mutex_t data_lock; 

Reducer reducer;

//////////////////////////////////////////////////////
int find_filesize(char * temp){
    struct stat st;
    stat(temp, &st);  
    return (int) st.st_size;
}

static int compare_filesize(const void* str1, const void* str2){
    return find_filesize(*(char**)str1)-find_filesize(*(char**)str2);
}

void sort(char* arr[], int n) 
{ 
    qsort(arr, n, sizeof(const char*), compare_filesize); 
} 
//////////////////////////////////////////////////////

char *MR_GetNext(char *key, int partition_number){
    // char * re= (data_pool[partition_number]->second)[key].begin().c_str();
    data_pool.at(partition_number);
    if (data_pool.at(partition_number).at(key).empty()){

        return NULL;
    }
    data_pool.at(partition_number).at(key).pop_back();
    char * re=(char *) "1";

    return re;      
}

void MR_ProcessPartition(void* argv){
    int partition_number= *((int *) argv);
    delete (int*) (argv);
    printf("partition_number: %d\n",partition_number);
    // void Reduce(char *key, int partition_number) {

    for (auto i = data_pool[partition_number].begin(); i != data_pool[partition_number].end(); i++) { 
		// std::cout << i->first << " : " << '\n'; 
        reducer((char*)i->first.c_str() ,partition_number);
	} 


}

unsigned long MR_Partition(char *key, int num_partitions){
    unsigned long hash=5381;
    int c;
    while((c=*key++)!='\0')
        hash=hash*33+c;
    return hash% num_partitions;
}

void MR_Emit(char *key, char *value){
    unsigned long partition= MR_Partition(key, num_partitions_g);
    // printf("partition: %lu %s \t: %s\n",partition, key, value);

    pthread_mutex_lock(&data_lock);     
    data_pool[partition][key].push_back(value);
    pthread_mutex_unlock(&data_lock); 
}

void MR_Run(int num_files, char *filenames[], Mapper map, int num_mappers,Reducer concate, int num_reducers){
    num_partitions_g=num_reducers;
    printf("Running MR_Run %d\n",num_files);
    reducer=concate;
    

    // for (int i=0; i<num_files;i++)
    //     printf("%d:  %s\n",i,filenames[i]);


    // printf("Tag A\n");
    ThreadPool_t *tp =ThreadPool_create(num_mappers); // Create Mappers    
   

    for (int i=0; i<num_files; i++) {  
        // printf("%d:  %s\n",i,filenames[i]);         
        ThreadPool_add_work(tp,(thread_func_t) map, filenames[i]);
        // printf("Tag D\n");
        
    }
    // printf("Tag E\n");
    // *************************************************************************************
    // thread_pool_wait(tp_mapper);

    if (tp == NULL){
        return;
    }
    pthread_mutex_lock(&(tp -> lock));
        // ********************************************************************************************
    while(true){
        if ((!tp -> stop && tp -> work_count != 0) || (tp -> stop && tp -> thread_count != 0)){
            pthread_cond_wait(&(tp -> working_cond), &(tp -> lock));
        }
        else {
            break;
        }
    }
    // while ((!tp->stop && (tp->work_count != 0 || tp->work_queue.work_first != NULL)) || (tp->stop && tp->thread_count != 0))
    // {
    //     pthread_cond_wait(&(tp->work_cond), &(tp->lock));
    // }
        // ********************************************************************************************
    pthread_mutex_unlock(&(tp -> lock));

    // *************************************************************************************
    // printf("Tag F\n");
    ThreadPool_destroy(tp);

    ThreadPool_t *tpr =ThreadPool_create(num_reducers); 

    
    // int array[10]={0,1,2,3,4,5,6,7,8,9};
    for (int i=0; i<num_reducers; i++) {  
        // printf("Tag 1 ：%d:  \n",i); 
        ThreadPool_add_work(tpr,(thread_func_t) MR_ProcessPartition, new int(i));
       
    // printf("Tag D\n");
        
    }

    // *************************************************************************************
    // thread_pool_wait(tp_mapper);

    if (tpr == NULL){
        return;
    }
    pthread_mutex_lock(&(tpr -> lock));
        // ********************************************************************************************
    while(true){
        if ((!tpr -> stop && tpr -> work_count != 0) || (tpr -> stop && tpr -> thread_count != 0)){
            pthread_cond_wait(&(tpr -> working_cond), &(tpr -> lock));
        }
        else {
            break;
        }
    }

    // while ((!tpr->stop && (tpr->work_count != 0 || tpr->work_queue.work_first != NULL)) || (tpr->stop && tpr->thread_count != 0))
    // {
    //     pthread_cond_wait(&(tpr->work_cond), &(tpr->lock));
    // }
        // ********************************************************************************************
    pthread_mutex_unlock(&(tpr -> lock));

    // *************************************************************************************
    ThreadPool_destroy(tpr);

}
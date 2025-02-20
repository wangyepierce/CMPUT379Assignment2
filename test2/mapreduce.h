#ifndef MAPREDUCE_H
#define MAPREDUCE_H

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#include "threadpool.h"

#include <map>
#include <string>
#include <bits/stdc++.h> 
#include <string>
#include <iostream>

// function pointer types used by library functions
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, int partition_number);

void MR_Run(int num_files, char *filenames[],
            Mapper map, int num_mappers,
            Reducer concate, int num_reducers);

void MR_Emit(char *key, char *value);

unsigned long MR_Partition(char *key, int num_partitions);

void MR_ProcessPartition(int partition_number);

char *MR_GetNext(char *key, int partition_number);
#endif
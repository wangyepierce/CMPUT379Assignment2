#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include "threadpool.h"

ThreadPool_t *ThreadPool_create(int num){
    ThreadPool_t *tpool;
    pthread_t thread;
    if (num == 0){
        num = 2;
    }

    tpool = (ThreadPool_t *)calloc(1, sizeof(*tpool));
    tpool -> thread_count = num;
    tpool -> stop = false;

    pthread_mutex_init(&(tpool -> lock), NULL);
    pthread_cond_init(&(tpool -> work_cond), NULL);
    pthread_cond_init(&(tpool -> working_cond), NULL);

    ThreadPool_work_queue_t *wqueue;
    wqueue = &tpool -> work_queue;
    wqueue -> work_first = NULL;
    wqueue -> work_last = NULL;

    for (int i = 0; i < num; i++){
        pthread_create(&thread, NULL, Thread_run, tpool);
        pthread_detach(thread);
    }

    return tpool;
}

void ThreadPool_destroy(ThreadPool_t *tp){
    ThreadPool_work_t *work;
    ThreadPool_work_t *work2;

    if (tp == NULL){
        return;
    }

    pthread_mutex_lock(&(tp -> lock));
    ThreadPool_work_queue_t *wqueue;
    wqueue = &tp -> work_queue;
    work = wqueue -> work_first;
    while (work != NULL){
        work2 = work -> next;
        if (work == NULL){
            return;
        }
        work = work2;
    }
    tp -> stop = true;
    pthread_cond_broadcast(&(tp -> work_cond));
    pthread_mutex_unlock(&(tp -> lock));
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
    pthread_mutex_destroy(&(tp -> lock));
    pthread_cond_destroy(&(tp -> work_cond));
    pthread_cond_destroy(&(tp -> working_cond));
}

bool ThreadPool_add_work(ThreadPool_t *tp, thread_func_t func, void *arg){
    ThreadPool_work_t *work;

    if (tp == NULL){
        return false;
    }

    if (func == NULL){
        work = NULL;
    }
    work = (ThreadPool_work_t *) malloc(sizeof(*work));
    work -> func = func;
    work -> arg = arg;
    work -> next = NULL;

    if (work == NULL){
        return false;
    }

    pthread_mutex_lock(&(tp -> lock));
    ThreadPool_work_queue_t *wqueue;
    wqueue = &tp -> work_queue;

    if (wqueue -> work_first == NULL) {
        wqueue -> work_first = work;
        wqueue -> work_last = wqueue -> work_first;
    } else {
        wqueue -> work_last -> next = work;
        wqueue -> work_last = work;
    }

    pthread_cond_broadcast(&(tp -> work_cond));
    pthread_mutex_unlock(&(tp -> lock));

    return true;
}

ThreadPool_work_t *ThreadPool_get_work(ThreadPool_t *tp){
    ThreadPool_work_t *work;
    if (tp == NULL){
        return NULL;
    }

    ThreadPool_work_queue_t *wqueue;
    wqueue = &tp -> work_queue;

    work = wqueue -> work_first;
    if (work == NULL){
        return NULL;
    }

    if (work -> next == NULL){
        wqueue -> work_first = NULL;
        wqueue -> work_first = NULL;
    } else {
        wqueue -> work_first = work -> next;
    }

    return work;
}

void *Thread_run(void *arg){
    ThreadPool_t *tpool = (ThreadPool_t *) arg;
    ThreadPool_work_t *work;
    
    while (true) {
        pthread_mutex_lock(&(tpool -> lock));
        if (tpool -> stop){
            break;
        }

        ThreadPool_work_queue_t *wqueue;
        wqueue = &tpool -> work_queue;

        if (wqueue -> work_first == NULL) {
            pthread_cond_wait(&(tpool -> work_cond), &(tpool -> lock));
        }

        work = ThreadPool_get_work(tpool);
        tpool -> work_count ++;
        pthread_mutex_unlock(&(tpool -> lock));

        if (work != NULL){
            work -> func(work -> arg);
            // if (work == NULL){
            //     return;
            // }
        }

        pthread_mutex_lock(&(tpool -> lock));
        tpool -> work_count --;
        wqueue = &tpool -> work_queue;

        if (!tpool -> stop && tpool -> work_count == 0 && wqueue -> work_first == NULL){
            pthread_cond_signal(&(tpool -> working_cond));
        }
        pthread_mutex_unlock(&(tpool -> lock));

    }

    tpool -> thread_count --;
    pthread_cond_signal(&(tpool -> working_cond));
    pthread_mutex_unlock(&(tpool -> lock));
    return NULL;
}

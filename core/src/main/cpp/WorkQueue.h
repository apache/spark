#ifndef WORKQUEUE_H
#define WORKQUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <chrono>
#include <unistd.h>

template <typename T> class WorkQueue{
public:
    typedef T queue_type;
    std::queue<queue_type> _queue;
    std::mutex _queue_lock;
    //std::mutex cond_lock;
    //std::condition_variable m_cond;
    //std::unique_lock<std::mutex> unique_lock;

    WorkQueue(){}

    void enqueue( queue_type _work ){
        std::lock_guard<std::mutex> guard(_queue_lock);
        this->_queue.push( _work );
        //m_cond.notify_all();
    }

    queue_type dequeue(){
        //m_cond.wait(unique_lock);
        //m_cond.wait_for(unique_lock, std::chrono::milliseconds(50));
        usleep(5);
        if(!empty()){
            std::lock_guard<std::mutex> guard(_queue_lock);
            queue_type data = this->_queue.front();
            this->_queue.pop();
            return data;

        }else{
            return nullptr;
        }
    }

    bool empty(){
        std::lock_guard<std::mutex> guard(_queue_lock);
        return this->_queue.empty();
    }

    ssize_t size(){
        std::lock_guard<std::mutex> guard(_queue_lock);
        return this->_queue.size();
    }

    /*void wake_all(){
        m_cond.notify_all();
    }*/
};
#endif

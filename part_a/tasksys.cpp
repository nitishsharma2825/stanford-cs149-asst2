#include "tasksys.h"

#include <thread>
IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::thread_worker(IRunnable* runnable, int thread_id, int start_task, int end_task,
                                            int num_total_tasks) {
    for (int i = start_task; i < end_task; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // static distribution of tasks to threads
    int tasks_per_thread = num_total_tasks / num_threads_;
    int remaining_tasks = num_total_tasks % num_threads_;
    std::thread* threads = new std::thread[num_threads_];

    if (tasks_per_thread == 0) {
        // If there are fewer tasks than threads, just run the tasks sequentially
        for (int i = 0; i < num_total_tasks; i++) {
            runnable->runTask(i, num_total_tasks);
        }
        return;
    }

    for (int i = 0; i < num_threads_; i++) {
        int start_task = i * tasks_per_thread;
        int end_task = start_task + tasks_per_thread;
        if (i == num_threads_ - 1) {
            end_task += remaining_tasks;
        }
        threads[i] = std::thread(&TaskSystemParallelSpawn::thread_worker, this, runnable, i, start_task, end_task, num_total_tasks);
    }

    for (int i = 0; i < num_threads_; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads_];
    mutex_ = new std::mutex();
    num_cur_task_id_ = 0;
    num_total_tasks_ = 0;
    num_tasks_done_ = 0;
    runnable_ = nullptr;
    stop_threads_ = false;
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::thread_worker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    mutex_->lock();
    stop_threads_ = true;
    mutex_->unlock();

    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }

    delete[] threads_;
    delete mutex_;
}

void TaskSystemParallelThreadPoolSpinning::thread_worker(int thread_id) {
    while (true) {
        // check if there are tasks to run
        mutex_->lock();

        // check if we should stop the thread
        if (stop_threads_) {
            mutex_->unlock();
            break;
        }

        if (num_cur_task_id_ < num_total_tasks_) {
            int task_id = num_cur_task_id_++;
            mutex_->unlock();
            runnable_->runTask(task_id, num_total_tasks_);

            mutex_->lock();
            num_tasks_done_++;
            mutex_->unlock();
        } else {
            mutex_->unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    mutex_->lock();
    num_cur_task_id_ = 0;
    num_tasks_done_ = 0;
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    mutex_->unlock();

    // spinning locks, wasting CPU
    while (true) {
        mutex_->lock();
        if (num_tasks_done_ == num_total_tasks_) {
            mutex_->unlock();
            break;
        }
        mutex_->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads_];
    mutex_ = new std::mutex();
    work_todo_ = new std::condition_variable();
    work_done_ = new std::condition_variable();
    work_todo_flag_ = false;
    work_done_flag_ = false;
    num_cur_task_id_ = 0;
    num_tasks_done_ = 0;
    num_total_tasks_ = 0;
    runnable_ = nullptr;
    stop_threads_ = false;
    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::thread_worker, this, i);
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_worker(int thread_id) {
    while (true) {
        // check if there are tasks to run
        std::unique_lock<std::mutex> lock(*mutex_);
        while (!work_todo_flag_ && !stop_threads_) {
            work_todo_->wait(lock);
        }

        // check if we should stop the thread
        if (stop_threads_) {
            break;
        }

        if (num_cur_task_id_ < num_total_tasks_) {
            int task_id = num_cur_task_id_++;
            lock.unlock();
            runnable_->runTask(task_id, num_total_tasks_);

            lock.lock();
            num_tasks_done_++;
            lock.unlock();
        } else {
            if (num_tasks_done_ == num_total_tasks_) {
                work_done_flag_ = true;
                // Whole batch is finished: clear the flag so workers go back to sleep
                work_todo_flag_ = false;
            }
            lock.unlock();
            work_done_->notify_all();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    mutex_->lock();
    stop_threads_ = true;
    mutex_->unlock();

    work_todo_->notify_all();

    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }

    delete[] threads_;
    delete mutex_;
    delete work_todo_;
    delete work_done_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    mutex_->lock();
    num_cur_task_id_ = 0;
    num_tasks_done_ = 0;
    num_total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    work_todo_flag_ = true;
    work_done_flag_ = false;
    mutex_->unlock();

    work_todo_->notify_all();

    // condition variable sleep
    std::unique_lock<std::mutex> lock(*mutex_);
    while(!work_done_flag_ ) {
        work_done_->wait(lock);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

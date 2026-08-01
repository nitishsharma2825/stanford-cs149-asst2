#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

void TaskSystemParallelThreadPoolSleeping::task_worker(int thread_id) {
    while (true) {
        // check if there is work to do
        std::unique_lock<std::mutex> lock(*mutex_);
        while(work_queue_.empty() && !stop_threads_) {
            work_todo_->wait(lock);
        }

        if (stop_threads_) {
            break;
        }

        // get the next work order
        WorkOrder work_order = work_queue_.back();
        work_queue_.pop_back();
        lock.unlock();

        // execute the work order
        for (int task_id = 0; task_id < work_order.num_total_tasks_; task_id++) {
            work_order.runnable_->runTask(task_id, work_order.num_total_tasks_);
        }

        lock.lock();
        completed_tasks_.insert(work_order.task_id_);
        if (completed_tasks_.size() == num_tasks_requested) {
            work_done_->notify_all();
        } else {
            check_for_work_->notify_all();
        }
        lock.unlock();
    }
}

void TaskSystemParallelThreadPoolSleeping::task_controller(int thread_id) {
    while (true) {
        std::unique_lock<std::mutex> lock(*mutex_);
        while (todo_queue_.empty() && !stop_threads_) {
            check_for_work_->wait(lock);
        }

        if (stop_threads_) {
            break;
        }

        // check if any work orders can be moved to the work queue
        for (auto it = todo_queue_.begin(); it != todo_queue_.end();) {
            WorkOrder work_order = *it;
            bool all_deps_completed = true;
            for (TaskID dep : work_order.deps_) {
                if (completed_tasks_.find(dep) == completed_tasks_.end()) {
                    all_deps_completed = false;
                    break;
                }
            }
            if (all_deps_completed) {
                work_queue_.push_back(work_order);
                it = todo_queue_.erase(it);
                work_todo_->notify_all();
            } else {
                ++it;
            }
        }

        if (completed_tasks_.size() == num_tasks_requested) {
            work_done_->notify_all();
        }
    }
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
    check_for_work_ = new std::condition_variable();
    cur_task_id_ = 0;
    num_tasks_requested = 0;
    stop_threads_ = false;
    completed_tasks_ = std::set<TaskID>();
    todo_queue_ = std::vector<WorkOrder>();
    work_queue_ = std::vector<WorkOrder>();

    // thread 0 will be the controller thread, and the rest will be worker threads
    for (int i = 0; i < num_threads_; i++) {
        if (i == 0) {
            threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::task_controller, this, i);
        } else {
            threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::task_worker, this, i);
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
    check_for_work_->notify_all();

    num_tasks_requested = 0;
    completed_tasks_.clear();
    todo_queue_.clear();
    work_queue_.clear();

    for (int i = 0; i < num_threads_; i++) {
        threads_[i].join();
    }

    delete[] threads_;
    delete mutex_;
    delete work_todo_;
    delete work_done_;
    delete check_for_work_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lock(*mutex_);
    num_tasks_requested += 1;
    TaskID task_id = cur_task_id_++;

    WorkOrder work_order(task_id, runnable, num_total_tasks, std::vector<TaskID>());
    work_queue_.push_back(work_order);
    work_todo_->notify_all();

    while (completed_tasks_.size() < num_tasks_requested) {
        work_done_->wait(lock);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    mutex_->lock();
    num_tasks_requested += 1;
    TaskID task_id = cur_task_id_++;

    // check if all dependencies are completed
    bool all_deps_completed = true;
    for (TaskID dep : deps) {
        if (completed_tasks_.find(dep) == completed_tasks_.end()) {
            all_deps_completed = false;
            break;
        }
    }

    WorkOrder work_order(task_id, runnable, num_total_tasks, deps);

    if (all_deps_completed) {
        // add to work queue
        work_queue_.push_back(work_order);
        work_todo_->notify_all();
    } else {
        // add to todo queue
        todo_queue_.push_back(work_order);
    }

    mutex_->unlock();
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(*mutex_);
    while (completed_tasks_.size() < num_tasks_requested) {
        work_done_->wait(lock);
    }
    return;
}

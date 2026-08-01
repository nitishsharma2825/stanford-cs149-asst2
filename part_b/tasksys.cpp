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
        work_order.runnable_->runTask(work_order.task_id_, work_order.num_total_tasks_);

        lock.lock();
        // check if this launch has completed all its tasks
        tasks_per_launch_left_[work_order.launch_id_]--;
        if (tasks_per_launch_left_[work_order.launch_id_] == 0) {
            completed_tasks_[work_order.launch_id_] = true;
            num_launches_completed++;

            // notify any tasks that are dependent on this launch that they can now be executed if all their dependencies are met
            for (TaskID dep : tasks_deps_[work_order.launch_id_]) {
                tasks_deps_left_[dep]--;

                // if all dependencies for this task are met, add it to the work queue
                if (tasks_deps_left_[dep] == 0) {
                    // move tasks for this launch from todo_map_ to work_queue_
                    // there will be one work order in todo_queue_ for this launch, which will be expanded into individual work orders for each task in the launch
                    WorkOrder launch_work_order = todo_map_[dep];
                    for (int i=0; i<launch_work_order.num_total_tasks_; i++) {
                        WorkOrder individual_work_order(dep, i, launch_work_order.runnable_, launch_work_order.num_total_tasks_);
                        work_queue_.push_back(individual_work_order);
                    }
                    todo_map_.erase(dep);

                    work_todo_->notify_all();
                }
            }
        }

        // notify the sync if all launches have completed
        if (num_launches_completed == num_launches_requested) {
            work_done_->notify_all();
        }

        lock.unlock();
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
    tasks_per_launch_left_ = std::vector<int>();
    tasks_deps_left_ = std::vector<int>();
    tasks_deps_ = std::vector<std::vector<TaskID>>();
    cur_task_id_ = 0;
    num_launches_requested = 0;
    num_launches_completed = 0;
    stop_threads_ = false;
    completed_tasks_ = std::vector<bool>();
    todo_map_ = std::unordered_map<TaskID, WorkOrder>();
    work_queue_ = std::vector<WorkOrder>();

    for (int i = 0; i < num_threads_; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::task_worker, this, i);
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
    num_launches_requested = 0;
    num_launches_completed = 0;
    mutex_->unlock();

    work_todo_->notify_all();
    work_done_->notify_all();

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
    std::unique_lock<std::mutex> lock(*mutex_);
    num_launches_requested += 1;
    TaskID task_id = cur_task_id_++;

    for (int i=0; i<num_total_tasks; i++) {
        WorkOrder work_order(task_id, i, runnable, num_total_tasks);
        work_queue_.push_back(work_order);
    }
    tasks_per_launch_left_.push_back(num_total_tasks);
    completed_tasks_.push_back(false);
    tasks_deps_left_.push_back(0);
    tasks_deps_.push_back(std::vector<TaskID>());
    work_todo_->notify_all();

    while(num_launches_completed != num_launches_requested) {
        work_done_->wait(lock);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    mutex_->lock();
    num_launches_requested += 1;
    TaskID task_id = cur_task_id_++;

    // check if all dependencies are completed
    int tasks_deps_not_ready = 0;
    for (TaskID dep : deps) {
        if (completed_tasks_[dep] == false) {
            tasks_deps_not_ready++;
        }
    }

    // create granular work order for this task and fill the reverse dependency graph for this task
    completed_tasks_.push_back(false);
    tasks_deps_.push_back(std::vector<TaskID>());
    tasks_per_launch_left_.push_back(num_total_tasks);
    if (tasks_deps_not_ready == 0) {
        for (int i=0; i<num_total_tasks; i++) {
            WorkOrder individual_work_order(task_id, i, runnable, num_total_tasks);
            work_queue_.push_back(individual_work_order);
        }
        tasks_deps_left_.push_back(0);
        work_todo_->notify_all();
    } else {
        // only create one work order for this task, and add it to the todo queue
        // when moving to work_queue create individual work orders for each task
        WorkOrder individual_work_order(task_id, -1, runnable, num_total_tasks);
        todo_map_[task_id] = individual_work_order;
        tasks_deps_left_.push_back(tasks_deps_not_ready);
        for (TaskID dep : deps) {
            tasks_deps_[dep].push_back(task_id);
        }
    }

    mutex_->unlock();
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::unique_lock<std::mutex> lock(*mutex_);
    while(num_launches_completed != num_launches_requested) {
        work_done_->wait(lock);
    }
    return;
}

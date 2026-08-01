#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <set>
#include <map>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class WorkOrder;

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

        int num_threads_;
        int cur_task_id_;
        std::thread* threads_;
        void task_worker(int thread_id);

        std::mutex* mutex_;

        // there is work in the work_queue_ to be done
        std::condition_variable* work_todo_;

        // all work done signal
        std::condition_variable* work_done_;

        std::unordered_map<TaskID, WorkOrder> todo_map_;

        std::vector<WorkOrder> work_queue_;

        // for each launch of tasks, how many tasks are left to be completed
        std::vector<int> tasks_per_launch_left_;

        // how many tasks I am dependent on that are left to be completed before I can be launched
        std::vector<int> tasks_deps_left_;

        // reverse dependency graph, for each task, which tasks are dependent on me
        std::vector<std::vector<TaskID>> tasks_deps_;

        std::vector<bool> completed_tasks_;
        int num_launches_requested;
        int num_launches_completed;

        bool stop_threads_;
};

/*
 Wrapper class around a task which needs to be done
 */
class WorkOrder {
    public:
        TaskID launch_id_;
        int task_id_;
        IRunnable* runnable_;
        int num_total_tasks_;
        WorkOrder() : launch_id_(0), task_id_(-1), runnable_(nullptr), num_total_tasks_(0) {}
        WorkOrder(TaskID launch_id, int task_id, IRunnable* runnable, int num_total_tasks) {
            launch_id_ = launch_id;
            task_id_ = task_id;
            runnable_ = runnable;
            num_total_tasks_ = num_total_tasks;
        }
};

#endif

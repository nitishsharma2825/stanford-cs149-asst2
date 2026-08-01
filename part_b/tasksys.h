#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <set>

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
        void task_controller(int thread_id);

        std::mutex* mutex_;

        // there is work in the work_queue_ to be done
        std::condition_variable* work_todo_;

        // all work done if completed_tasks_.size() == num_tasks_requested
        std::condition_variable* work_done_;

        // check if there is work to do after this work order is completed, signal controller to wake up and check if there is work to do
        std::condition_variable* check_for_work_;

        std::vector<WorkOrder> todo_queue_;
        std::vector<WorkOrder> work_queue_;

        std::set<TaskID> completed_tasks_;

        int num_tasks_requested;

        bool stop_threads_;
};

/*
 Wrapper class around a task which needs to be done
 */
class WorkOrder {
    public:
        TaskID task_id_;
        IRunnable* runnable_;
        int num_total_tasks_;
        std::vector<TaskID> deps_;
        WorkOrder(TaskID task_id, IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
            task_id_ = task_id;
            runnable_ = runnable;
            num_total_tasks_ = num_total_tasks;
            deps_ = deps;
        }
};

#endif

#define TASK_EXECUTOR_IMPLEMENTATION
#define TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW   TASK_EXECUTOR_DROP
#define TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW TASK_EXECUTOR_DROP
//#define TASK_EXECUTOR_LOGGER(...)
#define MAX_RETURN_SIZE 24
#include "task_executor.h"

#include <vector>
#include <unordered_map>


struct MyThing
{
    int  size;
    int* data;
};

struct OtherThing
{
    int  size;
    int* data;
};


MyThing heavy_task()
{
    int  size = 1024 * 1024;
    auto data = (int*) malloc(size * sizeof(int));

    for (int i = 0; i < size; ++i)
    {
        data[i] = i;
    }

    return { size, data };
}


OtherThing varying_task(int size)
{
    auto data = (int*) malloc(size * sizeof(int));

    data[0] = 2;
    for (int i = 1; i < size - 1; ++i)
    {
        data[i] = i;
    }
    data[size-1] = 3;

    return { size, data };
}


void test_executing_tasks()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor::initialize(8);

    std::vector<task_executor::task_result<MyThing>> tasks = {
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
            task_executor::create_task<heavy_task>(),
    };

    task_executor::wait_for_all(tasks);

    for (auto& task : tasks)
    {
        MyThing thing = task.get();
        assert(thing.size == 1024 * 1024);
        free(thing.data);
    }

    task_executor_terminate_and_wait();
}

void test_poll_tasks()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor::initialize(8);

    std::unordered_map<int, task_executor::task_result<MyThing>> tasks;
    tasks.reserve(16);
    for (int i = 0; i < tasks.size(); ++i)
    {
        auto task = task_executor::create_task<heavy_task>();
        tasks[task.get_id()] = task;
    }

    int results = tasks.size();
    while (results != 0)
    {
        int task_id = poll_result();
        if (task_id >= 0)
        {
            auto it = tasks.find(task_id);
            auto& task = it->second;
            MyThing thing = task.get();
            assert(thing.size == 1024 * 1024);
            tasks.erase(it);
            free(thing.data);
        }
    }

    task_executor::terminate_and_wait();
}

void test_executing_and_requesting_tasks_interleaved()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor::initialize(8);

    int task_count = 1000000;

    int counter = 0;

    int results = 0;
    int queries = 0;

    std::unordered_map<int, task_executor::task_result<MyThing>>    tasks_1;
    std::unordered_map<int, task_executor::task_result<OtherThing>> tasks_2;
    tasks_1.reserve(task_count/2);
    tasks_2.reserve(task_count/2);

    while (results < task_count - (g_dropped_tasks + g_dropped_responses + g_dropped_results))
    {
        int task_id = poll_result();
        if (task_id >= 0)
        {
            auto it = tasks_1.find(task_id);
            if (it != tasks_1.end())
            {
                auto& task = it->second;
                MyThing thing = task.get();
                assert(thing.size == 1024 * 1024);
                assert(thing.data[0] == 0 && thing.data[127] == 127);
                tasks_1.erase(it);
                free(thing.data);
            }
            else
            {
                auto it_2 = tasks_2.find(task_id);
                auto& task = it_2->second;
                OtherThing thing = task.get();
                assert(thing.data[0] == 2 && thing.data[127] == 3);
                counter += thing.data[0];
                counter += thing.data[127];
                tasks_2.erase(it_2);
                free(thing.data);
            }

            results += 1;
        }
        else if (queries < task_count)
        {
            if (queries % 2 == 0)
            {
                auto task = task_executor::create_task<heavy_task>();
                tasks_1[task.get_id()] = task;
            }
            else
            {
                auto task = task_executor::create_task<int, varying_task>(128);
                tasks_2[task.get_id()] = task;
            }

            queries += 1;
        }
    }

    assert(counter == (2 + 3) * (results / 2));

    task_executor::terminate_and_wait();
}



int main()
{
    test_executing_tasks();
    test_poll_tasks();
    test_executing_and_requesting_tasks_interleaved();
}
#define TASK_EXECUTOR_IMPLEMENTATION
#define TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW   TASK_EXECUTOR_DROP
#define TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW TASK_EXECUTOR_DROP
//#define TASK_EXECUTOR_LOGGER(...)
#define MAX_RETURN_SIZE 24
#include "task_executor.h"


struct MyThing
{
    int  size;
    int* data;
};


struct MyThing heavy_task()
{
    int  size = 1024 * 1024;
    int* data = malloc(size * sizeof(int));

    for (int i = 0; i < size; ++i)
    {
        data[i] = i;
    }

    return (struct MyThing) { size, data };
}


struct MyThing varying_task(int size)
{
    int* data = malloc(size * sizeof(int));

    data[0] = 2;
    for (int i = 1; i < size - 1; ++i)
    {
        data[i] = i;
    }
    data[size-1] = 3;

    return (struct MyThing) { size, data };
}

TASK_WRAPPER(heavy_task,  struct MyThing);
TASK_WRAPPER_WITH_ARG(varying_task, struct MyThing, int);


void test_executing_tasks()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor_initialize_with_thread_count(8);

    int tasks[] = {
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
    };
    int task_count = sizeof(tasks) / sizeof(tasks[0]);

    wait_for_all(tasks, task_count);

    for (int i = 0; i < task_count; ++i)
    {
        struct MyThing thing;
        assert(try_get_task_result(tasks[i], &thing));
        assert(thing.size == 1024 * 1024);
        free(thing.data);
    }

    task_executor_terminate_and_wait();
}

void test_poll_tasks()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor_initialize_with_thread_count(8);

    int tasks[] = {
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
            create_task(heavy_task_task_wrapper, struct MyThing),
    };
    int task_count = sizeof(tasks) / sizeof(tasks[0]);

    int results = task_count;
    while (results != 0)
    {
        int task = poll_result();
        if (task >= 0)
        {
            struct MyThing thing;
            assert(try_get_task_result(task, &thing));
            assert(thing.size == 1024 * 1024);
            free(thing.data);
            results -= 1;
        }

    }

    task_executor_terminate_and_wait();
}

void test_executing_and_requesting_tasks_interleaved()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor_initialize_with_thread_count(8);

    int task_count = 1000000;

    int results = 0;
    int queries = 0;
    while (results < task_count - (g_dropped_tasks + g_dropped_responses + g_dropped_results))
    {
        int task = poll_result();
        if (task >= 0)
        {
            struct MyThing thing;
            assert(try_get_task_result(task, &thing));
            assert(thing.data[0] == 2 && thing.data[127] == 3);
            free(thing.data);
            results += 1;
        }
        else if (queries < task_count)
        {
            create_task_with_arg(varying_task_task_wrapper, struct MyThing, 128);
            queries += 1;
        }
    }

    task_executor_terminate_and_wait();
}



int main()
{
    test_executing_tasks();
    test_poll_tasks();
    test_executing_and_requesting_tasks_interleaved();
}
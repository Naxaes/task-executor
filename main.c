#define TASK_EXECUTOR_IMPLEMENTATION
#define TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW   TASK_EXECUTOR_DROP
#define TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW TASK_EXECUTOR_DROP
#define TASK_EXECUTOR_LOGGER(...)
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


int varying_task_other(int size)
{
    int result = 0;
    for (int i = 0; i < size; ++i)
    {
        result += i;
    }

    return result;
}

double varying_task_other_float(double number)
{
    double result = 10.0 * number * number;
    return result;
}



TASK_WRAPPER(heavy_task, struct MyThing)
TASK_WRAPPER_WITH_ARG(varying_task, struct MyThing, int)
TASK_WRAPPER_WITH_ARG(varying_task_other, int, int)
TASK_WRAPPER_WITH_ARG(varying_task_other_float, double, double)


void test_executing_tasks()
{
    printf("-------- TEST: test_executing_tasks --------\n");
    task_executor_initialize_with_thread_count(8);

    int tasks[] = {
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
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
    printf("-------- DONE: test_executing_tasks --------\n");
}

void test_poll_tasks()
{
    printf("-------- TEST: test_poll_tasks --------\n");
    task_executor_initialize_with_thread_count(8);

    int tasks[] = {
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
            create_task(heavy_task, struct MyThing),
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
    printf("-------- DONE: test_poll_tasks --------\n");
}

void test_executing_and_requesting_tasks_interleaved()
{
    printf("-------- TEST: test_executing_and_requesting_tasks_interleaved --------\n");
    task_executor_initialize_with_thread_count(8);

    int task_count = 1000000;

    int results = 0;
    int queries = 0;
    while (results < task_count - g_dropped_tasks)
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
            int a = 128;
            create_task_with_arg(varying_task, struct MyThing, a);
            queries += 1;
        }
    }

    task_executor_terminate_and_wait();
    printf("-------- DONE: test_executing_and_requesting_tasks_interleaved --------\n");
}


void test_executing_and_requesting_tasks_interleaved_with_multiple_types()
{
    printf("-------- TEST: test_executing_and_requesting_tasks_interleaved_with_multiple_types --------\n");
    task_executor_initialize_with_thread_count(8);

    int task_count = 1000000;

    int results = 0;
    int queries = 0;
    while (results < task_count - g_dropped_tasks)
    {
        int task = poll_result();
        if (task >= 0)
        {
            int return_id = get_return_type(task);
            {
                if (return_id == varying_task_return_id)
                {
                    struct MyThing result;
                    get_task_result(task, &result);
                    assert(result.data[0] == 2 && result.data[127] == 3);
                    free(result.data);
                }
                else if (return_id == varying_task_other_return_id)
                {
                    int result;
                    get_task_result(task, &result);
                    assert(result == 45);
                }
                else if (return_id == varying_task_other_float_return_id)
                {
                    double result;
                    get_task_result(task, &result);
                    double expected = 10.0 * 42.0 * 42.0;
                    assert(-0.001 <= expected - result && expected - result <= 0.001);
                }
                else
                {
                    assert(0);
                }
            }
            results += 1;
        }
        else if (queries < task_count)
        {
            int a = 128;
            int b = 10;
            double c = 42.0;

            if (queries % 3 == 0)
                create_task_with_arg(varying_task, struct MyThing, a);
            else if (queries % 3 == 1)
                create_task_with_arg(varying_task_other, int, b);
            else
                create_task_with_arg(varying_task_other_float, double, c);

            queries += 1;
        }
    }

    task_executor_terminate_and_wait();
    printf("-------- DONE: test_executing_and_requesting_tasks_interleaved_with_multiple_types --------\n");
}


void test_better_interface()
{
    task_executor_initialize_with_thread_count(8);

    const size_t task_to_execute = 1000;
    size_t queries = 0;
    while (queries < task_to_execute || tasks_in_progress())
    {
        int task = poll_result();
        int return_id = get_return_type(task);
        {
            if (return_id == varying_task_return_id)
            {
                struct MyThing result;
                get_task_result(task, &result);
                assert(result.data[0] == 2 && result.data[127] == 3);
                free(result.data);
            }
            else if (return_id == varying_task_other_return_id)
            {
                int result;
                get_task_result(task, &result);
                assert(result == 45);
            }
            else if (return_id == varying_task_other_float_return_id)
            {
                double result;
                get_task_result(task, &result);
                double expected = 10.0 * 42.0 * 42.0;
                assert(-0.001 <= expected - result && expected - result <= 0.001);
            }
            else
            {
                if (queries >= task_to_execute)
                    break;

                int a = 128;
                int b = 10;
                double c = 42.0;

                if (queries % 3 == 0)
                    create_task_with_arg(varying_task, struct MyThing, a);
                else if (queries % 3 == 1)
                    create_task_with_arg(varying_task_other, int, b);
                else
                    create_task_with_arg(varying_task_other_float, double, c);

                queries += 1;
            }
        }
    }

    while (tasks_in_progress())
    {
        char result[255] = { 0 };
        int task = wait_for_one();
        get_task_result(task, &result);
    }

    assert(g_tasks_left == 0);
    task_executor_terminate_and_wait();
}


#include <unistd.h>
#include <time.h>
#include <stdlib.h>
void random_task()
{
    int micro_seconds = rand() % 1000000;
    printf("Sleeping for %d micro seconds\n", micro_seconds);
    usleep(micro_seconds);
}

TASK_WRAPPER_NO_RETURN(random_task)

void test_executing_tasks_of_random_time()
{
    srand(time(NULL));
    task_executor_initialize_with_thread_count(4);

    int results = 0;
    int queries = 0;
    const int task_to_execute = 10000;
    while (queries < task_to_execute || tasks_in_progress())
    {
        int task = poll_result();
        int return_id = get_return_type(task);
        {
            if (return_id == NO_RETURN_ID)
            {
                results += 1;
            }
            else
            {
                if (queries >= task_to_execute)
                    break;

                create_task_no_return(random_task);

                queries += 1;
            }
        }
    }

    while (tasks_in_progress())
    {
        wait_for_one();
        results += 1;
    }

    assert(results == task_to_execute - g_dropped_tasks);
    task_executor_terminate_and_wait();
}


int main(int argc, const char* argv[])
{
    test_executing_tasks();
    test_poll_tasks();
    test_executing_and_requesting_tasks_interleaved();
    test_executing_and_requesting_tasks_interleaved_with_multiple_types();
    test_better_interface();
    test_executing_tasks_of_random_time();
}

#define TASK_EXECUTOR_IMPLEMENTATION
#define TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW   TASK_EXECUTOR_DROP
#define TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW TASK_EXECUTOR_DROP
//#define TASK_EXECUTOR_LOGGER(...)
#define MAX_PENDING_RESPONSES_COUNT 8
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

double varying_task_other_double(double number)
{
    double result = 10.0 * number * number;
    return result;
}



TASK_WRAPPER(heavy_task, struct MyThing)
TASK_WRAPPER_WITH_ARG(varying_task, struct MyThing, int)
TASK_WRAPPER_WITH_ARG(varying_task_other, int, int)
TASK_WRAPPER_WITH_ARG(varying_task_other_double, double, double)

#include <unistd.h>

void test_better_interface()
{
    task_executor_initialize_with_thread_count(8);

    const size_t task_to_execute = 1000000;
    size_t queries = 0;

    while (queries < task_to_execute || tasks_are_remaining())
    {
        const Response* response = poll_for_next_result();
        if (response)
        {
            if (response->return_id == varying_task_return_id)
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 1\n");
                struct MyThing result;
                get_task_result(&response, &result);
                assert(result.data[0] == 2 && result.data[127] == 3);
                free(result.data);
            }
            else if (response->return_id == varying_task_other_return_id)
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 2\n");
                int result;
                get_task_result(&response, &result);
                assert(result == 45);
            }
            else if (response->return_id == varying_task_other_double_return_id)
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 3\n");
                double result;
                get_task_result(&response, &result);
                double expected = 10.0 * 42.0 * 42.0;
                assert(-0.001 <= expected - result && expected - result <= 0.001);
            }
            else
            {
                assert(0);
            }
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
                create_task_with_arg(varying_task_other_double, double, c);

            queries += 1;
        }
    }

    while (tasks_are_remaining())
    {
        const Response* response = wait_for_next_task();
        consume_task_result(&response);
    }

    assert(g_tasks_left == 0);
    task_executor_terminate_and_wait();
}



int main(int argc, const char* argv[])
{
    test_better_interface();
}

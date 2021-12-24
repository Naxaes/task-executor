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
    int* data = (int*) malloc(size * sizeof(int));

    for (int i = 0; i < size; ++i)
    {
        data[i] = i;
    }

    return (struct MyThing) { size, data };
}


struct MyThing varying_task(int size)
{
    int* data = (int*) malloc(size * sizeof(int));

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

void no_return_task_other_float(double number)
{

}


#include <unistd.h>

void test_better_interface()
{
    task_executor::initialize(8);

    const size_t task_to_execute = 1000000;
    size_t queries = 0;

    while (queries < task_to_execute || task_executor::tasks_are_remaining())
    {
        task_executor::Response response = task_executor::poll_for_next_result();
        if (response.has_result())
        {
            if (response.is_return_type<MyThing>())
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 1\n");
                auto result = response.get_result<MyThing>();
                assert(result.data[0] == 2 && result.data[127] == 3);
                free(result.data);
            }
            else if (response.is_return_type<int>())
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 2\n");
                auto result = response.get_result<int>();
                assert(result == 45);
            }
            else if (response.is_return_type<double>())
            {
                TASK_EXECUTOR_LOGGER("[Main]: Got result 3\n");
                auto result = response.get_result<double>();
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

            if (queries % 3 == 0)
                task_executor::create_task<int, varying_task>(128);
            else if (queries % 3 == 1)
                task_executor::create_task<int, varying_task_other>(10);
            else
                task_executor::create_task<double, varying_task_other_double>(42.0);

            queries += 1;
        }
    }

    while (task_executor::tasks_are_remaining())
    {
        task_executor::Response response = task_executor::wait_for_next_task();
        response.consume_task_result();
    }

    assert(task_executor::tasks_left() == 0);
    task_executor::terminate_and_wait();
}



int main(int argc, const char* argv[])
{
    test_better_interface();
}

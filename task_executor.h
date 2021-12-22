/**
 * 1. First initialize the library, which will create worker threads.
 * 2. Then create tasks. These will send the function (and its argument) to a
 *    worker. The function must have the interface `TaskFunction`. Two macros,
 *    `TASK_WRAPPER` and `TASK_WRAPPER_WITH_ARG` can generate a `TaskFunction`
 *    for any function that takes one or two arguments. Other functions have to
 *    be created manually. For C++, you can use the templated function
 *    `create_task`. Once a task is created, it's put in the task buffer. The
 *    task will be removed from the buffer when it's ready to start executing.
 *    When the task completes, a response will be put in the response buffer
 *    and the result in result buffer.
 * 3. To check whether a task is done, you can check the response buffer with
 *    `poll_result` or alternatively wait with `wait_for_one` and
 *    `wait_for_all`. These will remove the valid response(s) from the buffer.
 * 4. To get the result from a completed task, use `try_get_task_result`. You
 *    are responsible for providing the correct return type, unless you use the
 *    `task_result<T>` and `task_result<T>::try_get` or `task_result<T>::get`
 *    (C++ only). Once you get the result, it'll be removed from the result
 *    buffer.
 * 5. When you're done you can terminate with `task_executor_terminate`, or
 *    `task_executor::terminate` Unless you're planning for your program to
 *    continue afterwards, it's better to just let the process terminate
 *    naturally and let the OS clean up the threads.
 *
 *
 * ---- NOTICE ----
 * 1. Once you've received a valid result from `try_get_task_result`,
 *    `task_result<T>::try_get` or `task_result<T>::get` you're **not** allowed
 *    to use the task id or `task_result` anymore. Task id's will be reused.
 * 2. You're responsible for making sure all arguments to the task lives and
 *    are valid until the task has completed. Modifying or reading the
 *    arguments before the task has completed is not thread-safe unless you've
 *    taken own actions to ensure it.
 * 3. You're not allowed to pass in a modified task id. The task id's **must**
 *    be considered read-only.
 * 4. You should not create a task after `wait_for_all` unless you've fetched
 *    the results. This is because the response buffer might overflow.
 * 5. The input parameter can only be as big as one void*. If you need to pass
 *    bigger data structure or more parameters, you need to pass a pointer to
 *    it/them.
 *
 *
 * ---- TODO ----
 * 1. The C++ version is a bit janky and doesn't work with objects that have
 *    a destructor.
 * 2. I think we can omit the response buffer and just check the results
 *    directly.
 */
#ifdef __cplusplus
extern "C" {
#endif



/// Will create the worker threads and setup the task, response, and result
/// buffer.
void task_executor_initialize();
void task_executor_initialize_with_thread_count(int thread_count);

/// Will send the workers a "terminate" task.
void task_executor_terminate();
void task_executor_terminate_and_wait();

/// Wait for any task to complete. Always returns 0 or greater.
int wait_for_one();
/// Wait for all tasks to complete.
void wait_for_all(const int* task_id_array, int count);
/// Check whether a task has completed. Returns less than 0 if none has.
int poll_result();

/// The amount of worker threads currently running.
int get_thread_count();


/// Creates a task and puts it in the task buffer. The `arg` **must** be valid
/// until the `TaskResult` is available.
#define create_task_with_arg(function, return_type, arg)  do { void* x; memcpy(&x, &arg, sizeof(x)); task_executor_create_task_impl(function ## _task_wrapper, function ## _return_id, sizeof(return_type), x); } while (0)
#define create_task(function, return_type)                task_executor_create_task_impl(function ## _task_wrapper, function ## _return_id, sizeof(return_type), 0)

/// Sets the `result` if the task is done or panics. The `task_id` **must not**
/// be used afterwards.
#define get_task_result(task_id, result)   task_executor_get_task_result_impl(task_id, result, sizeof(*result))

/// Sets the `result` if the task is done. Returns 0 if a task hasn't completed
/// and 1 otherwise. If the function returns 1, then the `task_id` **must not**
/// be used again.
#define try_get_task_result(task_id, result)   task_executor_try_get_task_result_impl(task_id, result, sizeof(*result))
/// Same as `try_get_task_result` but waits and always returns a valid result.
/// When the function returns , then the `task_id` **must not** be used again.
#define wait_for_task_result(task_id, result)  task_executor_wait_for_task_result_impl(task_id, result, sizeof(result))


/// The amount of threads to create as default.
#ifndef DEFAULT_THREAD_COUNT
#define DEFAULT_THREAD_COUNT 8
#endif

/// The maximum size a return value of a task can have. Will be
/// `TASK_EXECUTOR_RESULT_META_DATA_SIZE` bytes smaller than the result buffer
/// block, which must be a power of 2. This is because metadata needs to be
/// stored in the result buffer block.
#ifndef MAX_RETURN_SIZE
#define MAX_RETURN_SIZE 48
#endif

/// The maximum amount of pending tasks.
#ifndef MAX_PENDING_TASKS_COUNT
#define MAX_PENDING_TASKS_COUNT 64
#endif

/// The maximum amount of pending responses. Set this to
/// `MAX_PENDING_TASKS_COUNT` if you use `wait_for_all`,
/// otherwise you can set this to something lower (at own risk).
#ifndef MAX_PENDING_RESPONSES_COUNT
#define MAX_PENDING_RESPONSES_COUNT MAX_PENDING_TASKS_COUNT
#endif

/// The maximum amount of results that can be stored. Set this
/// `MAX_PENDING_RESPONSES_COUNT` if you use `wait_for_all`.
/// otherwise you can set this to something lower (at own risk).
#ifndef MAX_IDLE_RESULT_COUNT
#define MAX_IDLE_RESULT_COUNT MAX_PENDING_RESPONSES_COUNT
#endif


/// How to log info. The macro needs to accept one format message and variadic
/// parameters, i.e. have the same signature as `printf`.
#ifndef TASK_EXECUTOR_LOGGER
#define TASK_EXECUTOR_LOGGER printf
#endif

/// Strategies for buffer overflows.
#define TASK_EXECUTOR_PANIC 0
#define TASK_EXECUTOR_DROP  1

#ifndef TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW
#define TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW TASK_EXECUTOR_DROP
#endif

#ifndef TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW
#define TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW TASK_EXECUTOR_PANIC
#endif

#ifndef TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW
#define TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW TASK_EXECUTOR_PANIC
#endif


#define STATIC_ASSERT(condition, message) typedef char static_assertion_##message[(condition) ? 1: -1]
#define PANIC(message) do { fprintf(stderr, message); exit(-1); } while (0)


struct return_type_erasure { char data[MAX_RETURN_SIZE]; };
typedef struct return_type_erasure (*TaskFunction)(void*);

#define TASK_WRAPPER(function, type)                                           \
const int function ## _return_id = __COUNTER__;                                \
struct return_type_erasure function ## _task_wrapper(__unused void* param)     \
{                                                                              \
    type thing = function();                                                   \
    STATIC_ASSERT(sizeof(thing) <= MAX_RETURN_SIZE, MAX_RETURN_SIZE_Too_small);\
    struct return_type_erasure result;                                         \
    memcpy(&result, &thing, sizeof(thing));                                    \
    return result;                                                             \
}

#define TASK_WRAPPER_WITH_ARG(function, type, arg_type)                        \
const int function ## _return_id = __COUNTER__;                                \
struct return_type_erasure function ## _task_wrapper(void* param)              \
{                                                                              \
    arg_type p;                                                                \
    memcpy(&p, &param, sizeof(p));                                             \
    type thing = function(p);                                                  \
    STATIC_ASSERT(sizeof(thing) <= MAX_RETURN_SIZE, MAX_RETURN_SIZE_Too_small);\
    STATIC_ASSERT(sizeof(arg_type) <= sizeof(param),   Param_too_big);         \
    struct return_type_erasure result;                                         \
    memcpy(&result, &thing, sizeof(thing));                                    \
    return result;                                                             \
}




// -------- IMPLEMENTATION --------
#ifdef TASK_EXECUTOR_IMPLEMENTATION
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <pthread.h>

#ifndef __cplusplus
#include <stdatomic.h>
#else
#include <atomic>
using std::atomic_int;
#endif

#define pthread_assert(code) do { int pthread_result = code; if (pthread_result) { fprintf(stderr, "pthread failed with code %d\n", pthread_result); exit(pthread_result); } }  while (0)

#define POWER_OF_TWO(x) (((x) & ((x) - 1)) == 0)


// ---- CIRCULAR TASK BUFFER ----
typedef struct 
{
    TaskFunction function;
    void* arg;
    int   id;
    int   return_id;
    int   return_size;
} Task;

STATIC_ASSERT(POWER_OF_TWO(MAX_PENDING_TASKS_COUNT), CIRCULAR_TASK_BUFFER_COUNT_Not_a_power_of_2);
/// A queue for the main thread to push Tasks on, and for the worker threads to
/// pop tasks from.
typedef struct
{
    Task array[MAX_PENDING_TASKS_COUNT];
    int  back;
    int  front;
} CircularTaskBuffer;
void  circular_task_buffer_init(CircularTaskBuffer* circular_task_buffer);
int   circular_task_buffer_is_empty(CircularTaskBuffer circular_task_buffer);
int   circular_task_buffer_push(CircularTaskBuffer* circular_task_buffer, Task value);
Task  circular_task_buffer_pop(CircularTaskBuffer* circular_task_buffer);


// ---- CIRCULAR RESPONSE BUFFER ----
STATIC_ASSERT(POWER_OF_TWO(MAX_PENDING_RESPONSES_COUNT), CIRCULAR_RESPONSE_BUFFER_COUNT_Not_a_power_of_2);
/// A queue for the worker threads to push Responses on, and for the main thread
/// to pop responses from. A response is just an index to the result buffer.
typedef struct 
{
    int array[MAX_PENDING_RESPONSES_COUNT];
    int back;
    int front;
} CircularResponseBuffer;
void circular_response_buffer_init(CircularResponseBuffer* circular_response_buffer);
int  circular_response_buffer_is_empty(CircularResponseBuffer circular_response_buffer);
int  circular_response_buffer_push(CircularResponseBuffer* circular_response_buffer, int id);
int  circular_response_buffer_pop(CircularResponseBuffer* circular_response_buffer);


// ---- LINKED BLOCK LIST ----
typedef struct 
{
    char   raw[MAX_RETURN_SIZE];
    size_t return_id;
    size_t size;
} Block;
STATIC_ASSERT(sizeof(Block) % 8 == 0, Block_Not_multiple_of_size_8);
STATIC_ASSERT(sizeof(size_t) == 8, size_t_of_unexpected_size);
void   block_set_data(Block* block, char* data, size_t size, int return_id);
size_t block_data_size(const Block* block);

union Node
{
    Block data;

    // This data is basically free as `Block` is big.
    struct
    {
        union Node* next;
        int has_been_freed;
    } info;
};

STATIC_ASSERT(POWER_OF_TWO(MAX_IDLE_RESULT_COUNT), LINKED_BLOCK_LIST_COUNT_Not_a_power_of_2);
/// The result buffer. The main thread first allocates space and then sends the
/// task for the workers to put the result into. The main thread will never
/// write to it and only one worker will ever write to it.
typedef struct 
{
    union Node  data[MAX_IDLE_RESULT_COUNT];
    union Node* free;
} LinkedBlockList;

void   linked_block_list_init(LinkedBlockList* list);
int    linked_block_list_add(LinkedBlockList* list);
Block* linked_block_list_get(LinkedBlockList* list, int index);
void   linked_block_list_remove(LinkedBlockList* list, int index);


// ---- CIRCULAR TASK BUFFER ----
void circular_task_buffer_init(CircularTaskBuffer* circular_task_buffer)
{
    circular_task_buffer->front = 0;
    circular_task_buffer->back  = 0;
}

int circular_task_buffer_is_empty(CircularTaskBuffer circular_task_buffer)
{
    return circular_task_buffer.back == circular_task_buffer.front;
}

int circular_task_buffer_push(CircularTaskBuffer* circular_task_buffer, Task value)
{
    int index = circular_task_buffer->front;
    int next  = (circular_task_buffer->front + 1) & (MAX_PENDING_RESPONSES_COUNT - 1);

    if (circular_task_buffer->back == next)
    {
        return 0;
    }

    circular_task_buffer->array[index] = value;
    circular_task_buffer->front = next;

    assert(0 <= circular_task_buffer->front && circular_task_buffer->front < MAX_PENDING_TASKS_COUNT && "Invalid implementation.");
    return 1;
}

Task circular_task_buffer_pop(CircularTaskBuffer* circular_task_buffer)
{
    assert(circular_task_buffer->front != circular_task_buffer->back && "Task buffer underflowed.");

    Task result = circular_task_buffer->array[circular_task_buffer->back];
    circular_task_buffer->back = (circular_task_buffer->back + 1) & (MAX_PENDING_TASKS_COUNT - 1);

    assert(0 <= circular_task_buffer->back && circular_task_buffer->back < MAX_PENDING_TASKS_COUNT && "Invalid implementation.");
    return result;
}


// ---- CIRCULAR RESPONSE BUFFER ----
void circular_response_buffer_init(CircularResponseBuffer* circular_response_buffer)
{
    circular_response_buffer->front = 0;
    circular_response_buffer->back  = 0;
}

int circular_response_buffer_is_empty(CircularResponseBuffer circular_response_buffer)
{
    return circular_response_buffer.back == circular_response_buffer.front;
}

int circular_response_buffer_push(CircularResponseBuffer* circular_response_buffer, int id)
{
    int index = circular_response_buffer->front;
    int next  = (circular_response_buffer->front + 1) & (MAX_PENDING_RESPONSES_COUNT - 1);

    if (circular_response_buffer->back == next)
    {
        return 0;
    }

    circular_response_buffer->array[index] = id;
    circular_response_buffer->front = next;

    assert(0 <= circular_response_buffer->front && circular_response_buffer->front < MAX_PENDING_RESPONSES_COUNT && "Invalid implementation");
    return 1;
}

int circular_response_buffer_pop(CircularResponseBuffer* circular_response_buffer)
{
    assert(circular_response_buffer->front != circular_response_buffer->back && "Response buffer underflowed");

    int result = circular_response_buffer->array[circular_response_buffer->back];
    circular_response_buffer->back = (circular_response_buffer->back + 1) & (MAX_PENDING_RESPONSES_COUNT - 1);

    assert(0 <= circular_response_buffer->back && circular_response_buffer->back < MAX_PENDING_RESPONSES_COUNT && "Invalid implementation");
    return result;
}


// ---- LINKED BLOCK LIST ----
void block_set_data(Block* block, char* data, size_t size, int return_id)
{
    assert(size <= MAX_RETURN_SIZE && "Too large size");
    assert(size != 0 && "Size cannot be 0");
    memcpy(block->raw, data, size);

    // NOTE(ted): Must be set in this order as we use `block->size` to check
    //  whether the block is done writing.
    block->return_id = return_id;
    block->size      = size;
}

size_t block_data_size(const Block* block)
{
    return block->size;
}

size_t block_return_id(const Block* block)
{
    return block->return_id;
}

void linked_block_list_init(LinkedBlockList* list)
{
    for (int i = 0; i < MAX_IDLE_RESULT_COUNT - 1; ++i)
    {
        list->data[i].info.next = &list->data[i+1];
        list->data[i].info.has_been_freed = 1;
    }
    list->data[MAX_IDLE_RESULT_COUNT - 1].info.has_been_freed = 1;
    list->data[MAX_IDLE_RESULT_COUNT - 1].info.next = 0;
    list->free = &list->data[0];
}

int linked_block_list_add(LinkedBlockList* list)
{
    if (!list->free)
    {
        return -1;
    }

    assert(list->free->info.has_been_freed);
    int index = (int) (list->free - list->data);
    list->free->info.has_been_freed = 0;
    list->free = list->free->info.next;

    assert(list->free == 0 || list->data <= list->free && list->free < list->data + MAX_IDLE_RESULT_COUNT && "Invalid implementation");

    return index;
}

Block* linked_block_list_get(LinkedBlockList* list, int index)
{
    assert(0 <= index && index < MAX_IDLE_RESULT_COUNT && "Wrong id");
    return &list->data[index].data;
}

void linked_block_list_remove(LinkedBlockList* list, int index)
{
    assert(0 <= index && index < MAX_IDLE_RESULT_COUNT && "Wrong id");
    union Node* node = &list->data[index];
    node->info.has_been_freed = 1;
    node->info.next = list->free;
    list->free = node;
    assert(list->data <= list->free && list->free < list->data + MAX_IDLE_RESULT_COUNT && "Invalid implementation");
}


// -------- TASK EXECUTOR --------
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_request_is_available_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_result_is_available_cond  = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_task_executor_terminated  = PTHREAD_COND_INITIALIZER;

static int g_task_executor_has_terminated     = 0;
static int g_task_executor_wants_to_terminate = 0;

static atomic_int g_tasks_left    = ATOMIC_VAR_INIT(0);
static atomic_int g_thread_count  = ATOMIC_VAR_INIT(0);
static atomic_int g_dropped_tasks = ATOMIC_VAR_INIT(0);

static CircularTaskBuffer     g_pending_requests  = {};
static CircularResponseBuffer g_pending_responses = {};
static LinkedBlockList        g_results           = {};


#define NO_TASK_FOUND_ID   -1
#define EXHAUSTED_TASK_ID  -2


int task_executor_create_task_impl(TaskFunction function, int return_id, int return_size, void* arg)
{
    assert(g_thread_count > 0 && "No threads have been created!");

    int id;
    {
        // NOTE(ted): Doesn't need synchronization as there's never more than
        //  one thread that'll read/write to the same data at the same time.
        //  The operations on the exact data are separated in time.
        //  1. The main thread allocates space and gets the id.
        //  2. Then the worker thread copies data into the space at the id.
        //  3. After that, the main thread copies out of the space and
        //     deallocates it.
        id = linked_block_list_add(&g_results);
        if (id >= 0)
        {
            Task task = { function, arg, id, return_id, return_size };

            // NOTE(ted): I don't think this needs to be synchronized as
            //  it's only the main thread that will call this. Any data race
            //  will not result in bugs.
            if (!circular_task_buffer_push(&g_pending_requests, task))
            {
                #if TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW == TASK_EXECUTOR_PANIC
                    PANIC("Task buffer overflowed.\n");
                #elif TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW == TASK_EXECUTOR_DROP
                    TASK_EXECUTOR_LOGGER("Can't allocate space in result buffer. Task not started...\n");
                    atomic_fetch_add(&g_dropped_tasks, 1);
                #else
                    #error "Invalid option for TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW"
                #endif
            }
            else
            {
                atomic_fetch_add(&g_tasks_left, 1);
            }
        }
        else
        {
            #if TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW == TASK_EXECUTOR_PANIC
                PANIC("Result buffer overflowed.\n");
            #elif TASK_EXECUTOR_ON_RESULT_BUFFER_OVERFLOW == TASK_EXECUTOR_DROP
                TASK_EXECUTOR_LOGGER("Can't allocate space in result buffer. Task not started...\n");
                atomic_fetch_add(&g_dropped_tasks, 1);
            #else
                #error "Invalid option for TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW"
            #endif
        }
    }
    pthread_assert(pthread_cond_signal(&g_request_is_available_cond));

    return id;
}


void task_executor_get_task_result_impl(int task_id, void* result, size_t return_size)
{
    assert(task_id >= 0 && "Invalid task!");

    // NOTE(ted): No need to lock for the check as once the block has been
    //  set it'll never be modified by another thread or change memory address.
    //  Only the main thread will declare it as free.
    Block* block = linked_block_list_get(&g_results, task_id);
    assert(block_data_size(block) > 0);
    memcpy(result, block, return_size);

    // NOTE(ted): Doesn't need synchronization as there's never more than
    //  one thread that'll read/write to the same data at the same time.
    //  The operations on the exact data are separated in time.
    //  1. The main thread allocates space and gets the id.
    //  2. Then the worker thread copies data into the space at the id.
    //  3. After that, the main thread copies out of the space and
    //     deallocates it.
    linked_block_list_remove(&g_results, task_id);
    atomic_fetch_add(&g_tasks_left, -1);
}


int task_executor_try_get_task_result_impl(int task_id, void* result, size_t return_size)
{
    assert(task_id >= 0 && "Invalid task!");

    // NOTE(ted): No need to lock for the check as once the block has been
    //  set it'll never be modified by another thread or change memory address.
    //  Only the main thread will declare it as free.
    Block* block = linked_block_list_get(&g_results, task_id);
    if (block_data_size(block) > 0)
    {
        memcpy(result, block, return_size);

        // NOTE(ted): Doesn't need synchronization as there's never more than
        //  one thread that'll read/write to the same data at the same time.
        //  The operations on the exact data are separated in time.
        //  1. The main thread allocates space and gets the id.
        //  2. Then the worker thread copies data into the space at the id.
        //  3. After that, the main thread copies out of the space and
        //     deallocates it.
        linked_block_list_remove(&g_results, task_id);
        atomic_fetch_add(&g_tasks_left, -1);
        return 1;
    }
    return 0;
}



void task_executor_wait_for_task_result_impl(int task_id, void* result, size_t return_size)
{
    // NOTE(ted): No need to lock for the check as once the block has been
    //  set it'll never be modified by another thread or change memory address.
    //  Only the main thread will declare it as free.
    Block* block = linked_block_list_get(&g_results, task_id);

    // NOTE(ted): Need to lock here as the main thread might enter the while
    //  loop, and before attaining the lock another thread might write and
    //  signal that a result is available, effectively making the main thread
    //  miss the signal and wait. This results in a deadlock if no more
    //  results are signaled to wake up the main thread.
    pthread_assert(pthread_mutex_lock(&g_lock));
    while (block_data_size(block) > 0)
    {
        TASK_EXECUTOR_LOGGER("[Main]: No results... Waiting.\n");
        pthread_assert(pthread_cond_wait(&g_result_is_available_cond, &g_lock));
        pthread_assert(pthread_mutex_unlock(&g_lock));
        block = linked_block_list_get(&g_results, task_id);
    }

    memcpy(result, block, return_size);

    // NOTE(ted): Doesn't need synchronization as there's never more than
    //  one thread that'll read/write to the same data at the same time.
    //  The operations on the exact data are separated in time.
    //  1. The main thread allocates space and gets the id.
    //  2. Then the worker thread copies data into the space at the id.
    //  3. After that, the main thread copies out of the space and
    //     deallocates it.
    linked_block_list_remove(&g_results, task_id);
    atomic_fetch_add(&g_tasks_left, -1);
}


void* task_executor_worker_loop(void* param)
{
    size_t id = (size_t) param;
    struct return_type_erasure value = {};
    Task task;

    while (1)
    {
        if (g_task_executor_wants_to_terminate)
            break;

        {
            // NOTE(ted): Need to lock here as the worker thread might enter the
            //  while loop, and before attaining the lock the main thread might
            //  write and signal that a result is available, effectively making
            //  the worker thread miss the signal and wait. This results in a
            //  deadlock if no more requests are signaled to wake up the worker
            //  thread.
            pthread_assert(pthread_mutex_lock(&g_lock));
            while (!g_task_executor_wants_to_terminate && circular_task_buffer_is_empty(g_pending_requests))
            {
                TASK_EXECUTOR_LOGGER("[Thread %zu]: Waiting for requests...\n", id);
                pthread_assert(pthread_cond_wait(&g_request_is_available_cond, &g_lock));
            }

            if (g_task_executor_wants_to_terminate)
            {
                pthread_assert(pthread_mutex_unlock(&g_lock));
                break;
            }

            // NOTE(ted): Need to lock as otherwise it would be read and written
            //  to by multiple threads.
            task = circular_task_buffer_pop(&g_pending_requests);
            pthread_assert(pthread_mutex_unlock(&g_lock));
            TASK_EXECUTOR_LOGGER("[Thread %zu]: Got request %d\n", id, task.id);
        }


        assert(task.id >= 0 && task.function != 0 && "Invalid task!");
        value = task.function(task.arg);

        // NOTE(ted): No need to block as the memory is exclusive to the
        //  `id` and will not change or be modified until it's set.
        Block* result = linked_block_list_get(&g_results, task.id);
        block_set_data(result, value.data, task.return_size, task.return_id);

        TASK_EXECUTOR_LOGGER("[Thread %zu]: Done with %d\n", id, task.id);
        {
            // NOTE(ted): Need to lock as otherwise it would be read and written
            //  to by multiple threads.
            pthread_assert(pthread_mutex_lock(&g_lock));
            if (!circular_response_buffer_push(&g_pending_responses, task.id))
            {
                pthread_assert(pthread_mutex_unlock(&g_lock));
                #if TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW == TASK_EXECUTOR_PANIC
                    PANIC("Response buffer overflowed.\n");
                #elif TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW == TASK_EXECUTOR_DROP
                    TASK_EXECUTOR_LOGGER("Can't allocate space in response buffer. Task %i dropped..\n", task.id);
                    atomic_fetch_add(&g_dropped_tasks, 1);
                    atomic_fetch_add(&g_tasks_left, -1);
                #else
                    #error "Invalid option for TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW"
                #endif
            }
            else
            {
                pthread_assert(pthread_mutex_unlock(&g_lock));
                // NOTE(ted): Should only exist one thread that receive results.
                pthread_assert(pthread_cond_signal(&g_result_is_available_cond));
            }
        }
    }

    int previous_thread_count = atomic_fetch_add(&g_thread_count, -1);
    if (previous_thread_count == 1)
    {
        // Do clean-up.
        TASK_EXECUTOR_LOGGER("[Thread %zu]: Last worker exiting\n", id);

        // NOTE(ted): This might be unnecessary as you might want to fetch
        //  results after you've terminated all threads. This is only useful
        //  if you want to terminate and then initialize the module again later,
        //  but maybe it's better to just have a specialized function for that
        //  case.
        // linked_block_list_init(&results);
        // circular_task_buffer_init(&pending_requests);
        // circular_response_buffer_init(&pending_results);

        // NOTE(ted): No need to synchronize as a data race doesn't really
        //  matter.
        g_task_executor_has_terminated = 1;
        pthread_assert(pthread_cond_signal(&g_task_executor_terminated));
    }
    else
    {
        TASK_EXECUTOR_LOGGER("[Thread %zu]: Exiting.\n", id);
    }

    return 0;
}

int wait_for_one()
{
    // NOTE(ted): Need to lock here as the main thread might enter the while
    //  loop, and before attaining the lock another thread might write and
    //  signal that a response is available, effectively making the main thread
    //  miss the signal and wait. This results in a deadlock if no more
    //  responses are signaled to wake up the main thread.
    pthread_assert(pthread_mutex_lock(&g_lock));
    while (circular_response_buffer_is_empty(g_pending_responses))
    {
        TASK_EXECUTOR_LOGGER("[Main]: Waiting for result...\n");
        pthread_assert(pthread_cond_wait(&g_result_is_available_cond, &g_lock));
    }
    pthread_assert(pthread_mutex_unlock(&g_lock));

    // NOTE(ted): Should be thread-safe as only main will
    //  call pop which only modifies `head`. As only one
    //  thread will modify and read from it, it's safe.
    int id = circular_response_buffer_pop(&g_pending_responses);
    TASK_EXECUTOR_LOGGER("[Main]: Got result %d\n", id);
    return id;
}

void wait_for_all(const int* task_id_array, int count)
{
    int fetched = 0;
    do {
        int id = wait_for_one();

        for (int i = 0; i < count; ++i)
        {
            if (task_id_array[i] == id)
            {
                fetched += 1;
                break;
            }
        }
    } while (fetched < count);
}


int poll_result()
{
    // NOTE(ted): Should be thread-safe as the only time the buffer
    //  can be empty is after a pop, which only the main thread will
    //  perform, or before any push has been done.
    if (!circular_response_buffer_is_empty(g_pending_responses))
        // NOTE(ted): Should be thread-safe as only main will
        //  call pop which only modifies `head`. As only one
        //  thread will modify and read from it, it's safe.
        return circular_response_buffer_pop(&g_pending_responses);
    else
        return NO_TASK_FOUND_ID;
}


int get_return_type(int task_id)
{
    if (task_id >= 0)
    {
        Block* block = linked_block_list_get(&g_results, task_id);
        if (block_data_size(block) > 0)
        {
            return block_return_id(block);
        }
    }
    return -1;
}


int tasks_in_progress()
{
    return g_tasks_left > 0;
}


void task_executor_initialize_with_thread_count(int thread_count)
{
    assert(thread_count > 0 && "Can't create a task executor with less than 1 thread.");
    assert(g_thread_count == 0 && "Seems like there already are threads created.");

    // NOTE(ted): Should be called from a single thread, so no atomic operations
    //  needed here.
    g_task_executor_wants_to_terminate = 0;
    g_thread_count = thread_count;
    g_task_executor_has_terminated = 0;
    g_tasks_left = 0;

    linked_block_list_init(&g_results);
    circular_task_buffer_init(&g_pending_requests);
    circular_response_buffer_init(&g_pending_responses);

    TASK_EXECUTOR_LOGGER("[Main]: Initializing task executor with %d threads\n", thread_count);
    for (int i = 0; i < thread_count; ++i)
    {
        pthread_t thread_id = { 0 };
        pthread_assert(pthread_create(&thread_id, 0, task_executor_worker_loop, (void*) (size_t) i));
        pthread_assert(pthread_detach(thread_id));
    }
}


void task_executor_initialize()
{
    task_executor_initialize_with_thread_count(DEFAULT_THREAD_COUNT);
}


void task_executor_terminate()
{
    assert(g_tasks_left == 0 && "Should not terminate with tasks left!\n");
    TASK_EXECUTOR_LOGGER("[Main]: Sending termination request\n");

    g_task_executor_wants_to_terminate = 1;

    // NOTE(ted): Locking here just to make sure that the workers
    //  were they checked the old value of `g_task_executor_wants_to_terminate`
    //  but still haven't gotten to wait on `g_request_is_available_cond`.
    //  Without the lock, they might miss the signal.
    pthread_assert(pthread_mutex_lock(&g_lock));
    pthread_assert(pthread_mutex_unlock(&g_lock));
    pthread_assert(pthread_cond_broadcast(&g_request_is_available_cond));
}


void task_executor_terminate_and_wait()
{
    TASK_EXECUTOR_LOGGER("[Main]: Sending termination request and waiting on termination signal\n");

    g_task_executor_wants_to_terminate = 1;

    // NOTE(ted): Locking here just to make sure that the workers
    //  were they checked the old value of `g_task_executor_wants_to_terminate`
    //  but still haven't gotten to wait on `g_request_is_available_cond`.
    //  Without the lock, they might miss the signal.
    pthread_assert(pthread_mutex_lock(&g_lock));
    pthread_assert(pthread_mutex_unlock(&g_lock));

    pthread_assert(pthread_cond_broadcast(&g_request_is_available_cond));
    pthread_assert(pthread_mutex_lock(&g_lock));
    while (!g_task_executor_has_terminated)
    {
        pthread_assert(pthread_cond_wait(&g_task_executor_terminated, &g_lock));
    }
    pthread_assert(pthread_mutex_unlock(&g_lock));

    TASK_EXECUTOR_LOGGER("[Main]: Task executor terminated\n");
}


int get_thread_count()
{
    return atomic_load(&g_thread_count);
}

#endif  // TASK_EXECUTOR_IMPLEMENTATION



#ifdef __cplusplus
#include <tuple>
namespace task_executor {

// Clean up unsafe macros and provide safer alternatives.
#undef create_task
#undef create_task_with_arg
#undef try_get_task_result
#undef wait_for_task_result
#undef STATIC_ASSERT
#undef TASK_WRAPPER
#undef TASK_WRAPPER_WITH_ARG



// Since we're in a namespace, it's nicer to have shorter names and overloading.
void initialize(int thread_count_);
void initialize();
void terminate();
void terminate_wait();


#ifdef TASK_EXECUTOR_IMPLEMENTATION
void initialize(int thread_count_) { task_executor_initialize_with_thread_count(thread_count_); }
void initialize()                  { task_executor_initialize();                                }
void terminate()                   { task_executor_terminate();                                 }
void terminate_and_wait()          { task_executor_terminate_and_wait();                            }
#endif


template <class Iterable>
void wait_for_all(const Iterable& iterable)
{
    int fetched = 0;
    do {
        int id = wait_for_one();

        for (const auto& task_id : iterable)
        {
            if (task_id == id)
            {
                fetched += 1;
                break;
            }
        }
    } while (fetched < iterable.size());
}


// Should maybe swap for std::optional #include <optional>
template <class T>
class option
{
public:
    option(T value, bool is_valid) : data(value), is_valid(is_valid) {}

    [[nodiscard]] bool has_value() const noexcept { return is_valid; }
    T value() const noexcept { assert(is_valid); return data; }
private:
    T    data;
    bool is_valid;
};


// Type-safe interface.
template <class T>
class task_result
{
public:
    explicit task_result() : id(EXHAUSTED_TASK_ID) {  }
    explicit task_result(int id) : id(id) {  }

    option<T> try_get()
    {
        T result { };
        bool valid = task_executor_try_get_task_result_impl(id, &result, sizeof(T));
        if (valid) id = EXHAUSTED_TASK_ID;
        return { result, valid };
    }

    T get()
    {
        T result { };
        assert(task_executor_try_get_task_result_impl(id, &result, sizeof(T)));
        id = EXHAUSTED_TASK_ID;
        return result;
    }

    int get_id() const noexcept { return id; }

    [[nodiscard]] bool is_exhausted() const noexcept { return id == EXHAUSTED_TASK_ID; }

    bool operator== (int i) const noexcept { return id == i; }

private:
    int id;
};



template <class U, auto f(U)>
auto create_task(U arg) -> task_result<decltype(f(arg))>
{
    using ReturnType = decltype(f(arg));

    auto function = [](void* arg) -> return_type_erasure {
        auto thing = f((U)(size_t)(arg));
        return_type_erasure result { };
        memcpy(&result, &thing, sizeof(thing));

        // Prevent any destructor from running.
        char buffer[sizeof(ReturnType) + alignof(ReturnType)];
        char* aligned_buffer = buffer + alignof(ReturnType) - reinterpret_cast<intptr_t>(buffer) % alignof(ReturnType);
        ReturnType* object = new (aligned_buffer) ReturnType;

        return result;
    };

    int id = task_executor_create_task_impl(function, sizeof(ReturnType), (void*) arg);
    return task_result<ReturnType> { id };
}

template <auto f()>
auto create_task() -> task_result<decltype(f())>
{
    using T = decltype(f());

    auto function = [](void* arg) -> return_type_erasure {
        return_type_erasure result {};

        {
            auto thing = f();
            auto* ptr = new((unsigned char*) &result) T;
            *ptr = std::move(thing);

            thing.~T();
        }

        return result;
    };

    int id = task_executor_create_task_impl(function, sizeof(T), 0);
    return task_result<T> { id };
}

}  // namespace task_executor
#endif  // __cplusplus


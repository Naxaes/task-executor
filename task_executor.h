#ifdef __cplusplus
#include <cstddef>  // size_t
namespace task_executor::details {
extern "C" {
#else
#include <stddef.h>  // size_t
#endif

/// Creates a task and puts it in the task buffer. The `arg` **must** be valid
/// until the `TaskResult` is available.
#define create_task_with_arg(function, return_type, arg)  do { void* x; memcpy(&x, &arg, sizeof(x)); task_executor_create_task_impl(function ## _task_wrapper, function ## _return_id, sizeof(return_type), x); } while (0)
#define create_task(function, return_type)                task_executor_create_task_impl(function ## _task_wrapper, function ## _return_id, sizeof(return_type), 0)

#define create_task_with_arg_no_return(function, arg)  do { void* x; memcpy(&x, &arg, sizeof(x)); task_executor_create_task_without_return_impl(function ## _task_wrapper, x); } while (0)
#define create_task_no_return(function)                task_executor_create_task_without_return_impl(function ## _task_wrapper, 0)

/// Sets the `result` if the task is done or panics. The `task_id` **must not**
/// be used afterwards.
#define get_task_result(response, result)   task_executor_get_task_result_impl(response, result, sizeof(*result))

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

#ifndef TASK_EXECUTOR_CACHE_LINE_SIZE
#define TASK_EXECUTOR_CACHE_LINE_SIZE 64
#endif

/// Assert if the implementation is correct.
#ifndef TASK_EXECUTOR_IMPLEMENTATION_ASSERTIONS
#define TASK_EXECUTOR_IMPLEMENTATION_ASSERTIONS 1
#endif

/// Assert if the user is using the library correctly.
#ifndef TASK_EXECUTOR_DEBUG_USER_CONTRACT
#define TASK_EXECUTOR_DEBUG_USER_CONTRACT 1
#endif

#if TASK_EXECUTOR_IMPLEMENTATION_ASSERTIONS
#define TASK_EXECUTOR_IMPLEMENTATION_ASSERT(condition) assert((condition) && "Invalid implementation.")
#else
#define TASK_EXECUTOR_IMPLEMENTATION_ASSERT(condition) 
#endif

#if TASK_EXECUTOR_DEBUG_USER_CONTRACT
#define TASK_EXECUTOR_USER_CONTRACT_ASSERT(condition) assert(condition && "User contract broken.")
#else
#define TASK_EXECUTOR_USER_CONTRACT_ASSERT(condition)
#endif

#define STATIC_ASSERT(condition, message) typedef char static_assertion_##message[(condition) ? 1: -1]
#define PANIC(message) do { fprintf(stderr, message); exit(-1); } while (0)


/// Generic holder of data.
struct return_type_erasure
{
    char data[MAX_RETURN_SIZE];
};
typedef struct return_type_erasure (*TaskFunction)(void*);

#define TASK_WRAPPER(function, type)                                           \
const int function ## _return_id = UNIQUE_CONSTANT + 1;                        \
struct return_type_erasure function ## _task_wrapper(void* param)              \
{                                                                              \
    (void*)param;                                                              \
    type thing = function();                                                   \
    STATIC_ASSERT(sizeof(thing) <= MAX_RETURN_SIZE, MAX_RETURN_SIZE_Too_small);\
    struct return_type_erasure result = { 0 };                                 \
    memcpy(&result, &thing, sizeof(thing));                                    \
    return result;                                                             \
}

#define TASK_WRAPPER_WITH_ARG(function, type, arg_type)                        \
const int function ## _return_id = UNIQUE_CONSTANT + 1;                        \
struct return_type_erasure function ## _task_wrapper(void* param)              \
{                                                                              \
    arg_type p;                                                                \
    memcpy(&p, &param, sizeof(p));                                             \
    type thing = function(p);                                                  \
    STATIC_ASSERT(sizeof(thing) <= MAX_RETURN_SIZE, MAX_RETURN_SIZE_Too_small);\
    STATIC_ASSERT(sizeof(arg_type) <= sizeof(param),   Param_too_big);         \
    struct return_type_erasure result = { 0 };                                 \
    memcpy(&result, &thing, sizeof(thing));                                    \
    return result;                                                             \
}

#define TASK_WRAPPER_NO_RETURN(function)                                       \
struct return_type_erasure function ## _task_wrapper(void* param)              \
{                                                                              \
    (void*)param;                                                              \
    function();                                                                \
    struct return_type_erasure result = { 0 };                                 \
    return result;                                                             \
}

#define TASK_WRAPPER_WITH_ARG_NO_RETURN(function, arg_type)                    \
struct return_type_erasure function ## _task_wrapper(void* param)              \
{                                                                              \
    arg_type p;                                                                \
    memcpy(&p, &param, sizeof(p));                                             \
    function(p);                                                               \
    struct return_type_erasure result = { 0 };                                 \
    return result;                                                             \
}

typedef struct
{
    struct return_type_erasure result;
    size_t task_id;
    int    return_id;
    int    return_size;
} __attribute__ ((aligned(TASK_EXECUTOR_CACHE_LINE_SIZE))) Response;

size_t task_executor_create_task_without_return_impl(TaskFunction function, void* arg);
size_t task_executor_create_task_impl(TaskFunction function, int return_id, int return_size, void* arg);
void task_executor_get_task_result_impl(const Response** x, void* result, int return_size);
void consume_task_result(const Response** x);
const Response* wait_for_next_task();
const Response* poll_for_next_result();
int tasks_are_remaining();
void task_executor_initialize_with_thread_count(int thread_count);
void task_executor_initialize();
void task_executor_terminate();
void task_executor_terminate_and_wait();

#ifdef __cplusplus
} // extern "C"
} // namespace task_executor
#endif


// -------- IMPLEMENTATION --------
#ifdef TASK_EXECUTOR_IMPLEMENTATION
#ifdef __cplusplus
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <memory.h>
#include <pthread.h>

namespace task_executor::details {
using std::atomic_int;
extern "C" {
#else
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <pthread.h>
#include <stdatomic.h>
#endif

#define PTHREAD_ASSERT(code) do { int pthread_result = code; if (pthread_result) { fprintf(stderr, "pthread failed with code %d\n", pthread_result); exit(pthread_result); } }  while (0)

#define IS_POWER_OF_TWO(x) ((x) != 0 && ((x) & ((x) - 1)) == 0)
#ifndef __COUNTER__
#define UNIQUE_CONSTANT __LINE__
#else
#define UNIQUE_CONSTANT __COUNTER__
#endif



// ---- TASK QUEUE ----
typedef struct 
{
    size_t id;
    TaskFunction function;
    void*  arg;
    int    return_id;
    int    return_size;
} __attribute__ ((aligned(TASK_EXECUTOR_CACHE_LINE_SIZE))) Task;

STATIC_ASSERT(IS_POWER_OF_TWO(MAX_PENDING_TASKS_COUNT), CIRCULAR_TASK_BUFFER_COUNT_Not_a_power_of_2);
/// A queue for the main thread to push Tasks on, and for the worker threads to
/// pop tasks from.
typedef struct
{
    Task array[MAX_PENDING_TASKS_COUNT];
    int  back;
    int  front;
} TaskQueue;

static inline void task_queue_init(TaskQueue* task_queue)
{
    task_queue->front = 0;
    task_queue->back  = 0;
}

static inline int task_queue_is_empty(TaskQueue task_queue)
{
    return task_queue.back == task_queue.front;
}

static inline int task_queue_push(TaskQueue* task_queue, Task task)
{
    int index = task_queue->front;
    int next  = (task_queue->front + 1) & (MAX_PENDING_TASKS_COUNT - 1);

    if (task_queue->back == next)
    {
        return 0;
    }

    task_queue->array[index] = task;
    task_queue->front = next;

    TASK_EXECUTOR_IMPLEMENTATION_ASSERT(0 <= task_queue->front && task_queue->front < MAX_PENDING_TASKS_COUNT);
    return 1;
}

static inline Task task_queue_pop(TaskQueue* task_queue)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(task_queue->front != task_queue->back && "Task queue underflowed.");

    Task result = task_queue->array[task_queue->back];
    task_queue->back = (task_queue->back + 1) & (MAX_PENDING_TASKS_COUNT - 1);

    TASK_EXECUTOR_IMPLEMENTATION_ASSERT(0 <= task_queue->back && task_queue->back < MAX_PENDING_TASKS_COUNT);
    return result;
}


// ---- RESPONSE QUEUE ----
STATIC_ASSERT(IS_POWER_OF_TWO(MAX_PENDING_RESPONSES_COUNT), RESULT_BUFFER_COUNT_Not_a_power_of_2);
STATIC_ASSERT(sizeof(Response) <= TASK_EXECUTOR_CACHE_LINE_SIZE, Response_Too_big);

/// A queue for the main thread to push Tasks on, and for the worker threads to
/// pop tasks from.
typedef struct
{
    Response array[MAX_PENDING_TASKS_COUNT];
    int  back;
    int  front;
} ResponseQueue;

static inline void response_queue_init(ResponseQueue* response_queue)
{
    response_queue->front = 0;
    response_queue->back  = 0;
}

static inline int response_queue_is_empty(ResponseQueue response_queue)
{
    return response_queue.back == response_queue.front;
}

static inline int response_queue_push(ResponseQueue* response_queue, const Response* response)
{
    int index = response_queue->front;
    int next  = (response_queue->front + 1) & (MAX_PENDING_TASKS_COUNT - 1);

    if (response_queue->back == next)
    {
        return 0;
    }

    Response* x = &response_queue->array[index];
    memcpy(&x->result, &response->result, response->return_size);
    x->return_size = response->return_size;
    x->return_id   = response->return_id;
    x->task_id     = response->task_id;

    response_queue->front = next;

    TASK_EXECUTOR_IMPLEMENTATION_ASSERT(0 <= response_queue->front && response_queue->front < MAX_PENDING_TASKS_COUNT);
    return 1;
}

static inline void response_queue_pop(ResponseQueue* response_queue, Response* response)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(response_queue->front != response_queue->back && "Task queue underflowed.");

    Response* x = &response_queue->array[response_queue->back];

    memcpy(&response->result, &x->result, x->return_size);
    response->return_size = x->return_size;
    response->return_id   = x->return_id;
    response->task_id     = x->task_id;

    response_queue->back = (response_queue->back + 1) & (MAX_PENDING_TASKS_COUNT - 1);

    TASK_EXECUTOR_IMPLEMENTATION_ASSERT(0 <= response_queue->back && response_queue->back < MAX_PENDING_TASKS_COUNT);
}

static inline void response_queue_remove(ResponseQueue* response_queue)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(response_queue->front != response_queue->back);
    response_queue->back = (response_queue->back + 1) & (MAX_PENDING_TASKS_COUNT - 1);
    TASK_EXECUTOR_IMPLEMENTATION_ASSERT(0 <= response_queue->back && response_queue->back < MAX_PENDING_TASKS_COUNT);
}

static inline Response* response_queue_get(ResponseQueue* response_queue)
{
    return &response_queue->array[response_queue->back];
}


// -------- TASK EXECUTOR --------
#define NO_RETURN_ID        0
#define TASK_ID_INVALID     0
#define TASK_ID_START_VALUE 1


// NOTE(ted): All these are shared between the threads.
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_task_is_available_cond     = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_response_is_available_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_task_executor_terminated   = PTHREAD_COND_INITIALIZER;

static int g_task_executor_has_terminated     = 0;
static int g_task_executor_wants_to_terminate = 0;

static size_t g_task_id = TASK_ID_START_VALUE;

static atomic_int g_tasks_left    = ATOMIC_VAR_INIT(0);
static atomic_int g_thread_count  = ATOMIC_VAR_INIT(0);
static atomic_int g_dropped_tasks = ATOMIC_VAR_INIT(0);

static TaskQueue     g_task_queue     = { 0 };
static ResponseQueue g_response_queue = { 0 };


static inline size_t task_executor_post_task(Task task)
{
    // NOTE(ted): I don't think this needs to be synchronized as
    //  it's only the main thread that will call this. Any data race
    //  will not result in bugs.
    if (!task_queue_push(&g_task_queue, task))
    {
        #if TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW == TASK_EXECUTOR_PANIC
            PANIC("Task queue overflowed.\n");
        #elif TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW == TASK_EXECUTOR_DROP
            TASK_EXECUTOR_LOGGER("[Main]: Can't allocate space in task buffer. Task not started...\n");
            atomic_fetch_add(&g_dropped_tasks, 1);
        #else
            #error "Invalid option for TASK_EXECUTOR_ON_TASK_BUFFER_OVERFLOW"
        #endif
    }
    else
    {
        PTHREAD_ASSERT(pthread_cond_signal(&g_task_is_available_cond));
        atomic_fetch_add(&g_tasks_left, 1);
    }

    return task.id;
}


size_t task_executor_create_task_without_return_impl(TaskFunction function, void* arg)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(g_thread_count > 0 && "Task executor has not been initialized!");

    size_t id = g_task_id;
    g_task_id += 1;

    Task task = { id, function, arg, NO_RETURN_ID, 0 };

    return task_executor_post_task(task);
}


size_t task_executor_create_task_impl(TaskFunction function, int return_id, int return_size, void* arg)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(g_thread_count > 0 && "Task executor has not been initialized!");
    assert(return_size > 0 && "Return was unexpectedly 0.");
    assert(return_id != NO_RETURN_ID && "Return id is wrong.");

    size_t id = g_task_id;
    g_task_id += 1;

    Task task = { id, function, arg, return_id, return_size };

    return task_executor_post_task(task);
}


void task_executor_get_task_result_impl(const Response** x, void* result, int return_size)
{
    // NOTE(ted): No need to lock for the check as once the block has been
    //  set it'll never be modified by another thread or change memory address.
    //  Only the main thread will declare it as free.
    Response response = { 0 };
    response_queue_pop(&g_response_queue, &response);
    assert(response.return_size == return_size && return_size == (*x)->return_size);
    memcpy(result, &response.result, response.return_size);
    atomic_fetch_add(&g_tasks_left, -1);
    *x = 0;
}


void consume_task_result(const Response** x)
{
    // NOTE(ted): No need to lock for the check as once the block has been
    //  set it'll never be modified by another thread or change memory address.
    //  Only the main thread will declare it as free.
    response_queue_remove(&g_response_queue);
    atomic_fetch_add(&g_tasks_left, -1);
    *x = 0;
}


static inline void* task_executor_worker_loop(void* param)
{
    size_t id = (size_t) param;
    struct return_type_erasure value = { 0 };
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
            PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
            while (!g_task_executor_wants_to_terminate && task_queue_is_empty(g_task_queue))
            {
                TASK_EXECUTOR_LOGGER("[Thread %zu]: Waiting for requests...\n", id);
                PTHREAD_ASSERT(pthread_cond_wait(&g_task_is_available_cond, &g_lock));
            }

            if (g_task_executor_wants_to_terminate)
            {
                PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));
                break;
            }

            // NOTE(ted): Need to lock as otherwise it would be read and written
            //  to by multiple threads.
            task = task_queue_pop(&g_task_queue);
            PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));
            TASK_EXECUTOR_LOGGER("[Thread %zu]: Got request %zu\n", id, task.id);
        }


        assert(task.id > 0 && task.function != 0 && "Invalid task!");
        value = task.function(task.arg);

        Response response;
        response.task_id     = task.id;
        response.return_id   = task.return_id;
        response.return_size = task.return_size;
        memcpy(&response.result, &value, response.return_size);

        TASK_EXECUTOR_LOGGER("[Thread %zu]: Done with %zu\n", id, task.id);
        int result;
        {
            // NOTE(ted): Need to lock as otherwise it would be read and written
            //  to by multiple threads.
            PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
            result = response_queue_push(&g_response_queue, &response);
            PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));
        }
        if (result)
        {
            // NOTE(ted): Should only exist one thread that receive results.
            PTHREAD_ASSERT(pthread_cond_signal(&g_response_is_available_cond));
        }
        else
        {
            #if TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW == TASK_EXECUTOR_PANIC
                PANIC("Task queue overflowed.\n");
            #elif TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW == TASK_EXECUTOR_DROP
                TASK_EXECUTOR_LOGGER("Can't allocate space in task buffer. Task not started...\n");
                atomic_fetch_add(&g_dropped_tasks, 1);
                atomic_fetch_add(&g_tasks_left, -1);
            #else
                #error "Invalid option for TASK_EXECUTOR_ON_RESPONSE_BUFFER_OVERFLOW"
            #endif
        }
    }

    int previous_thread_count = atomic_fetch_add(&g_thread_count, -1);
    if (previous_thread_count == 1)
    {
        // Do clean-up.
        TASK_EXECUTOR_LOGGER("[Thread %zu]: Last worker exiting\n", id);

        // NOTE(ted): No need to synchronize as a data race doesn't really
        //  matter.
        g_task_executor_has_terminated = 1;
        PTHREAD_ASSERT(pthread_cond_signal(&g_task_executor_terminated));
    }
    else
    {
        TASK_EXECUTOR_LOGGER("[Thread %zu]: Exiting.\n", id);
    }

    return 0;
}

const Response* wait_for_next_task()
{
    // NOTE(ted): Need to lock here as the main thread might enter the while
    //  loop, and before attaining the lock another thread might write and
    //  signal that a response is available, effectively making the main thread
    //  miss the signal and wait. This results in a deadlock if no more
    //  responses are signaled to wake up the main thread.
    PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
    while (response_queue_is_empty(g_response_queue))
    {
        TASK_EXECUTOR_LOGGER("[Main]: Waiting for result...\n");
        PTHREAD_ASSERT(pthread_cond_wait(&g_response_is_available_cond, &g_lock));
    }
    PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));

    return response_queue_get(&g_response_queue);
}


const Response* poll_for_next_result()
{
    // NOTE(ted): Should be thread-safe as the only time the buffer
    //  can be empty is after a pop, which only the main thread will
    //  perform, or before any push has been done.
    if (!response_queue_is_empty(g_response_queue))
    {
        return response_queue_get(&g_response_queue);
    }
    else
    {
        return 0;
    }
}


int tasks_are_remaining()
{
    return g_tasks_left > 0;
}


void task_executor_initialize_with_thread_count(int thread_count)
{
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(thread_count > 0 && "Can't create a task executor with less than 1 thread.");
    TASK_EXECUTOR_USER_CONTRACT_ASSERT(g_thread_count == 0 && "Seems like there already are threads created.");

    // NOTE(ted): Should be called from a single thread, so no atomic operations
    //  needed here.
    g_task_executor_has_terminated     = 0;
    g_task_executor_wants_to_terminate = 0;
    g_task_id = TASK_ID_START_VALUE;

    g_tasks_left    = ATOMIC_VAR_INIT(0);
    g_thread_count  = ATOMIC_VAR_INIT(thread_count);
    g_dropped_tasks = ATOMIC_VAR_INIT(0);

    task_queue_init(&g_task_queue);
    response_queue_init(&g_response_queue);

    TASK_EXECUTOR_LOGGER("[Main]: Initializing task executor with %d threads\n", thread_count);
    for (int i = 0; i < thread_count; ++i)
    {
        pthread_t thread_id = { 0 };
        PTHREAD_ASSERT(pthread_create(&thread_id, 0, task_executor_worker_loop, (void*) (size_t) i));
        PTHREAD_ASSERT(pthread_detach(thread_id));
    }
}


void task_executor_initialize()
{
    task_executor_initialize_with_thread_count(DEFAULT_THREAD_COUNT);
}


void task_executor_terminate()
{
    int tasks_left = atomic_load(&g_tasks_left);
    if (tasks_left != 0)
    {
        TASK_EXECUTOR_LOGGER("[Main]: Terminating with %d tasks left\n", tasks_left);
    }
    TASK_EXECUTOR_LOGGER("[Main]: Sending termination request\n");

    g_task_executor_wants_to_terminate = 1;

    // NOTE(ted): Locking here just to make sure that the workers
    //  were they checked the old value of `g_task_executor_wants_to_terminate`
    //  but still haven't gotten to wait on `g_request_is_available_cond`.
    //  Without the lock, they might miss the signal.
    PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
    PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));
    PTHREAD_ASSERT(pthread_cond_broadcast(&g_task_is_available_cond));
}


void task_executor_terminate_and_wait()
{
    TASK_EXECUTOR_LOGGER("[Main]: Sending termination request and waiting on termination signal\n");

    g_task_executor_wants_to_terminate = 1;

    // NOTE(ted): Locking here just to make sure that the workers
    //  were they checked the old value of `g_task_executor_wants_to_terminate`
    //  but still haven't gotten to wait on `g_request_is_available_cond`.
    //  Without the lock, they might miss the signal.
    PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
    PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));

    PTHREAD_ASSERT(pthread_cond_broadcast(&g_task_is_available_cond));
    PTHREAD_ASSERT(pthread_mutex_lock(&g_lock));
    while (!g_task_executor_has_terminated)
    {
        PTHREAD_ASSERT(pthread_cond_wait(&g_task_executor_terminated, &g_lock));
    }
    PTHREAD_ASSERT(pthread_mutex_unlock(&g_lock));

    TASK_EXECUTOR_LOGGER("[Main]: Task executor terminated\n");
}


int get_thread_count()
{
    return atomic_load(&g_thread_count);
}


#ifdef __cplusplus
} // namespace details
} // extern "C"
#endif

#endif  // TASK_EXECUTOR_IMPLEMENTATION





#ifdef __cplusplus
namespace task_executor
{
    #undef create_task_with_arg
    #undef create_task
    #undef create_task_with_arg_no_return
    #undef create_task_no_return
    #undef get_task_result

    #undef TASK_WRAPPER
    #undef TASK_WRAPPER_WITH_ARG
    #undef TASK_WRAPPER_NO_RETURN
    #undef TASK_WRAPPER_WITH_ARG_NO_RETURN


    template <class T>
    struct Task
    {
        const size_t id;
    };


    inline int get_unique_return_type_id()
    {
        static int return_type_id_counter = 0;
        return_type_id_counter += 1;
        return return_type_id_counter;
    }

    template <class T>
    inline int return_type_id()
    {
        static int id = get_unique_return_type_id();
        return id;
    }

    class Response
    {
    public:
        explicit Response(const details::Response* response) : inner(response) {}
        ~Response()
        {
            TASK_EXECUTOR_USER_CONTRACT_ASSERT(this->inner == nullptr);
        }

        Response(const Response& other) = delete;
        Response& operator=(const Response& other) = delete;

        Response& operator=(Response&& other) noexcept
        {
            if (this != &other)
            {
                this->inner = other.inner;
                other.inner = nullptr;
            }
            return *this;
        }

        template <class T>
        [[nodiscard]] bool is_return_type() const noexcept
        {
            return inner->return_id == return_type_id<T>();
        }

        template <class T>
        inline T get_result()
        {
            TASK_EXECUTOR_USER_CONTRACT_ASSERT(is_return_type<T>());

            T result;
            details::task_executor_get_task_result_impl(&inner, &result, sizeof(result));
            return result;
        }

        inline bool has_result()
        {
            return inner != nullptr;
        }

        inline void consume_task_result()
        {
            details::consume_task_result(&inner);
        }

    private:
        const details::Response* inner;
    };

    template <class U, auto f(U)>
    inline auto create_task(U arg) -> Task<decltype(f(arg))>
    {
        using T = decltype(f(arg));

        auto function = [](void* a) -> details::return_type_erasure
        {
            U param;
            memcpy(&param, &a, sizeof(param));

            details::return_type_erasure return_buffer = { 0 };
            T result = f(param);
            memcpy(&return_buffer, &result, sizeof(result));

            return return_buffer;
        };

        static_assert(sizeof(arg) <= sizeof(void*));
        void* a;
        memcpy(&a, &arg, sizeof(arg));

        size_t id = details::task_executor_create_task_impl(
            function, return_type_id<T>(), sizeof(T), a
        );

        return { id };
    }

    template <class U, void f(U)>
    inline Task<void> create_task(U arg)
    {
        auto function = [](void* a) -> details::return_type_erasure
        {
            U param;
            memcpy(&param, &a, sizeof(param));

            details::return_type_erasure return_buffer = { 0 };
            f(param);

            return return_buffer;
        };

        static_assert(sizeof(arg) <= sizeof(void*));
        void* a;
        memcpy(&a, &arg, sizeof(arg));

        size_t id = details::task_executor_create_task_without_return_impl(
            function, a
        );

        return { id };
    }

    template <auto f()>
    inline auto create_task() -> decltype(f())
    {
        using T = decltype(f());

        auto function = [](void* a) -> details::return_type_erasure
        {
            details::return_type_erasure return_buffer = { 0 };
            T result = f();
            memcpy(&return_buffer, &result, sizeof(result));

            return return_buffer;
        };

        size_t id = details::task_executor_create_task_impl(
            function, return_type_id<T>(), sizeof(T), 0
        );

        return { id };
    }

    template <void f()>
    inline Task<void> create_task()
    {
        auto function = [](void* a) -> details::return_type_erasure
        {
            details::return_type_erasure return_buffer = { 0 };
            f();

            return return_buffer;
        };

        size_t id = details::task_executor_create_task_without_return_impl(
            function, 0
        );

        return { id };
    }

    inline int tasks_left()
    {
        return details::g_tasks_left.load();
    }


    Response poll_for_next_result()
    {
        const auto response = details::poll_for_next_result();
        return Response(response);
    }

    Response wait_for_next_task()
    {
        const auto response = details::wait_for_next_task();
        return Response(response);
    }

    inline void initialize(int thread_count)   { details::task_executor_initialize_with_thread_count(thread_count); }
    inline void initialize()                   { details::task_executor_initialize(); }
    inline void terminate()                    { details::task_executor_terminate();  }
    inline void terminate_and_wait()           { details::task_executor_terminate_and_wait(); }
    inline bool tasks_are_remaining()          { return details::tasks_are_remaining(); }
}
#endif
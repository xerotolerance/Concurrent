//
// Created by thecm on 9/1/2020.
//

#ifndef MAILSEARCH_THREADPOOL_H
#define MAILSEARCH_THREADPOOL_H
#define _ITERATOR_DEBUG_LEVEL 0
#include <thread>
#include <iostream>
#include <shared_mutex>
#include <mutex>
#include <utility>
#include <vector>
#include <future>
#include <queue>
#include <functional>

using namespace std;
namespace concurrent{
    using Task = function<void()>;
    static mutex sout_permission = mutex();

    template<typename... Args>
    struct tsout;    // ThreadSafe StreamWriter Functor

    class ThreadPool;
}

template<typename ...CArgs>
struct concurrent::tsout{
    /// ThreadSafe StreamWriter Functor
    ostream* dest = nullptr;
    string sep;

    tsout(ostream& os, string sep, CArgs... args): dest(&os), sep(move(sep)){ operator()(args...); }
    explicit tsout(ostream& os, CArgs... args): tsout {os, " ", args...}{}
    explicit tsout(CArgs... args): tsout{cout, args...}{}


    friend ostream& operator<<(ostream& os, const tsout& self){ return os; }

    template<typename... FArgs>
    ostream& operator()(FArgs&... args){
        unique_lock<mutex> lock(concurrent::sout_permission);
        int watch = 0;
        return ((*dest << args << (++watch < sizeof...(args) ? sep : "")),...);
    }
};

class concurrent::ThreadPool{
    /// Constructor & Destructor stubs
public:
    explicit ThreadPool(int nthreads = static_cast<int>(thread::hardware_concurrency()), chrono::seconds ttl=30s);
    explicit ThreadPool(string  name, int nthreads = static_cast<int>(thread::hardware_concurrency()), chrono::seconds ttl=30s);

    // No copying
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool& operator=(ThreadPool&) = delete;

    // No moving
    ThreadPool(const ThreadPool&&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(const ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    ~ThreadPool();  // destructor

    /// Private Members
private:
    static int n_pools; // Class Variable
    static shared_mutex npaccess; // Class Variable

    const string name;
    mutable bool running;
    int nthreads, nrunning;
    chrono::seconds ttl;
    queue<Task> jobs;
    thread manager;
    vector<thread> workers;
    shared_mutex jqaccess, nraccess, ntaccess;
    condition_variable_any cv;

    /// Accessor & Mutator stubs
public:
    static int pool_count();
    bool shutdown_requested() const;
    string get_name() const;
    chrono::seconds get_timeout() const;
    void set_timeout(chrono::seconds _ttl=30s);

    // Mutex protected functions
    void set_thread_count(int max_count=static_cast<int>(thread::hardware_concurrency()));
    int get_thread_count();
    int num_active();
    bool is_running();

    /// Internal method stubs
private:
    void poll(chrono::seconds timeout=6s);
    void control_loop();


public:
    ////Exposed API Template methods definitions
    /*  (NOTE:
    //          Keeping Template Func/Obj definitions in the header file is the easiest way to
    //          prevent linking errors. (Hence the <STL> being implemented entirely in header files!)
    //  ) */


    /// VOID Variants
    template<typename Signature, typename... Args>
    future<void> submit(Signature func, Args&... args){
        /// Run a task asynchronously in the ThreadPool

        /// The Async Stuff
        auto promise = make_shared<::promise<void>>();
        // auto* promise = new ::promise<ReturnType>();     // Make sure this promise outlives the scope of this function
        //  so that it can be used by the ThreadPool to set the value
        //  of '.get()' on the future returned by this method.

        auto res = promise->get_future();   // This is the future which this call to '.submit()' will return.
        // It's value will be set whenever the ThreadPool gets around
        // to calling '.set_value()' on it's corresponding promise. (declared above)


        /// Package 'func' into a 'Task' object (aka 'function<void()>') for the ThreadPool to execute asynchronously
        function<void()> fn = bind(func, args...);                     // Hides func's Arguments
        Task task = bind([](const decltype(fn)& f, decltype(promise)& p){               // Hides func's return type

            /// TODO: FIND & FIX THE STACK OVERFLOW CAUSING err 0X800000003 (Encountered a breakpont)

            /// call the function & make signal that its been run.
            ///    (Note: 'Promise.set_value()' sets 'Promise.ready()' to true) (see 'The Async Stuff' above)
            f();
            p->set_value();      // heap-alloc'd promise no longer needed after calling it's set_value() method.
            //delete p;               // delete the promise to free its memory & avoid memory leaks.

        }, fn, promise); //(passes the func & promise into above lambda expression)


        /// Submit the 'task' to the ThreadPool
        {
            // Obtain exclusive access to the Jobs queue
            unique_lock<shared_mutex> lock(jqaccess);
            jobs.emplace(move(task));
            // End-of-scope releases exclusive access
        }
        // Alert next available thread of job waiting
        cv.notify_one();

        /// Return the future that represents func's return value.
        return res;
    }

    template<typename Signature, typename... Args>
    void map(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1) {
        vector<future<void>> chunks;

        auto map_chunk = [](Signature fn, auto begin, auto end) -> void{

            auto run_with_tuple_args = [&](Args... args) {
                return fn(args...);
            };  // using this wrapper lambda for call to submit in order to avoid passing 'fn' as tuple

            for (auto args_iter = begin; args_iter < end; ++args_iter) {
                apply(run_with_tuple_args, *args_iter); // Use apply(fn, tuple<CArgs...>{args...}) to call a fn(args...)
            }
        };

        int count = 0;
        // Submit jobs to the Pool in "chunks" of "chunksize" (or less) tasks.
        //  WITHIN a chunk, jobs are executed SEQUENTIALLY in the same thread,
        //  while EACH CHUNK is executed ASYNCHRONOUSLY in different threads.
        auto seg_begin=argsVector.begin(), seg_end=argsVector.begin()+chunksize;
        do {
            auto dist = distance(seg_begin, seg_end);
            ++count;
            if (seg_end > argsVector.end())
                seg_end = argsVector.end();

            chunks.emplace_back(move(submit(map_chunk, fn, seg_begin, seg_end)));
            seg_begin = seg_end;
            seg_end += chunksize;
        } while (seg_begin < argsVector.end());


        // Wait for all chunks/futures to resolve
        while (!all_of(chunks.begin(), chunks.end(), [](future<void>& fut){return fut.wait_for(0s)==future_status::ready;}));
    }

    template<typename Signature, typename... Args>
    future<void> map_async(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1){
        /// Returns a future representing the results of a ThreadPool::map() call
        ///  that will be executed asynchronously. (Non-blocking)

        return async(launch::async, [&]() -> void {return map(fn, move(argsVector), chunksize);});
    }


    /// NON-VOID Variants
    // IMPORTANT: Return type MUST be 1st in template for Signature & CArgs types to be inferred
    template<typename ReturnType, typename Signature, typename... Args>
    future<ReturnType> submit(Signature func, Args&... args){
        /// Run a task asynchronously in the ThreadPool

        /// The Async Stuff
        auto promise = make_shared<::promise<ReturnType>>();
        // auto* promise = new ::promise<ReturnType>();     // Make sure this promise outlives the scope of this function
        //  so that it can be used by the ThreadPool to set the value
        //  of '.get()' on the future returned by this method.

        auto res = promise->get_future();   // This is the future which this call to '.submit()' will return.
        // It's value will be set whenever the ThreadPool gets around
        // to calling '.set_value()' on it's corresponding promise. (declared above)


        /// Package 'func' into a 'Task' object (aka 'function<void()>') for the ThreadPool to execute asynchronously
        function<ReturnType()> fn = bind(func, args...);                     // Hides func's Arguments
        Task task = bind([](decltype(fn) f, decltype(promise)& p){               // Hides func's return type

            /// TODO: FIND & FIX THE STACK OVERFLOW CAUSING err 0X800000003 (Encountered a breakpont)

            /// call the function & make its return value available
            ///   by calling '.get()' on its corresponding future (see 'The Async Stuff' above)
            if (auto res = f())
                p->set_value(res);      // heap-alloc'd promise no longer needed after calling it's set_value() method.
            //delete p;               // delete the promise to free its memory & avoid memory leaks.


        }, fn, promise); //(passes the func & promise into above lambda expression)


        /// Submit the 'task' to the ThreadPool
        {
            // Obtain exclusive access to the Jobs queue
            unique_lock<shared_mutex> lock(jqaccess);
            jobs.emplace(move(task));
            // End-of-scope releases exclusive access
        }
        // Alert next available thread of job waiting
        cv.notify_one();

        /// Return the future that represents func's return value.
        return res;
    }

    template<typename ReturnType, typename Signature, typename... Args>
    vector<ReturnType> map(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1) {
        /// Returns results of asynchronous operations in their original order. (Slower)
        /// Blocks until all operations are complete.
        
        auto results = make_shared<vector<ReturnType>>();
        results.reserve(argsVector.size());
        vector<future<vector<ReturnType>>> chunks;

        auto map_chunk = [](Signature fn, auto begin, auto end) -> vector<ReturnType>{
            vector<ReturnType> results;

            auto run_with_tuple_args = [&](Args... args) {
                return fn(args...);
            };  // using this wrapper lambda for call to submit in order to avoid passing 'fn' as tuple

            for (auto args_iter = begin; args_iter < end; ++args_iter) {
                results.emplace_back(move(apply(run_with_tuple_args, *args_iter))); // Use apply(fn, tuple<CArgs...>{args...}) to call a fn(args...)
            }
            return results;
        };

        // Submit jobs to the Pool in "chunks" of "chunksize" (or less) tasks.
        //  WITHIN a chunk, jobs are executed SEQUENTIALLY in the same thread,
        //  while EACH CHUNK is executed ASYNCHRONOUSLY in different threads.
        auto seg_begin=argsVector.begin(), seg_end=argsVector.begin()+chunksize;
        do {
            chunks.emplace_back(move(submit<vector<ReturnType>>(map_chunk, fn, seg_begin, seg_end)));
            seg_begin = seg_end;
            seg_end += chunksize;
        } while (seg_begin < argsVector.end());
        cout << "";

        // Wait for all chunks/futures to resolve
        while (!all_of(chunks.begin(), chunks.end(), [](future<ReturnType>& fut){return fut.wait_for(0s)==future_status::ready;}));

        // Extract results from completed chunk-futures into results vector
        for_each(chunks.begin(), chunks.end(), [&results](future<ReturnType>&& fut) mutable {
            auto& res = move(fut).get();
            results.insert(results->end(), res.begin(), res.end());
        });

        // Lose spent chunk-futures
        chunks.clear();
        return *results;
    }

    template<typename ReturnType, typename Signature, typename... Args>
    vector<ReturnType> map_unordered(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1) {
        /// Returns results of asynchronous operations in (more-or-less) the order of completion. (Faster)
        /// Blocks until all operations are complete.

        auto results = make_shared<vector<ReturnType>>();
        //auto* results = new vector<ReturnType>;
        //results->reserve(argsVector.size());
        vector<future<vector<ReturnType>>> chunks;

        auto map_chunk = [](Signature fn, auto begin, auto end) -> vector<ReturnType>{
            vector<ReturnType> results;

            auto run_with_tuple_args = [&](Args... args) {
                return fn(args...);
            };  // using this wrapper lambda for call to submit in order to avoid passing 'fn' as tuple

            for (auto args_iter = begin; args_iter < end; args_iter++) {
                results.emplace_back(move(apply(run_with_tuple_args, *args_iter))); // Use apply(fn, tuple<CArgs...>{args...}) to call a fn(args...)
            }
            return results;
        };
        
        // Submit jobs to the Pool in "chunks" of "chunksize" (or less) tasks.
        //  WITHIN a chunk, jobs are executed SEQUENTIALLY in the same thread,
        //  while EACH CHUNK is executed ASYNCHRONOUSLY in different threads.
        auto seg_begin=argsVector.begin(), seg_end=argsVector.begin()+chunksize;
        do {
            chunks.emplace_back(move(submit<vector<ReturnType>>(map_chunk, fn, seg_begin, seg_end)));
            seg_begin = seg_end;
            seg_end += chunksize;
        } while (seg_begin < argsVector.end());
        
        // Destructor pattern extracts & stores results as they become available
        //  while also removing completed futures from the futures vector
        while (!chunks.empty()) {
            for (auto it = chunks.begin(); it < chunks.end();) {
                if (it->wait_for(0s) == std::future_status::ready) {
                    auto res = move(it->get());
                    results->insert(results->end(), res.begin(), res.end());
                    it = chunks.erase(it); // remove the spent future from the vector and advance 'it'
                } else ++it;  // just advance 'it'
            }
        }

        return *results;
    }

    template<typename ReturnType, typename Signature, typename... Args>
    future<vector<ReturnType>> map_async(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1){
        /// Returns a future representing the results of a ThreadPool::map() call
        ///  that will be executed asynchronously. (Non-blocking)

        return async(launch::async, [&](){return map<ReturnType>(fn, move(argsVector), chunksize);});
    }

    template<typename ReturnType, typename Signature, typename... Args>
    future<vector<ReturnType>> map_unordered_async(Signature fn, vector<tuple<Args...>>&& argsVector, int chunksize=1){
        /// Returns a future representing the results of a ThreadPool::map_unordered() call
        ///  that will be executed asynchronously. (Non-blocking)

        return async(launch::async, [&](){return map_unordered<ReturnType>(fn, move(argsVector), chunksize);});
    }

};

#endif //MAILSEARCH_THREADPOOL_H

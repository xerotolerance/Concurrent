//
// Created by thecm on 10/15/2020.
//
#include "threadpool.h"
#include <string>
#include <utility>
using namespace std;
using namespace concurrent;


// Static Members
int ThreadPool::n_pools = 0;
shared_mutex ThreadPool::npaccess = shared_mutex();
int ThreadPool::pool_count() {
    shared_lock<shared_mutex> lock(npaccess);
    return n_pools;
}


// Constructors & Destructors
ThreadPool::ThreadPool(string name, int nthreads, chrono::seconds ttl)
        : nthreads(nthreads), running(true), nrunning(0), jobs(queue<Task>()), ttl(ttl), name(std::move(name)){
    {
        unique_lock<shared_mutex> lock(npaccess);
        ++n_pools;
    }
    manager = thread(&ThreadPool::control_loop, this); // Start manager thread (for dynamic load balancing)
    for (int i = 0; i < nthreads-1; i++)
        workers.emplace_back(thread(&ThreadPool::poll, this, this->ttl));   // Start nthreads-1 worker threads
}

ThreadPool::ThreadPool(int nthreads, chrono::seconds ttl)
    : ThreadPool{"Anon-ThreadPool_ID:_" + to_string(pool_count()), nthreads, ttl}{}

ThreadPool::~ThreadPool(){
    running = false;        // used by cv.wait_for(...) in Poll()
    manager.join();    // stop dynamic load balancing (assures no new workers are created)
    cv.notify_all();        // force all workers waiting on cv to re-evaluate their conditions
    for_each(workers.begin(), workers.end(), [](thread& t){t.join();}); // wait for workers to end
    workers.clear();        // lose thread objects
    {
        unique_lock<shared_mutex> lock(npaccess);
        --n_pools;
    }
}


// Accessors & Mutators
string ThreadPool::get_name() const { return name; }

chrono::seconds ThreadPool::get_timeout() const { return ttl; }
void ThreadPool::set_timeout(chrono::seconds _ttl) { ttl = _ttl; }

int ThreadPool::get_thread_count(){
    shared_lock<shared_mutex> lock(ntaccess);
    return nthreads;
}
void ThreadPool::set_thread_count(int n){
    unique_lock<shared_mutex> lock(ntaccess);
    nthreads = n;
}

int ThreadPool::num_active() {
    shared_lock<shared_mutex> lock(nraccess);
    return nrunning;
}

bool ThreadPool::is_running(){
    shared_lock<shared_mutex> lock(jqaccess);
    return (running || !jobs.empty());
}
bool ThreadPool::shutdown_requested() const{ return !running; }


// Internal methods
void ThreadPool::poll(chrono::seconds timeout){
    {
        unique_lock<shared_mutex> lock(nraccess);
        ++nrunning;
    }
    Task job;
    bool returned_in_time;
    while (true){
        { // begin lock scope
            unique_lock<shared_mutex> lock(jqaccess);

            // assure that remaining jobs finish before shutting down the pool
            if (!running && jobs.empty()) {
                break;
            }

            // while pool is running, block until job is available
            returned_in_time = cv.wait_for(lock, timeout, [this] {
                if (!running)
                    cv.notify_all();
                return !running || !jobs.empty();
            });
            if (returned_in_time && !jobs.empty()){
                job = jobs.front();
                jobs.pop();
            }
            else {
                if (running)
                    tsout(cout, " ", "\nTimeout: Thread", this_thread::get_id(), "exited after", to_string(timeout.count())+"s", "idle.\n");
                break;
            }
        } // end of scope releases lock
        job();  // run job
    }
    {
        unique_lock<shared_mutex> lock(nraccess);
        --nrunning;
    }
}

void ThreadPool::control_loop(){
    {
        unique_lock<shared_mutex> lock(nraccess);
        ++nrunning;
    }
    do {
        {
            shared_lock<shared_mutex> nr_lock(nraccess);
            shared_lock<shared_mutex> nt_lock(ntaccess);
            shared_lock<shared_mutex> q_lock(jqaccess);
            if (jobs.size() > (nrunning-1) * 2 && nrunning < nthreads) {
                workers.emplace_back(thread(&ThreadPool::poll, this, this->ttl));
            }
        }
    } while (running);
    {
        unique_lock<shared_mutex> lock(nraccess);
        --nrunning;
    }
}


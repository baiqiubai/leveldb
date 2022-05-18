
#include "blob/threadpool.h"

#include <unistd.h>

#include "include/leveldb/status.h"

namespace leveldb {

namespace ThreadConfig {
long GetCPUCore() { return ::sysconf(_SC_NPROCESSORS_ONLN); }

}  // namespace ThreadConfig

ThreadPool::ThreadPool(int thread_num)
    : thread_num_(thread_num), task_num_(0), stop_(true) {
  threads_.reserve(thread_num_);
}

void ThreadPool::Start() {
  stop_ = false;
  for (int i = 0; i < thread_num_; ++i) {
    threads_.emplace_back(std::unique_ptr<std::thread>(
        new std::thread(&ThreadPool::DoTask, this)));
  }
}

void ThreadPool::Stop() {
  stop_ = true;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.notify_all();
  }
  for (auto& thread : threads_) {
    thread->join();
  }
}

void ThreadPool::AddTask(Task&& task) {
  task_num_++;
  std::unique_lock<std::mutex> lock(mutex_);
  tasks_queue_.emplace_back(std::move(task));
  cv_.notify_one();
}

void ThreadPool::SetTaskNum(int task_num) { task_num_ = task_num; }

bool ThreadPool::AllTaskIsFinished() const { return task_num_.load() == 0; }

void ThreadPool::DoTask() {
  while (!stop_) {
    Task task = TakeTask();
    if (task != nullptr) {
      Status s = task();
      task_num_--;
      assert(s.ok());
    }
  }
}

ThreadPool::Task ThreadPool::TakeTask() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stop_ && tasks_queue_.empty()) {
    cv_.wait(lock);
  }

  if (stop_ || tasks_queue_.empty()) {
    return nullptr;
  }

  Task task = std::move(tasks_queue_.front());
  tasks_queue_.pop_front();
  lock.unlock();
  return task;
}

ThreadPool::~ThreadPool() {
  if (!stop_) {
    Stop();
  }
}

}  // namespace leveldb
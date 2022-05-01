

#ifndef STORAGE_LEVELDB_UTIL_THREADPOOL_H_
#define STORAGE_LEVELDB_UTIL_THREADPOOL_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "include/leveldb/status.h"

namespace leveldb {

namespace ThreadConfig {
long GetCPUCore();
}

class ThreadPool {
 public:
  explicit ThreadPool(int thread_num);

  using Task = std::function<Status()>;

  ~ThreadPool();

  void AddTask(ThreadPool::Task&& task);

  void SetTaskNum(int task_num);

  void Stop();

  void Start();

  int GetRemainTaskNum() const;

 private:
  void DoTask();

  Task TakeTask();

  const int thread_num_;
  std::atomic<int> task_num_;

  std::mutex mutex_;
  std::condition_variable cv_;

  std::atomic<bool> stop_;

  std::deque<Task> tasks_queue_;
  std::vector<std::unique_ptr<std::thread>> threads_;
};

}  // namespace leveldb

#endif

**This Project fork leveldb,Do Some Optimize**

[![ci](https://github.com/google/leveldb/actions/workflows/build.yml/badge.svg)](https://github.com/google/leveldb/actions/workflows/build.yml)

Leveldb Authors: Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com)

## Getting the Source
```
git clone --recurse-submodules https://github.com/baiqiubai/leveldb.git
```
## Building

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```
## Optimizations in This Project
```
Frist, For a large value, the KV separation strategy is adopted
Second,The traditional LSM is replaced by Fragmented LSM
Third,Adaptive cache elimination algorithm is used to resist the change of different loads
```

## Run benchmark

Here is a performance report (with explanations) from the run of the
included db_bench program.  The results are somewhat noisy, but should
be enough to get a ballpark performance estimate.
未完正在测试中...

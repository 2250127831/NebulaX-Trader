#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>

template<typename T>
class MemoryPool {
public:
    MemoryPool(size_t capacity) : capacity_(capacity) {
        pool_ = static_cast<T*>(std::aligned_alloc(alignof(T), sizeof(T) * capacity));
        if (!pool_) throw std::bad_alloc();
    }
    ~MemoryPool() { std::free(pool_); }
    T* allocate() { /* TODO */ return nullptr; }
    void deallocate(T*) { /* TODO */ }
private:
    T* pool_;
    size_t capacity_;
};

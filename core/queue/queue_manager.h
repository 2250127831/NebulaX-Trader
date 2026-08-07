#pragma once

#include "core/queue/spmc_byte_ring.h"
#include "core/queue/spmc_event_queue.h"
#include "core/queue/spsc_byte_ring.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <variant>

// ── 全局队列管理器 ──
// 统一管理无锁队列的创建、获取、销毁。每个线程都能访问（静态单例）。
//
// 用法：
//   uint8_t* byte_buf = new uint8_t[1 << 20];
//   size_t id0 = QueueManager::create(QueueManager::Type::SPSC_BYTE_RING, byte_buf, 1 << 20);
//   auto& byte_ring = QueueManager::get<SPSCByteRing>(id0);
//
//   MarketEvent* ev_slots = new MarketEvent[1 << 16];
//   size_t id1 = QueueManager::create(QueueManager::Type::SPMC_EVENT_QUEUE, ev_slots, 1 << 16, 3);
//   auto& ev_queue = QueueManager::get<SPMCEventQueue<16>>(id1);
//
// 存储空间由用户分配（堆 / 共享内存 / 内核映射），管理器只持有队列对象，不拥有 buf。
// id 按创建顺序分配；get<T>(id) 用 std::get 返回对应类型引用（类型不匹配会抛 std::bad_variant_access）。
class QueueManager {
public:
    enum class Type {
        SPSC_BYTE_RING,     // 字节流单消费者（发送方向等）
        SPMC_BYTE_RING,     // 字节流多消费者（第1级：收包→解析, V2.3）
        SPMC_EVENT_QUEUE,   // 定长槽位多消费者（第2级：通道A/B）
    };

    static constexpr size_t kMaxQueues = 16;

    // 创建队列：传类型 + 用户 buf + 容量，返回分配的 id。
    // capacity 必须 2 的幂（由队列构造校验）。
    static size_t create(Type type, void* buf, size_t capacity,
                         size_t max_consumers = 1) {
        if (count_ >= kMaxQueues) return static_cast<size_t>(-1);
        size_t id = count_++;
        switch (type) {
            case Type::SPSC_BYTE_RING:
                queues_[id] = std::make_unique<SPSCByteRing>(
                    static_cast<uint8_t*>(buf), capacity);
                break;
            case Type::SPMC_BYTE_RING:
                queues_[id] = std::make_unique<SPMCByteRing>(
                    static_cast<uint8_t*>(buf), capacity);
                break;
            case Type::SPMC_EVENT_QUEUE: {
                auto q = std::make_unique<SPMCEventQueue<16>>(
                    static_cast<MarketEvent*>(buf), capacity);
                q->set_num_consumers(max_consumers);
                queues_[id] = std::move(q);
                break;
            }
        }
        return id;
    }

    // 获取队列引用。T 必须与 create 时类型一致（否则 std::get 抛 bad_variant_access）。
    template <typename T>
    static T& get(size_t id) {
        return *std::get<std::unique_ptr<T>>(queues_[id]);
    }

    // 已创建的队列数
    static size_t count() { return count_; }

private:
    using AnyQueue = std::variant<std::unique_ptr<SPSCByteRing>,
                                  std::unique_ptr<SPMCByteRing>,
                                  std::unique_ptr<SPMCEventQueue<16>>>;

    static AnyQueue queues_[kMaxQueues];
    static size_t count_;
};

// 静态成员定义（头文件内，需 C++17 inline 或 .cpp）
inline std::variant<std::unique_ptr<SPSCByteRing>,
                    std::unique_ptr<SPMCByteRing>,
                    std::unique_ptr<SPMCEventQueue<16>>>
    QueueManager::queues_[QueueManager::kMaxQueues];
inline size_t QueueManager::count_ = 0;

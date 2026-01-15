#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "result.h"
#include "versioned_value.h"

namespace trelliskv {

class StorageEngine {
   public:
    struct CacheEntry {
        std::string key;
        VersionedValue value;
        std::optional<std::chrono::milliseconds> ttl;
        std::chrono::steady_clock::time_point expiry_time;
        std::chrono::steady_clock::time_point inserted_at;

        bool has_ttl() const { return ttl.has_value(); }
    };

   private:
    using LRUList = std::list<CacheEntry>;
    using LRUIterator = LRUList::iterator;

    LRUList lru_list_;
    std::unordered_map<std::string, LRUIterator> cache_map_;
    mutable std::shared_mutex data_mutex_;

    std::atomic<size_t> current_memory_bytes_{0};
    size_t max_capacity_;  // max number of entries (0 = unlimited)

    // TTL expiry thread
    std::thread expiry_thread_;
    std::mutex expiry_mutex_;
    std::condition_variable expiry_cv_;
    std::atomic<bool> stop_expiry_thread_{false};

    // Stats
    std::atomic<size_t> eviction_count_{0};
    std::atomic<size_t> expiry_count_{0};

    void evict_lru_if_needed();
    void move_to_front(LRUIterator it);
    bool is_expired(const CacheEntry& entry) const;
    void expiry_thread_func();
    void remove_entry_unlocked(LRUIterator it);

   public:
    explicit StorageEngine(size_t max_capacity = 0);  // 0 = unlimited
    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;

    Result<void> put(const std::string& key, const VersionedValue& value,
                     std::optional<std::chrono::milliseconds> ttl = std::nullopt);
    Result<VersionedValue> get(const std::string& key);
    Result<bool> remove(const std::string& key);
    Result<bool> contains(const std::string& key);
    size_t size() const;
    bool empty() const;
    void clear();

    std::vector<std::string> get_all_keys() const;
    std::pair<size_t, size_t> get_stats() const;

    void set_max_capacity(size_t capacity);
    size_t get_max_capacity() const;

    size_t evicted_count() const { return eviction_count_.load(); }
    size_t expired_count() const { return expiry_count_.load(); }
};

}  // namespace trelliskv

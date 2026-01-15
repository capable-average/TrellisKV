#include "trelliskv/storage_engine.h"

#include <algorithm>
#include <mutex>

#include "trelliskv/result.h"

namespace trelliskv {

StorageEngine::StorageEngine(size_t max_capacity) : max_capacity_(max_capacity) {
    expiry_thread_ = std::thread(&StorageEngine::expiry_thread_func, this);
}

StorageEngine::~StorageEngine() {
    stop_expiry_thread_.store(true);
    expiry_cv_.notify_all();
    
    if (expiry_thread_.joinable()) {
        expiry_thread_.join();
    }
}

void StorageEngine::expiry_thread_func() {
    while (!stop_expiry_thread_.load()) {
        std::unique_lock<std::mutex> lock(expiry_mutex_);
        // Wake up every 1 second or when signaled
        expiry_cv_.wait_for(lock, std::chrono::seconds(1), [this] {
            return stop_expiry_thread_.load();
        });

        if (stop_expiry_thread_.load()) {
            break;
        }

        // Scan and remove expired entries
        std::unique_lock<std::shared_mutex> data_lock(data_mutex_);
        auto now = std::chrono::steady_clock::now();
        
        auto it = lru_list_.begin();
        while (it != lru_list_.end()) {
            if (it->has_ttl() && it->expiry_time <= now) {
                auto to_remove = it++;
                cache_map_.erase(to_remove->key);
                lru_list_.erase(to_remove);
                expiry_count_.fetch_add(1);
            } else {
                ++it;
            }
        }
    }
}

bool StorageEngine::is_expired(const CacheEntry& entry) const {
    if (!entry.has_ttl()) {
        return false;
    }
    return std::chrono::steady_clock::now() >= entry.expiry_time;
}

void StorageEngine::move_to_front(LRUIterator it) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

void StorageEngine::remove_entry_unlocked(LRUIterator it) {
    cache_map_.erase(it->key);
    lru_list_.erase(it);
}

void StorageEngine::evict_lru_if_needed() {
    while (max_capacity_ > 0 && lru_list_.size() > max_capacity_) {
        auto& lru_entry = lru_list_.back();
        cache_map_.erase(lru_entry.key);
        lru_list_.pop_back();
        eviction_count_.fetch_add(1);
    }
}

Result<void> StorageEngine::put(const std::string& key,
                                const VersionedValue& value,
                                std::optional<std::chrono::milliseconds> ttl) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Update existing entry
            it->second->value = value;
            it->second->ttl = ttl;
            it->second->inserted_at = std::chrono::steady_clock::now();
            if (ttl.has_value()) {
                it->second->expiry_time = it->second->inserted_at + *ttl;
            }
            move_to_front(it->second);
        } else {
            CacheEntry entry;
            entry.key = key;
            entry.value = value;
            entry.ttl = ttl;
            entry.inserted_at = std::chrono::steady_clock::now();
            if (ttl.has_value()) {
                entry.expiry_time = entry.inserted_at + *ttl;
            }

            lru_list_.push_front(std::move(entry));
            cache_map_[key] = lru_list_.begin();

            evict_lru_if_needed();
        }

        return Result<void>::success();
    } catch (const std::exception& e) {
        return Result<void>::error("Failed to store key '" + key +
                                   "': " + e.what());
    }
}

Result<VersionedValue> StorageEngine::get(const std::string& key) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Check TTL expiration (lazy expiry)
            if (is_expired(*it->second)) {
                remove_entry_unlocked(it->second);
                expiry_count_.fetch_add(1);
                return Result<VersionedValue>::error("Key '" + key + "' not found");
            }

            move_to_front(it->second);
            return Result<VersionedValue>::success(it->second->value);
        } else {
            return Result<VersionedValue>::error("Key '" + key + "' not found");
        }
    } catch (const std::exception& e) {
        return Result<VersionedValue>::error("Failed to retrieve key '" + key +
                                             "': " + e.what());
    }
}

Result<bool> StorageEngine::remove(const std::string& key) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            remove_entry_unlocked(it->second);
            return Result<bool>::success(true);
        }
        return Result<bool>::success(false);
    } catch (const std::exception& e) {
        return Result<bool>::error("Failed to remove key '" + key +
                                   "': " + e.what());
    }
}

Result<bool> StorageEngine::contains(const std::string& key) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);

        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Check TTL expiration (lazy expiry)
            if (is_expired(*it->second)) {
                remove_entry_unlocked(it->second);
                expiry_count_.fetch_add(1);
                return Result<bool>::success(false);
            }
            move_to_front(it->second);
            return Result<bool>::success(true);
        }
        return Result<bool>::success(false);
    } catch (const std::exception& e) {
        return Result<bool>::error("Failed to look for key '" + key +
                                   "': " + e.what());
    }
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    return lru_list_.size();
}

bool StorageEngine::empty() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    return lru_list_.empty();
}

void StorageEngine::clear() {
    std::unique_lock<std::shared_mutex> lock(data_mutex_);
    lru_list_.clear();
    cache_map_.clear();
}

std::vector<std::string> StorageEngine::get_all_keys() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    std::vector<std::string> keys;
    keys.reserve(lru_list_.size());
    for (const auto& entry : lru_list_) {
        keys.push_back(entry.key);
    }
    return keys;
}

std::pair<size_t, size_t> StorageEngine::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    size_t total_keys = lru_list_.size();
    size_t total_memory = current_memory_bytes_.load();
    return std::make_pair(total_keys, total_memory);
}

void StorageEngine::set_max_capacity(size_t capacity) {
    std::unique_lock<std::shared_mutex> lock(data_mutex_);
    max_capacity_ = capacity;
    evict_lru_if_needed();
}

size_t StorageEngine::get_max_capacity() const {
    return max_capacity_;
}

}  // namespace trelliskv

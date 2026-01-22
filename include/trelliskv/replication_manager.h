#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

#include "result.h"
#include "types.h"

namespace trelliskv {

class HashRing;
class NetworkManager;
class StorageEngine;
class ThreadPool;
struct VersionedValue;
struct WriteOperation;
struct ReadResult;
struct ReplicationRequest;

class ReplicationManager {
   public:
    struct ReplicationStats {
        size_t total_writes_replicated = 0;
        size_t successful_replications = 0;
        size_t failed_replications = 0;
        size_t conflicts_resolved = 0;
        size_t concurrent_writes_detected = 0;
        size_t pending_replications = 0;
        std::chrono::system_clock::time_point start_time;
    };

    ReplicationManager(const NodeId& node_id, size_t replication_factor = 3);

    ~ReplicationManager();

    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    void initialize(HashRing* hash_ring, NetworkManager* network_manager,
                    StorageEngine* storage_engine);

    void start();
    void stop();

    Result<void> replicate_write(const WriteOperation& operation);
    Result<void> replicate_delete(const std::string& key,
                                  const TimestampVersion& version,
                                  const NodeId& original_deleter);
    Response handle_replication_request(const ReplicationRequest& request);
    Response handle_delete_replication(const std::string& key);
    ReadResult read_with_consistency(
        const std::string& key,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    VersionedValue resolve_conflicts(const std::vector<VersionedValue>& values);

    ReplicationStats get_stats() const;
    size_t get_replication_factor() const;
    size_t get_pending_operations_count() const;

   private:
    void async_replicate_to_nodes(const WriteOperation& operation,
                                  const std::vector<NodeId>& replica_nodes);
    Response apply_replicated_write(const ReplicationRequest& request);
    bool is_primary_for_key(const std::string& key) const;
    std::vector<NodeId> get_replica_nodes_for_key(const std::string& key) const;
    void update_stats(bool successful);
    bool validate_initialization() const;

    NodeId node_id_;
    size_t replication_factor_;
    std::atomic<bool> running_;
    std::atomic<bool> initialized_;
    std::chrono::milliseconds replication_timeout_;

    HashRing* hash_ring_;
    NetworkManager* network_manager_;
    StorageEngine* storage_engine_;

    std::unique_ptr<ThreadPool> replication_thread_pool_;

    mutable std::mutex stats_mutex_;
    ReplicationStats stats_;
};

}  // namespace trelliskv
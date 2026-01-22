#include "trelliskv/replication_manager.h"

#include <atomic>
#include <mutex>
#include <thread>

#include "trelliskv/hash_ring.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/messages.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/read_result.h"
#include "trelliskv/replication_request.h"
#include "trelliskv/storage_engine.h"
#include "trelliskv/thread_pool.h"
#include "trelliskv/versioned_value.h"
#include "trelliskv/write_operation.h"

namespace trelliskv {

ReplicationManager::ReplicationManager(const NodeId& node_id,
                                       size_t replication_factor)
    : node_id_(node_id),
      replication_factor_(replication_factor),
      running_(false),
      initialized_(false),
      replication_timeout_(std::chrono::milliseconds(1000)),
      hash_ring_(nullptr),
      network_manager_(nullptr),
      storage_engine_(nullptr),
      replication_thread_pool_(std::make_unique<ThreadPool>(8)) {
    if (replication_factor_ == 0) {
        throw std::invalid_argument("Replication factor must be greater than 0");
    }
    stats_.start_time = std::chrono::system_clock::now();
}

ReplicationManager::~ReplicationManager() { stop(); }

void ReplicationManager::initialize(HashRing* hash_ring,
                                    NetworkManager* network_manager,
                                    StorageEngine* storage_engine) {
    hash_ring_ = hash_ring;
    network_manager_ = network_manager;
    storage_engine_ = storage_engine;
    initialized_ = true;
}

void ReplicationManager::start() {
    if (!validate_initialization()) {
        throw std::runtime_error("ReplicationManager not properly initialized");
    }
    running_ = true;
}

void ReplicationManager::stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

Result<void> ReplicationManager::replicate_write(
    const WriteOperation& operation) {
    if (!validate_initialization() || !running_) {
        return Result<void>::error("ReplicationManager not running");
    }

    TimestampVersion write_version = operation.version;

    VersionedValue versioned_value(operation.value, write_version,
                                   operation.original_writer);
    versioned_value.timestamp = operation.timestamp;

    std::optional<std::chrono::milliseconds> ttl_chrono;
    if (versioned_value.ttl_ms.has_value()) {
        ttl_chrono = std::chrono::milliseconds(versioned_value.ttl_ms.value());
    }

    auto local_result = storage_engine_->put(operation.key, versioned_value, ttl_chrono);
    if (!local_result.is_success()) {
        update_stats(false);
        return Result<void>::error("Failed to apply write locally: " +
                                   local_result.error());
    }

    WriteOperation updated_operation = operation;
    updated_operation.version = write_version;

    auto replica_nodes = get_replica_nodes_for_key(operation.key);

    if (!replica_nodes.empty()) {
        async_replicate_to_nodes(updated_operation, replica_nodes);
    }

    update_stats(true);
    return Result<void>::success();
}

Result<void> ReplicationManager::replicate_delete(
    const std::string& key, const TimestampVersion& version,
    const NodeId& original_deleter) {
    if (!validate_initialization() || !running_) {
        return Result<void>::error("ReplicationManager not running");
    }

    auto local_result = storage_engine_->remove(key);
    if (!local_result.is_success()) {
        update_stats(false);
        return Result<void>::error("Failed to delete locally: " +
                                   local_result.error());
    }

    auto replica_nodes = get_replica_nodes_for_key(key);

    if (!replica_nodes.empty()) {
        for (const auto& node_id : replica_nodes) {
            replication_thread_pool_->enqueue(
                [this, node_id, key, version, original_deleter]() {
                    try {
                        auto node_info = hash_ring_->get_node_info(node_id);
                        if (!node_info) {
                            update_stats(false);
                            return;
                        }

                        DeleteRequest delete_request(key);
                        delete_request.expected_version = version;
                        delete_request.consistency = ConsistencyLevel::EVENTUAL;
                        delete_request.is_replication = true;
                        delete_request.sender_id = node_id_;
                        delete_request.request_id =
                            "repl_del_" +
                            std::to_string(std::chrono::duration_cast<
                                               std::chrono::microseconds>(
                                               std::chrono::system_clock::now()
                                                   .time_since_epoch())
                                               .count());

                        auto response = network_manager_->send_request(
                            node_info->address, delete_request,
                            std::chrono::milliseconds(50));
                        update_stats(response.is_success());

                    } catch (const std::exception& e) {
                        update_stats(false);
                    }
                });
        }
    }

    update_stats(true);
    return Result<void>::success();
}

Response ReplicationManager::handle_delete_replication(const std::string& key) {
    if (!validate_initialization() || !running_) {
        return Response::error("ReplicationManager not running");
    }

    auto result = storage_engine_->remove(key);
    if (result.is_success()) {
        return Response::success();
    } else {
        return Response::error("Failed to delete replicated key: " +
                               result.error());
    }
}

Response ReplicationManager::handle_replication_request(
    const ReplicationRequest& request) {
    if (!validate_initialization() || !running_) {
        return Response::error("ReplicationManager not running");
    }

    return apply_replicated_write(request);
}

ReadResult ReplicationManager::read_with_consistency(const std::string& key,
                                                     ConsistencyLevel) {
    if (!validate_initialization() || !running_) {
        return ReadResult::error("ReplicationManager not running");
    }

    auto result = storage_engine_->get(key);
    if (result.is_success()) {
        return ReadResult::success(result.value());
    } else if (result.error().find("not found") != std::string::npos) {
        return ReadResult::not_found();
    } else {
        return ReadResult::error(result.error());
    }
}

VersionedValue ReplicationManager::resolve_conflicts(
    const std::vector<VersionedValue>& values) {
    if (values.empty()) {
        throw std::invalid_argument(
            "Cannot resolve conflicts with empty values list");
    }

    if (values.size() == 1) {
        return values[0];
    }

    VersionedValue winner = values[0];

    for (size_t i = 1; i < values.size(); ++i) {
        const auto& current = values[i];

        if (current.version.is_newer_than(winner.version)) {
            winner = current;
        }
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.conflicts_resolved++;
    }

    return winner;
}

ReplicationManager::ReplicationStats ReplicationManager::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

size_t ReplicationManager::get_replication_factor() const {
    return replication_factor_;
}

size_t ReplicationManager::get_pending_operations_count() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_.pending_replications;
}

void ReplicationManager::async_replicate_to_nodes(
    const WriteOperation& operation, const std::vector<NodeId>& replica_nodes) {
    ReplicationRequest request(operation.key, operation.value,
                               operation.version, operation.original_writer);
    request.timestamp = operation.timestamp;

    for (const auto& node_id : replica_nodes) {
        replication_thread_pool_->enqueue([this, node_id, request]() {
            try {
                auto node_info = hash_ring_->get_node_info(node_id);
                if (!node_info) {
                    update_stats(false);
                    return;
                }

                PutRequest put_request(request.key, request.value);
                put_request.expected_version = request.version;
                put_request.consistency = ConsistencyLevel::EVENTUAL;
                put_request.is_replication = true;
                put_request.sender_id = node_id_;
                put_request.request_id =
                    "repl_" +
                    std::to_string(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());

                auto response = network_manager_->send_request(
                    node_info->address, put_request,
                    std::chrono::milliseconds(50));
                update_stats(response.is_success());

            } catch (const std::exception& e) {
                update_stats(false);
            }
        });
    }
}

Response ReplicationManager::apply_replicated_write(
    const ReplicationRequest& request) {
    try {
        auto existing_result = storage_engine_->get(request.key);

        VersionedValue new_value(request.value, request.version,
                                 request.original_writer);
        new_value.timestamp = request.timestamp;

        if (existing_result.is_success()) {
            auto existing_value = existing_result.value();

            bool should_apply = false;

            if (new_value.version.is_newer_than(existing_value.version)) {
                should_apply = true;
            } else if (new_value.version.timestamp ==
                       existing_value.version.timestamp) {
                should_apply = (new_value.version.last_writer >=
                                existing_value.version.last_writer);
            }

            if (should_apply) {
                std::optional<std::chrono::milliseconds> ttl_chrono;
                if (new_value.ttl_ms.has_value()) {
                    ttl_chrono = std::chrono::milliseconds(new_value.ttl_ms.value());
                }

                auto put_result = storage_engine_->put(request.key, new_value, ttl_chrono);
                if (put_result.is_success()) {
                    return Response::success();
                } else {
                    return Response::error(
                        "Failed to store replicated value: " +
                        put_result.error());
                }
            } else {
                return Response::success();
            }
        } else {
            std::optional<std::chrono::milliseconds> ttl_chrono;
            if (new_value.ttl_ms.has_value()) {
                ttl_chrono = std::chrono::milliseconds(new_value.ttl_ms.value());
            }

            auto put_result = storage_engine_->put(request.key, new_value, ttl_chrono);
            if (put_result.is_success()) {
                return Response::success();
            } else {
                return Response::error(
                    "Failed to store new replicated value: " +
                    put_result.error());
            }
        }
    } catch (const std::exception& e) {
        return Response::error("Exception during replication: " +
                               std::string(e.what()));
    }
}

bool ReplicationManager::is_primary_for_key(const std::string& key) const {
    if (!hash_ring_) {
        return false;
    }

    auto primary_node = hash_ring_->get_primary_node(key);
    return primary_node == node_id_;
}

std::vector<NodeId> ReplicationManager::get_replica_nodes_for_key(
    const std::string& key) const {
    if (!hash_ring_) {
        return {};
    }

    auto all_replicas = hash_ring_->get_replica_nodes(key, replication_factor_);

    std::vector<NodeId> replica_nodes;
    for (const auto& node_id : all_replicas) {
        if (node_id != node_id_) {
            replica_nodes.push_back(node_id);
        }
    }

    return replica_nodes;
}

void ReplicationManager::update_stats(bool successful) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_writes_replicated++;
    if (successful) {
        stats_.successful_replications++;
    } else {
        stats_.failed_replications++;
    }
}

bool ReplicationManager::validate_initialization() const {
    return initialized_ && hash_ring_ && network_manager_ && storage_engine_;
}

}  // namespace trelliskv
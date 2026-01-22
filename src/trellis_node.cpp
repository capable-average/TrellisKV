#include "trelliskv/trellis_node.h"

#include <random>

#include "trelliskv/cluster_state.h"
#include "trelliskv/connection_pool.h"
#include "trelliskv/gossip_protocol.h"
#include "trelliskv/hash_ring.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/logger.h"
#include "trelliskv/messages.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/node_config.h"
#include "trelliskv/node_info.h"
#include "trelliskv/read_result.h"
#include "trelliskv/replication_manager.h"
#include "trelliskv/replication_request.h"
#include "trelliskv/request_router.h"
#include "trelliskv/storage_engine.h"
#include "trelliskv/write_operation.h"

namespace trelliskv {

TrellisNode::TrellisNode(const NodeId& node_id, const NodeConfig& config)
    : node_id_(node_id), config_(config), running_(false) {
    if (config_.replication_factor < 1) {
        throw std::invalid_argument("Replication factor must be >= 1");
    }
    if (config_.replication_factor > config_.max_cluster_size) {
        throw std::invalid_argument(
            "Replication factor (" + std::to_string(config_.replication_factor) +
            ") cannot exceed max cluster size (" +
            std::to_string(config_.max_cluster_size) + ")");
    }

    network_manager_ = std::make_unique<NetworkManager>(config_.address.port);

    network_manager_->configure_uds(config_.enable_uds, config_.uds_socket_dir);

    storage_engine_ = std::make_unique<StorageEngine>(config_.storage_max_capacity);

    hash_ring_ = std::make_unique<HashRing>();
    connection_pool_ = std::make_unique<ConnectionPool>(10);

    connection_pool_->configure_uds(config_.enable_uds, config_.address.hostname,
                                    config_.address.port, config_.uds_socket_dir);

    request_router_ = std::make_unique<RequestRouter>(
        node_id_, hash_ring_.get(), connection_pool_.get(),
        config_.replication_factor);

    NodeInfo local_node_info(node_id_, config_.address, NodeState::ACTIVE);
    gossip_protocol_ = std::make_unique<GossipProtocol>(
        node_id_, local_node_info, network_manager_.get(),
        config_.heartbeat_interval, config_.failure_timeout);

    replication_manager_ = std::make_unique<ReplicationManager>(
        node_id_, config_.replication_factor);

    network_manager_->set_message_handler(
        [this](const Request& request) -> std::unique_ptr<Response> {
            return handle_request(request);
        });
}

TrellisNode::~TrellisNode() { stop(); }

NodeConfig TrellisNode::create_default_config(const std::string& hostname,
                                              uint16_t port) {
    NodeConfig config;
    config.address = NodeAddress(hostname, port);
    config.replication_factor = 3;
    config.virtual_nodes_per_physical = 50;
    config.default_consistency = ConsistencyLevel::EVENTUAL;
    config.heartbeat_interval = std::chrono::milliseconds(1000);
    config.failure_timeout = std::chrono::milliseconds(5000);
    return config;
}

Result<void> TrellisNode::start() {
    if (running_.load()) {
        return Result<void>::error("Node is already running");
    }

    auto start_result = network_manager_->start_server(config_.address.port);
    if (!start_result) {
        return Result<void>::error("Failed to start network manager: " +
                                   start_result.error());
    }

    NodeInfo self_info;
    self_info.id = node_id_;
    self_info.address = config_.address;
    self_info.state = NodeState::ACTIVE;
    self_info.last_seen = std::chrono::system_clock::now();

    hash_ring_->add_node(self_info);

    replication_manager_->initialize(hash_ring_.get(), network_manager_.get(),
                                     storage_engine_.get());

    gossip_protocol_->set_node_failure_callback([this](const NodeId& node_id) {
        hash_ring_->remove_node(node_id);
        auto node_info = hash_ring_->get_node_info(node_id);
        if (node_info) {
            connection_pool_->close_connections_to_node(node_info->address);
        }
    });

    gossip_protocol_->set_node_join_callback(
        [this](const NodeInfo& node_info) { hash_ring_->add_node(node_info); });

    gossip_protocol_->set_hash_ring(hash_ring_.get());

    gossip_protocol_->set_cluster_change_callback(
        [this](const ClusterState& state) {
            for (const auto& [id, info] : state.nodes) {
                if (id == node_id_) continue;
                if (info.is_active()) {
                    if (!hash_ring_->get_node_info(id).has_value()) {
                        hash_ring_->add_node(info);
                    }
                } else if (info.is_failed()) {
                    hash_ring_->remove_node(id);
                }
            }
        });

    replication_manager_->start();
    gossip_protocol_->start();

    running_.store(true);
    start_time_ = std::chrono::steady_clock::now();
    return Result<void>::success();
}

void TrellisNode::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    try {
        if (gossip_protocol_ && gossip_protocol_->is_running()) {
            GossipMessage departure_message(node_id_);
            departure_message.known_nodes.clear();

            auto active_nodes = gossip_protocol_->get_active_nodes();
            for (const auto& node_id : active_nodes) {
                if (node_id != node_id_) {
                    auto node_info = gossip_protocol_->get_node_info(node_id);
                    if (node_info) {
                        try {
                            auto json_message_result =
                                JsonSerializer::serialize_gossip_message(
                                    departure_message);
                            if (json_message_result) {
                                network_manager_->send_message_async(
                                    node_info->address,
                                    json_message_result.value());
                            }
                        } catch (const std::exception& e) {
                            LOG_WARN("Failed to send departure message: " +
                                     std::string(e.what()));
                        }
                    }
                }
            }

            gossip_protocol_->remove_node(node_id_);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        auto wait_start = std::chrono::system_clock::now();
        const auto max_wait_time = std::chrono::seconds(5);

        if (connection_pool_) {
            while (connection_pool_->get_stats().active_requests > 0) {
                auto elapsed = std::chrono::system_clock::now() - wait_start;
                if (elapsed > max_wait_time) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (storage_engine_ && replication_manager_ && request_router_) {
            try {
                auto all_keys = storage_engine_->get_all_keys();

                const size_t max_transfer_attempts = 3;

                for (const auto& key : all_keys) {
                    if (request_router_->is_key_local(key)) {
                        auto value_result = storage_engine_->get(key);
                        if (value_result.is_success()) {
                            WriteOperation transfer_op(
                                key, value_result.value().value,
                                value_result.value().version, node_id_,
                                ConsistencyLevel::EVENTUAL);

                            bool transfer_successful = false;
                            for (size_t attempt = 0;
                                 attempt < max_transfer_attempts &&
                                 !transfer_successful;
                                 ++attempt) {
                                auto replicate_result =
                                    replication_manager_->replicate_write(
                                        transfer_op);
                                if (replicate_result.is_success()) {
                                    transfer_successful = true;
                                } else {
                                    if (attempt < max_transfer_attempts - 1) {
                                        std::this_thread::sleep_for(
                                            std::chrono::milliseconds(500));
                                    }
                                }
                            }
                        }
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(1000));

            } catch (const std::exception& e) {
                LOG_ERROR("Error during data transfer on shutdown: " +
                          std::string(e.what()));
            }
        }

        if (gossip_protocol_) {
            gossip_protocol_->set_node_failure_callback(nullptr);
            gossip_protocol_->set_node_recovery_callback(nullptr);
            gossip_protocol_->set_cluster_change_callback(nullptr);
            gossip_protocol_->set_node_join_callback(nullptr);
            gossip_protocol_->stop();
        }

        if (replication_manager_) {
            replication_manager_->stop();
        }

        if (network_manager_) {
            network_manager_->stop_server();
        }

        if (connection_pool_) {
            connection_pool_->close_all_connections();
        }

    } catch (const std::exception& e) {
        if (gossip_protocol_) {
            gossip_protocol_->stop();
        }
        if (replication_manager_) {
            replication_manager_->stop();
        }
        if (network_manager_) {
            network_manager_->stop_server();
        }
        if (connection_pool_) {
            connection_pool_->close_all_connections();
        }
    }
}

bool TrellisNode::is_running() const { return running_.load(); }

const NodeConfig& TrellisNode::get_config() const { return config_; }

const NodeId& TrellisNode::get_node_id() const { return node_id_; }

NodeInfo TrellisNode::get_node_info() const {
    NodeInfo info;
    info.id = node_id_;
    info.address = config_.address;
    info.state = running_.load() ? NodeState::ACTIVE : NodeState::FAILED;
    info.last_seen = std::chrono::system_clock::now();
    return info;
}

Result<void> TrellisNode::join_cluster() {
    if (config_.seed_nodes.empty()) {
        return Result<void>::success();
    }

    auto discovery_result = discover_cluster_from_seeds();
    if (!discovery_result) {
        return Result<void>::error("Failed to discover cluster: " +
                                   discovery_result.error());
    }

    auto bootstrap_result = bootstrap_from_cluster();
    if (!bootstrap_result) {
        return Result<void>::error("Failed to bootstrap from cluster: " +
                                   bootstrap_result.error());
    }

    NodeInfo local_info = get_node_info();
    gossip_protocol_->add_node(local_info);

    return Result<void>::success();
}

void TrellisNode::add_node(const NodeInfo& node) {
    hash_ring_->add_node(node);
    gossip_protocol_->add_node(node);
}

void TrellisNode::remove_node(const NodeId& node_id) {
    hash_ring_->remove_node(node_id);
    gossip_protocol_->remove_node(node_id);

    auto node_info = hash_ring_->get_node_info(node_id);
    if (node_info) {
        connection_pool_->close_connections_to_node(node_info->address);
    }
}

TrellisNode::ClusterStats TrellisNode::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;
    stats.total_nodes = hash_ring_->size();
    stats.active_connections = connection_pool_->get_stats().total_connections;
    stats.local_keys = storage_engine_->size();
    return stats;
}

std::unique_ptr<Response> TrellisNode::handle_request(const Request& request) {
    if (!running_.load()) {
        return std::make_unique<Response>(
            Response::error("Node is not running"));
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.total_requests_handled++;
    }

    if (const auto* get_req = dynamic_cast<const GetRequest*>(&request)) {
        auto resp = handle_get_request(*get_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* put_req = dynamic_cast<const PutRequest*>(&request)) {
        auto resp = handle_put_request(*put_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* delete_req = dynamic_cast<const DeleteRequest*>(&request)) {
        auto resp = handle_delete_request(*delete_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* discovery_req =
            dynamic_cast<const ClusterDiscoveryRequest*>(&request)) {
        auto resp = handle_cluster_discovery_request(*discovery_req);
        return resp;
    }
    if (const auto* bootstrap_req =
            dynamic_cast<const BootstrapRequest*>(&request)) {
        auto resp = handle_bootstrap_request(*bootstrap_req);
        return resp;
    }
    if (const auto* heartbeat_req =
            dynamic_cast<const HeartbeatRequest*>(&request)) {
        auto resp = std::make_unique<HeartbeatResponse>(
            heartbeat_req->sequence_number, NodeState::ACTIVE);
        resp->request_id = heartbeat_req->request_id;
        resp->responder_id = node_id_;

        auto sender_info =
            gossip_protocol_->get_node_info(heartbeat_req->sender_id);
        if (sender_info) {
            gossip_protocol_->handle_heartbeat(*heartbeat_req,
                                               sender_info->address);
        }

        return resp;
    }
    if (const auto* health_req =
            dynamic_cast<const HealthCheckRequest*>(&request)) {
        return handle_health_check_request(*health_req);
    }
    LOG_WARN("Unknown request type received");
    return std::make_unique<Response>(Response::error("Unknown request type"));
}

Response TrellisNode::handle_get_request(const GetRequest& request) {
    const bool is_internal = (!request.sender_id.empty() &&
                              (request.sender_id == node_id_ ||
                               request.sender_id.rfind("node_", 0) == 0));
    if (is_internal) {
        auto read_result = replication_manager_->read_with_consistency(
            request.key, ConsistencyLevel::EVENTUAL);
        if (read_result.is_success()) {
            const auto& versioned_value = read_result.value.value();
            Response response = Response::success(versioned_value.value,
                                                  versioned_value.version);
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
        if (read_result.is_not_found()) {
            Response response = Response::not_found();
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
        return Response::error("Read failed: " + read_result.error_message);
    }

    const bool is_local = request_router_->is_key_local(request.key);
    if (!is_local) {
        auto forward_result = request_router_->forward_get_request(request);
        if (forward_result) {
            return *forward_result.value();
        }
    }

    auto read_result = replication_manager_->read_with_consistency(
        request.key, request.consistency);

    if (read_result.is_success()) {
        const auto& versioned_value = read_result.value.value();
        Response response =
            Response::success(versioned_value.value, versioned_value.version);
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }

    if (read_result.is_not_found()) {
        if (is_local && request.consistency == ConsistencyLevel::EVENTUAL) {
            auto forward_result = request_router_->forward_get_request(request);
            if (forward_result && forward_result.value()->is_success()) {
                return *forward_result.value();
            }
            try {
                std::vector<NodeId> tried =
                    request_router_->get_replica_nodes_for_key(request.key);
                tried.push_back(
                    request_router_->get_primary_node_for_key(request.key));
                auto active = gossip_protocol_->get_active_nodes();
                for (const auto& nid : active) {
                    if (nid == node_id_) continue;
                    if (std::find(tried.begin(), tried.end(), nid) !=
                        tried.end())
                        continue;
                    auto alt =
                        request_router_->forward_request_to_node(nid, request);
                    if (alt && alt.value()->is_success()) {
                        return *alt.value();
                    }
                }
            } catch (const std::exception& e) {
                LOG_WARN("Exception during fallback node search: " +
                         std::string(e.what()));
            }
        }

        Response response = Response::not_found();
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }

    if (read_result.is_conflict()) {
        Response response = Response::conflict();
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        response.error_message = read_result.error_message;
        return response;
    }
    return Response::error("Read failed: " + read_result.error_message);
}

Response TrellisNode::handle_put_request(const PutRequest& request) {
    if (request.is_replication) {
        ReplicationRequest repl_request(
            request.key, request.value,
            request.expected_version.value_or(TimestampVersion{}),
            request.sender_id);

        auto result =
            replication_manager_->handle_replication_request(repl_request);
        Response response = result;
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }

    if (request_router_->is_key_local(request.key)) {
        WriteOperation write_op(
            request.key, request.value,
            request.expected_version.value_or(TimestampVersion{}), node_id_,
            request.consistency);

        write_op.version = TimestampVersion::now(node_id_);

        auto result = replication_manager_->replicate_write(write_op);
        if (result) {
            Response response = Response::success("", write_op.version);
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        } else {
            return Response::error("Failed to replicate write: " +
                                   result.error());
        }
    } else {
        // Forward to responsible node
        auto forward_result = request_router_->forward_put_request(request);
        if (forward_result) {
            return *forward_result.value();
        } else {
            return Response::error("Failed to forward request: " +
                                   forward_result.error());
        }
    }
}

Response TrellisNode::handle_delete_request(const DeleteRequest& request) {
    if (request.is_replication) {
        auto result =
            replication_manager_->handle_delete_replication(request.key);
        result.request_id = request.request_id;
        result.responder_id = node_id_;
        return result;
    }

    if (request_router_->is_key_local(request.key)) {
        TimestampVersion delete_version = TimestampVersion::now(node_id_);

        auto result = replication_manager_->replicate_delete(
            request.key, delete_version, node_id_);

        if (result) {
            Response response = Response::success("", delete_version);
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        } else {
            return Response::error("Failed to delete value: " + result.error());
        }
    } else {
        auto forward_result = request_router_->forward_delete_request(request);
        if (forward_result) {
            return *forward_result.value();
        } else {
            return Response::error("Failed to forward request: " +
                                   forward_result.error());
        }
    }
}

std::unique_ptr<Response> TrellisNode::handle_health_check_request(
    const HealthCheckRequest& request) {
    auto response = std::make_unique<HealthCheckResponse>();

    response->request_id = request.request_id;
    response->responder_id = node_id_;
    response->status = ResponseStatus::OK;
    response->node_id = node_id_;
    response->node_state =
        running_.load() ? NodeState::ACTIVE : NodeState::FAILED;
    response->is_healthy = running_.load();

    if (running_.load()) {
        auto now = std::chrono::steady_clock::now();
        auto uptime_duration =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time_);
        auto hours =
            std::chrono::duration_cast<std::chrono::hours>(uptime_duration)
                .count();
        auto minutes = std::chrono::duration_cast<std::chrono::minutes>(
                           uptime_duration % std::chrono::hours(1))
                           .count();
        auto seconds = (uptime_duration % std::chrono::minutes(1)).count();

        std::stringstream uptime_ss;
        uptime_ss << hours << "h " << minutes << "m " << seconds << "s";
        response->uptime = uptime_ss.str();
    } else {
        response->uptime = "0s";
    }

    if (request.include_details) {
        response->total_nodes = hash_ring_->size();
        response->active_connections =
            connection_pool_->get_stats().total_connections;
        response->local_keys = storage_engine_->size();

        std::lock_guard<std::mutex> lock(stats_mutex_);
        response->total_requests = stats_.total_requests_handled;
    }

    return response;
}

std::string TrellisNode::generate_request_id() const {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t id = dis(gen);
    std::stringstream ss;
    ss << std::hex << id;
    return ss.str();
}

Result<void> TrellisNode::discover_cluster_from_seeds() {
    std::vector<ClusterState> discovered_states;
    size_t successful_contacts = 0;

    for (const auto& seed_address : config_.seed_nodes) {
        auto cluster_state_result = contact_seed_node(seed_address);
        if (cluster_state_result) {
            discovered_states.push_back(cluster_state_result.value());
            successful_contacts++;
        }
    }

    if (successful_contacts == 0) {
        return Result<void>::error(
            "Failed to contact any seed nodes for cluster discovery");
    }

    for (const auto& state : discovered_states) {
        merge_discovered_cluster_state(state);
    }

    return Result<void>::success();
}

Result<ClusterState> TrellisNode::contact_seed_node(
    const NodeAddress& seed_address) {
    try {
        ClusterDiscoveryRequest discovery_request;
        discovery_request.requesting_node_id = node_id_;
        discovery_request.requesting_node_address = config_.address;
        discovery_request.request_id = generate_request_id();

        auto response_result = connection_pool_->send_request(
            seed_address, discovery_request, std::chrono::milliseconds(5000));

        if (!response_result) {
            return Result<ClusterState>::error(
                "Failed to send discovery request: " + response_result.error());
        }

        const auto& response = response_result.value();
        if (response->status != ResponseStatus::OK) {
            return Result<ClusterState>::error("Discovery request failed: " +
                                               response->error_message);
        }

        if (const auto* discovery_response =
                dynamic_cast<const ClusterDiscoveryResponse*>(response.get())) {
            if (discovery_response->replication_factor != 0 &&
                discovery_response->replication_factor != config_.replication_factor) {
                LOG_WARN("Replication factor mismatch: cluster RF=" +
                         std::to_string(discovery_response->replication_factor) +
                         ", adopting cluster's RF.");
                config_.replication_factor = discovery_response->replication_factor;
            }

            if (discovery_response->cluster_state) {
                return Result<ClusterState>::success(
                    *discovery_response->cluster_state);
            } else {
                return Result<ClusterState>::error(
                    "Null cluster state in discovery response");
            }
        } else {
            return Result<ClusterState>::error(
                "Invalid response type for cluster discovery");
        }

    } catch (const std::exception& e) {
        return Result<ClusterState>::error(
            "Exception during seed node contact: " + std::string(e.what()));
    }
}

void TrellisNode::merge_discovered_cluster_state(
    const ClusterState& discovered_state) {
    for (const auto& [node_id, node_info] : discovered_state.nodes) {
        if (node_id == node_id_) {
            continue;
        }

        if (node_info.is_active()) {
            hash_ring_->add_node(node_info);
            gossip_protocol_->add_node(node_info);
        }
    }
}

Result<void> TrellisNode::bootstrap_from_cluster() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto active_nodes = hash_ring_->get_all_nodes();
    if (active_nodes.empty()) {
        return Result<void>::success();
    }

    size_t successful_syncs = 0;
    const size_t min_successful_syncs =
        std::min(static_cast<size_t>(2), active_nodes.size());
    const size_t max_retry_attempts = 3;

    for (const auto& node_id : active_nodes) {
        if (node_id == node_id_) {
            continue;
        }

        bool node_success = false;
        for (size_t attempt = 0; attempt < max_retry_attempts && !node_success;
             ++attempt) {
            try {
                if (attempt > 0) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(1000 * attempt));
                }

                BootstrapRequest bootstrap_request;
                bootstrap_request.requesting_node_id = node_id_;
                bootstrap_request.requesting_node_address = config_.address;
                bootstrap_request.request_id = generate_request_id();

                auto node_info = hash_ring_->get_node_info(node_id);
                if (!node_info) {
                    break;
                }

                auto timeout = (attempt == 0) ? std::chrono::milliseconds(10000)
                                              : std::chrono::milliseconds(5000);
                auto response_result = connection_pool_->send_request(
                    node_info->address, bootstrap_request, timeout);

                if (response_result) {
                    const auto& response = response_result.value();
                    if (response->status == ResponseStatus::OK) {
                        successful_syncs++;
                        node_success = true;

                        if (const auto* bootstrap_response =
                                dynamic_cast<const BootstrapResponse*>(
                                    response.get())) {
                            if (bootstrap_response->cluster_state) {
                                merge_discovered_cluster_state(
                                    *bootstrap_response->cluster_state);

                                for (const auto& peer_id :
                                     bootstrap_response->recommended_peers) {
                                    auto peer_info =
                                        bootstrap_response->cluster_state->nodes
                                            .find(peer_id);
                                    if (peer_info !=
                                        bootstrap_response->cluster_state->nodes
                                            .end()) {
                                        gossip_protocol_->add_node(
                                            peer_info->second);
                                    }
                                }
                            }
                        }
                    }
                }

            } catch (const std::exception& e) {
                LOG_WARN("Bootstrap attempt failed for node " + node_id + ": " +
                         std::string(e.what()));
            }
        }

        if (successful_syncs >= min_successful_syncs) {
            break;
        }
    }

    if (successful_syncs == 0) {
        return Result<void>::error(
            "Failed to bootstrap from any cluster nodes after retries");
    }

    return Result<void>::success();
}

std::unique_ptr<Response> TrellisNode::handle_cluster_discovery_request(
    const ClusterDiscoveryRequest& request) {
    try {
        if (request.requesting_node_id.empty()) {
            return std::make_unique<Response>(Response::error(
                "Invalid cluster discovery request: missing node ID"));
        }

        if (request.requesting_node_address.hostname.empty() ||
            request.requesting_node_address.port == 0) {
            return std::make_unique<Response>(Response::error(
                "Invalid cluster discovery request: invalid node address"));
        }

        auto cluster_state = gossip_protocol_->get_cluster_state();

        auto requesting_node_it =
            cluster_state.nodes.find(request.requesting_node_id);
        if (requesting_node_it == cluster_state.nodes.end()) {
            NodeInfo requesting_node(request.requesting_node_id,
                                     request.requesting_node_address,
                                     NodeState::ACTIVE);
            cluster_state.nodes[request.requesting_node_id] = requesting_node;

            gossip_protocol_->add_node(requesting_node);
            hash_ring_->add_node(requesting_node);

            try {
                auto active_nodes = gossip_protocol_->get_active_nodes();
                for (const auto& peer_id : active_nodes) {
                    if (peer_id != node_id_ &&
                        peer_id != request.requesting_node_id) {
                        gossip_protocol_->exchange_metadata(peer_id);
                    }
                }
            } catch (const std::exception& e) {
                LOG_WARN(
                    "Failed to exchange metadata with peers during "
                    "discovery: " +
                    std::string(e.what()));
            }
        } else {
            requesting_node_it->second.address =
                request.requesting_node_address;
            requesting_node_it->second.update_last_seen();
        }

        auto cluster_state_ptr = std::make_shared<ClusterState>(cluster_state);
        ClusterDiscoveryResponse discovery_response(cluster_state_ptr, node_id_,
                                                    cluster_state.nodes.size(),
                                                    config_.replication_factor);
        discovery_response.status = ResponseStatus::OK;
        discovery_response.request_id = request.request_id;
        discovery_response.responder_id = node_id_;

        return std::make_unique<ClusterDiscoveryResponse>(
            std::move(discovery_response));

    } catch (const std::exception& e) {
        return std::make_unique<Response>(
            Response::error("Failed to process cluster discovery request: " +
                            std::string(e.what())));
    }
}

std::unique_ptr<Response> TrellisNode::handle_bootstrap_request(
    const BootstrapRequest& request) {
    try {
        if (request.requesting_node_id.empty()) {
            return std::make_unique<Response>(
                Response::error("Invalid bootstrap request: missing node ID"));
        }

        if (request.requesting_node_address.hostname.empty() ||
            request.requesting_node_address.port == 0) {
            return std::make_unique<Response>(Response::error(
                "Invalid bootstrap request: invalid node address"));
        }

        auto cluster_state = gossip_protocol_->get_cluster_state();

        auto requesting_node_it =
            cluster_state.nodes.find(request.requesting_node_id);
        if (requesting_node_it == cluster_state.nodes.end()) {
            NodeInfo requesting_node(request.requesting_node_id,
                                     request.requesting_node_address,
                                     NodeState::ACTIVE);
            cluster_state.nodes[request.requesting_node_id] = requesting_node;

            gossip_protocol_->add_node(requesting_node);
            hash_ring_->add_node(requesting_node);

            try {
                auto active_nodes = gossip_protocol_->get_active_nodes();
                for (const auto& peer_id : active_nodes) {
                    if (peer_id != node_id_ &&
                        peer_id != request.requesting_node_id) {
                        gossip_protocol_->exchange_metadata(peer_id);
                    }
                }
            } catch (const std::exception& e) {
                LOG_WARN(
                    "Failed to exchange metadata with peers during "
                    "bootstrap: " +
                    std::string(e.what()));
            }
        } else {
            requesting_node_it->second.address =
                request.requesting_node_address;
            requesting_node_it->second.state = NodeState::ACTIVE;
            requesting_node_it->second.update_last_seen();
        }

        auto cluster_state_ptr = std::make_shared<ClusterState>(cluster_state);
        BootstrapResponse bootstrap_response(cluster_state_ptr, node_id_);
        bootstrap_response.status = ResponseStatus::OK;
        bootstrap_response.request_id = request.request_id;
        bootstrap_response.responder_id = node_id_;

        auto active_nodes = gossip_protocol_->get_active_nodes();
        size_t max_recommended_peers =
            std::min(static_cast<size_t>(5), active_nodes.size());
        size_t added_peers = 0;

        for (const auto& node_id : active_nodes) {
            if (node_id != node_id_ && node_id != request.requesting_node_id &&
                added_peers < max_recommended_peers) {
                bootstrap_response.recommended_peers.push_back(node_id);
                added_peers++;
            }
        }

        return std::make_unique<BootstrapResponse>(
            std::move(bootstrap_response));

    } catch (const std::exception& e) {
        return std::make_unique<Response>(Response::error(
            "Failed to process bootstrap request: " + std::string(e.what())));
    }
}

}  // namespace trelliskv
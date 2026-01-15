#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"
#include "node_info.h"
#include "types.h"

namespace trelliskv {

struct ClusterState;

// Requests
struct Request {
    std::string request_id;
    NodeId sender_id;

    Request() = default;
    Request(const std::string& id, const NodeId& sender)
        : request_id(id), sender_id(sender) {}
    virtual ~Request() = default;

    virtual void to_json(nlohmann::json& json) const;
    virtual void from_json(const nlohmann::json& json);
};

struct GetRequest : public Request {
    std::string key;
    ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL;
    std::optional<TimestampVersion> client_version;

    GetRequest() = default;
    GetRequest(const std::string& k,
               ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : key(k), consistency(c) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct PutRequest : public Request {
    std::string key;
    std::string value;
    std::optional<TimestampVersion> expected_version;
    ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL;
    bool is_replication = false;
    std::optional<int64_t> ttl_ms;  // TTL in milliseconds, nullopt = no expiry

    PutRequest() = default;
    PutRequest(const std::string& k, const std::string& v,
               ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : key(k), value(v), consistency(c) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct DeleteRequest : public Request {
    std::string key;
    std::optional<TimestampVersion> expected_version;
    ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL;
    bool is_replication = false;

    DeleteRequest() = default;
    DeleteRequest(const std::string& k,
                  ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : key(k), consistency(c) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

// Batch requests
struct BatchPutRequest : public Request {
    struct KeyValue {
        std::string key;
        std::string value;
    };

    std::vector<KeyValue> items;
    ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL;

    BatchPutRequest() = default;
    explicit BatchPutRequest(const std::vector<KeyValue>& kvs,
                             ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : items(kvs), consistency(c) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct BatchGetRequest : public Request {
    std::vector<std::string> keys;
    ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL;

    BatchGetRequest() = default;
    explicit BatchGetRequest(const std::vector<std::string>& k,
                             ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : keys(k), consistency(c) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct Response {
    std::string request_id;
    ResponseStatus status;
    std::optional<std::string> value;
    std::optional<TimestampVersion> version;
    std::string error_message;
    NodeId responder_id;

    Response() = default;
    Response(ResponseStatus s) : status(s) {}
    Response(ResponseStatus s, const std::string& val,
             const TimestampVersion& ver)
        : status(s), value(val), version(ver) {}
    virtual ~Response() = default;

    bool is_success() const { return status == ResponseStatus::OK; }
    bool is_not_found() const { return status == ResponseStatus::NOT_FOUND; }
    bool is_error() const { return status == ResponseStatus::ERROR; }
    bool is_conflict() const { return status == ResponseStatus::CONFLICT; }
    bool is_timeout() const { return status == ResponseStatus::TIMEOUT; }

    static Response success(const std::string& val = "",
                            const TimestampVersion& ver = TimestampVersion{}) {
        return Response(ResponseStatus::OK, val, ver);
    }

    static Response not_found() { return Response(ResponseStatus::NOT_FOUND); }

    static Response error(const std::string& message) {
        Response resp(ResponseStatus::ERROR);
        resp.error_message = message;
        return resp;
    }

    static Response conflict() {
        Response resp(ResponseStatus::CONFLICT);
        resp.error_message = "Multiple conflicting values found";
        return resp;
    }

    static Response timeout() {
        Response resp(ResponseStatus::TIMEOUT);
        resp.error_message = "Request timed out";
        return resp;
    }

    virtual void to_json(nlohmann::json& json) const;
    virtual void from_json(const nlohmann::json& json);
};

// Batch responses
struct BatchPutResponse : public Response {
    struct ResultItem {
        std::string key;
        bool success;
        std::string error_message;
    };

    std::vector<ResultItem> results;
    size_t successful_count = 0;
    size_t failed_count = 0;

    BatchPutResponse() = default;

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct BatchGetResponse : public Response {
    struct ResultItem {
        std::string key;
        bool found;
        std::string value;
        std::string error_message;
    };

    std::vector<ResultItem> results;
    size_t found_count = 0;
    size_t not_found_count = 0;

    BatchGetResponse() = default;

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

// Cluster Discovery
struct ClusterDiscoveryRequest : public Request {
    NodeId requesting_node_id;
    NodeAddress requesting_node_address;

    ClusterDiscoveryRequest() = default;
    ClusterDiscoveryRequest(const NodeId& node_id, const NodeAddress& address)
        : requesting_node_id(node_id), requesting_node_address(address) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct ClusterDiscoveryResponse : public Response {
    std::shared_ptr<ClusterState> cluster_state;
    NodeId responding_node_id;
    size_t cluster_size;

    ClusterDiscoveryResponse() = default;
    ClusterDiscoveryResponse(std::shared_ptr<ClusterState> state,
                             const NodeId& responder, size_t size)
        : cluster_state(state),
          responding_node_id(responder),
          cluster_size(size) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

// Bootstrap
struct BootstrapRequest : public Request {
    NodeId requesting_node_id;
    NodeAddress requesting_node_address;

    BootstrapRequest() = default;
    BootstrapRequest(const NodeId& node_id, const NodeAddress& address)
        : requesting_node_id(node_id), requesting_node_address(address) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct BootstrapResponse : public Response {
    std::shared_ptr<ClusterState> cluster_state;
    NodeId responding_node_id;
    std::vector<NodeId> recommended_peers;

    BootstrapResponse() = default;
    BootstrapResponse(std::shared_ptr<ClusterState> state,
                      const NodeId& responder)
        : cluster_state(state), responding_node_id(responder) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

// Heartbeats
struct HeartbeatRequest : public Request {
    uint64_t sequence_number;
    NodeState sender_state;
    Timestamp timestamp;

    HeartbeatRequest() = default;
    HeartbeatRequest(const NodeId& sender, uint64_t seq, NodeState state)
        : Request("", sender),
          sequence_number(seq),
          sender_state(state),
          timestamp(std::chrono::system_clock::now()) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct HeartbeatResponse : public Response {
    uint64_t sequence_number;
    NodeState responder_state;
    Timestamp timestamp;

    HeartbeatResponse() = default;
    HeartbeatResponse(uint64_t seq, NodeState state)
        : Response(ResponseStatus::OK),
          sequence_number(seq),
          responder_state(state),
          timestamp(std::chrono::system_clock::now()) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct HealthCheckRequest : public Request {
    bool include_details = false;

    HealthCheckRequest() = default;
    explicit HealthCheckRequest(bool details) : include_details(details) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct HealthCheckResponse : public Response {
    std::string node_id;
    NodeState node_state;
    bool is_healthy;
    std::string uptime;

    size_t active_connections = 0;
    size_t total_nodes = 0;
    size_t local_keys = 0;
    size_t total_requests = 0;

    HealthCheckResponse() = default;
    HealthCheckResponse(const std::string& id, NodeState state, bool healthy,
                        const std::string& up)
        : Response(ResponseStatus::OK),
          node_id(id),
          node_state(state),
          is_healthy(healthy),
          uptime(up) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

}  // namespace trelliskv
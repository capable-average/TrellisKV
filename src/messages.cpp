#include "trelliskv/messages.h"

#include "nlohmann/json.hpp"
#include "trelliskv/cluster_state.h"
#include "trelliskv/json_serializer.h"

namespace trelliskv {

// Base Request
void Request::to_json(nlohmann::json& json) const {
    json["request_id"] = request_id;
    json["sender_id"] = sender_id;
}

void Request::from_json(const nlohmann::json& json) {
    if (json.contains("request_id")) {
        request_id = json["request_id"];
    }
    if (json.contains("sender_id")) {
        sender_id = json["sender_id"];
    }
}

// GetRequest
void GetRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "GET";
    json["key"] = key;
    json["consistency"] =
        JsonSerializer::consistency_level_to_string(consistency);
    if (client_version.has_value()) {
        json["client_version"] =
            JsonSerializer::serialize_timestamp_version(client_version.value());
    }
}

void GetRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("consistency")) {
        auto result =
            JsonSerializer::string_to_consistency_level(json["consistency"]);
        if (result) {
            consistency = result.value();
        }
    }
    if (json.contains("client_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["client_version"]);
        if (result) {
            client_version = result.value();
        }
    }
}

// PutRequest
void PutRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "PUT";
    json["key"] = key;
    json["value"] = value;
    json["consistency"] =
        JsonSerializer::consistency_level_to_string(consistency);
    json["is_replication"] = is_replication;
    if (expected_version.has_value()) {
        json["expected_version"] = JsonSerializer::serialize_timestamp_version(
            expected_version.value());
    }
    if (ttl_ms.has_value()) {
        json["ttl_ms"] = ttl_ms.value();
    }
}

void PutRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("value")) {
        value = json["value"];
    }
    if (json.contains("consistency")) {
        auto result =
            JsonSerializer::string_to_consistency_level(json["consistency"]);
        if (result) {
            consistency = result.value();
        }
    }
    if (json.contains("is_replication")) {
        is_replication = json["is_replication"];
    }
    if (json.contains("expected_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["expected_version"]);
        if (result) {
            expected_version = result.value();
        }
    }
    if (json.contains("ttl_ms")) {
        ttl_ms = json["ttl_ms"].get<int64_t>();
    }
}

// DeleteRequest
void DeleteRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "DELETE";
    json["key"] = key;
    json["consistency"] =
        JsonSerializer::consistency_level_to_string(consistency);
    json["is_replication"] = is_replication;
    if (expected_version.has_value()) {
        json["expected_version"] = JsonSerializer::serialize_timestamp_version(
            expected_version.value());
    }
}

void DeleteRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("consistency")) {
        auto result =
            JsonSerializer::string_to_consistency_level(json["consistency"]);
        if (result) {
            consistency = result.value();
        }
    }
    if (json.contains("is_replication")) {
        is_replication = json["is_replication"];
    }
    if (json.contains("expected_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["expected_version"]);
        if (result) {
            expected_version = result.value();
        }
    }
}

// Batch Requests
void BatchPutRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "BATCH_PUT";
    json["consistency"] =
        JsonSerializer::consistency_level_to_string(consistency);
    nlohmann::json items_json = nlohmann::json::array();
    for (const auto& item : items) {
        items_json.push_back({{"key", item.key}, {"value", item.value}});
    }
    json["items"] = items_json;
}

void BatchPutRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("consistency")) {
        auto result =
            JsonSerializer::string_to_consistency_level(json["consistency"]);
        if (result) {
            consistency = result.value();
        }
    }
    if (json.contains("items") && json["items"].is_array()) {
        items.clear();
        for (const auto& item : json["items"]) {
            KeyValue kv;
            if (item.contains("key")) kv.key = item["key"];
            if (item.contains("value")) kv.value = item["value"];
            items.push_back(kv);
        }
    }
}

void BatchGetRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "BATCH_GET";
    json["consistency"] =
        JsonSerializer::consistency_level_to_string(consistency);
    json["keys"] = keys;
}

void BatchGetRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("consistency")) {
        auto result =
            JsonSerializer::string_to_consistency_level(json["consistency"]);
        if (result) {
            consistency = result.value();
        }
    }
    if (json.contains("keys")) {
        keys = json["keys"].get<std::vector<std::string>>();
    }
}

// ClusterDiscoveryRequest
void ClusterDiscoveryRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "CLUSTER_DISCOVERY";
    json["requesting_node_id"] = requesting_node_id;
    json["requesting_node_address"] =
        JsonSerializer::serialize_node_address(requesting_node_address);
}

void ClusterDiscoveryRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("requesting_node_id")) {
        requesting_node_id = json["requesting_node_id"];
    }
    if (json.contains("requesting_node_address")) {
        auto result = JsonSerializer::deserialize_node_address(
            json["requesting_node_address"]);
        if (result) {
            requesting_node_address = result.value();
        }
    }
}

// BootstrapRequest
void BootstrapRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "BOOTSTRAP";
    json["requesting_node_id"] = requesting_node_id;
    json["requesting_node_address"] =
        JsonSerializer::serialize_node_address(requesting_node_address);
}

void BootstrapRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("requesting_node_id")) {
        requesting_node_id = json["requesting_node_id"];
    }
    if (json.contains("requesting_node_address")) {
        auto result = JsonSerializer::deserialize_node_address(
            json["requesting_node_address"]);
        if (result) {
            requesting_node_address = result.value();
        }
    }
}

// HeartbeatRequest
void HeartbeatRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "HEARTBEAT";
    json["sequence_number"] = sequence_number;
    json["sender_state"] = JsonSerializer::node_state_to_string(sender_state);
    json["timestamp"] = JsonSerializer::serialize_timestamp(timestamp);
}

void HeartbeatRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("sequence_number")) {
        sequence_number = json["sequence_number"];
    }
    if (json.contains("sender_state")) {
        auto result =
            JsonSerializer::string_to_node_state(json["sender_state"]);
        if (result) {
            sender_state = result.value();
        }
    }
    if (json.contains("timestamp")) {
        auto result = JsonSerializer::deserialize_timestamp(json["timestamp"]);
        if (result) {
            timestamp = result.value();
        }
    }
}

// HealthCheckRequest
void HealthCheckRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "HEALTH_CHECK";
    json["include_details"] = include_details;
}

void HealthCheckRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("include_details")) {
        include_details = json["include_details"];
    }
}

// Base Response
void Response::to_json(nlohmann::json& json) const {
    json["request_id"] = request_id;
    json["status"] = JsonSerializer::response_status_to_string(status);
    json["responder_id"] = responder_id;

    if (value.has_value()) {
        json["value"] = value.value();
    }
    if (version.has_value()) {
        json["version"] =
            JsonSerializer::serialize_timestamp_version(version.value());
    }
    if (!error_message.empty()) {
        json["error_message"] = error_message;
    }
}

void Response::from_json(const nlohmann::json& json) {
    if (json.contains("request_id")) {
        request_id = json["request_id"];
    }
    if (json.contains("status")) {
        auto result = JsonSerializer::string_to_response_status(json["status"]);
        if (result) {
            status = result.value();
        }
    }
    if (json.contains("responder_id")) {
        responder_id = json["responder_id"];
    }
    if (json.contains("value")) {
        value = json["value"].get<std::string>();
    }
    if (json.contains("version")) {
        auto result =
            JsonSerializer::deserialize_timestamp_version(json["version"]);
        if (result) {
            version = result.value();
        }
    }
    if (json.contains("error_message")) {
        error_message = json["error_message"];
    }
}

// Batch Responses
void BatchPutResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "BATCH_PUT_RESPONSE";
    json["successful_count"] = successful_count;
    json["failed_count"] = failed_count;
    nlohmann::json results_json = nlohmann::json::array();
    for (const auto& result : results) {
        results_json.push_back({{"key", result.key},
                                {"success", result.success},
                                {"error_message", result.error_message}});
    }
    json["results"] = results_json;
}

void BatchPutResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("successful_count")) {
        successful_count = json["successful_count"];
    }
    if (json.contains("failed_count")) {
        failed_count = json["failed_count"];
    }
    if (json.contains("results") && json["results"].is_array()) {
        results.clear();
        for (const auto& item : json["results"]) {
            ResultItem ri;
            if (item.contains("key")) ri.key = item["key"];
            if (item.contains("success")) ri.success = item["success"];
            if (item.contains("error_message"))
                ri.error_message = item["error_message"];
            results.push_back(ri);
        }
    }
}

void BatchGetResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "BATCH_GET_RESPONSE";
    json["found_count"] = found_count;
    json["not_found_count"] = not_found_count;
    nlohmann::json results_json = nlohmann::json::array();
    for (const auto& result : results) {
        results_json.push_back({{"key", result.key},
                                {"found", result.found},
                                {"value", result.value},
                                {"error_message", result.error_message}});
    }
    json["results"] = results_json;
}

void BatchGetResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("found_count")) {
        found_count = json["found_count"];
    }
    if (json.contains("not_found_count")) {
        not_found_count = json["not_found_count"];
    }
    if (json.contains("results") && json["results"].is_array()) {
        results.clear();
        for (const auto& item : json["results"]) {
            ResultItem ri;
            if (item.contains("key")) ri.key = item["key"];
            if (item.contains("found")) ri.found = item["found"];
            if (item.contains("value")) ri.value = item["value"];
            if (item.contains("error_message"))
                ri.error_message = item["error_message"];
            results.push_back(ri);
        }
    }
}

// ClusterDiscoveryResponse
void ClusterDiscoveryResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "CLUSTER_DISCOVERY_RESPONSE";
    json["responding_node_id"] = responding_node_id;
    json["cluster_size"] = cluster_size;
    json["replication_factor"] = replication_factor;
    if (cluster_state) {
        json["cluster_state"] =
            JsonSerializer::serialize_cluster_state(*cluster_state);
    }
}

void ClusterDiscoveryResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("responding_node_id")) {
        responding_node_id = json["responding_node_id"];
    }
    if (json.contains("cluster_size")) {
        cluster_size = json["cluster_size"];
    }
    if (json.contains("replication_factor")) {
        replication_factor = json["replication_factor"];
    }
    if (json.contains("cluster_state")) {
        auto result =
            JsonSerializer::deserialize_cluster_state(json["cluster_state"]);
        if (result) {
            cluster_state = std::make_shared<ClusterState>(result.value());
        }
    }
}

// BootstrapResponse
void BootstrapResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "BOOTSTRAP_RESPONSE";
    json["responding_node_id"] = responding_node_id;
    if (cluster_state) {
        json["cluster_state"] =
            JsonSerializer::serialize_cluster_state(*cluster_state);
    }
    json["recommended_peers"] = recommended_peers;
}

void BootstrapResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("responding_node_id")) {
        responding_node_id = json["responding_node_id"];
    }
    if (json.contains("cluster_state")) {
        auto result =
            JsonSerializer::deserialize_cluster_state(json["cluster_state"]);
        if (result) {
            cluster_state = std::make_shared<ClusterState>(result.value());
        }
    }
    if (json.contains("recommended_peers")) {
        recommended_peers =
            json["recommended_peers"].get<std::vector<NodeId>>();
    }
}

// HeartbeatResponse
void HeartbeatResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "HEARTBEAT_RESPONSE";
    json["sequence_number"] = sequence_number;
    json["responder_state"] =
        JsonSerializer::node_state_to_string(responder_state);
    json["timestamp"] = JsonSerializer::serialize_timestamp(timestamp);
}

void HeartbeatResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("sequence_number")) {
        sequence_number = json["sequence_number"];
    }
    if (json.contains("responder_state")) {
        auto result =
            JsonSerializer::string_to_node_state(json["responder_state"]);
        if (result) {
            responder_state = result.value();
        }
    }
    if (json.contains("timestamp")) {
        auto result = JsonSerializer::deserialize_timestamp(json["timestamp"]);
        if (result) {
            timestamp = result.value();
        }
    }
}

// HealthCheckResponse
void HealthCheckResponse::to_json(nlohmann::json& json) const {
    Response::to_json(json);
    json["type"] = "HEALTH_CHECK_RESPONSE";
    json["node_id"] = node_id;
    json["node_state"] = JsonSerializer::node_state_to_string(node_state);
    json["is_healthy"] = is_healthy;
    json["uptime"] = uptime;
    json["active_connections"] = active_connections;
    json["total_nodes"] = total_nodes;
    json["local_keys"] = local_keys;
    json["total_requests"] = total_requests;
}

void HealthCheckResponse::from_json(const nlohmann::json& json) {
    Response::from_json(json);
    if (json.contains("node_id")) {
        node_id = json["node_id"];
    }
    if (json.contains("node_state")) {
        auto result = JsonSerializer::string_to_node_state(json["node_state"]);
        if (result) {
            node_state = result.value();
        }
    }
    if (json.contains("is_healthy")) {
        is_healthy = json["is_healthy"];
    }
    if (json.contains("uptime")) {
        uptime = json["uptime"];
    }
    if (json.contains("active_connections")) {
        active_connections = json["active_connections"];
    }
    if (json.contains("total_nodes")) {
        total_nodes = json["total_nodes"];
    }
    if (json.contains("local_keys")) {
        local_keys = json["local_keys"];
    }
    if (json.contains("total_requests")) {
        total_requests = json["total_requests"];
    }
}

}  // namespace trelliskv

#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "node_info.h"
#include "result.h"

namespace trelliskv {

struct Request;
struct Response;

class TcpClient {
   public:
    TcpClient();
    ~TcpClient();

    Result<void> connect(
        const NodeAddress& server_address,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    void disconnect();
    bool is_connected() const;

    void set_prefer_uds(bool prefer, const std::string& uds_dir = "/tmp/trelliskv");

    // Request operations
    Result<Response> send_get_request(
        const std::string& key,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    Result<Response> send_put_request(
        const std::string& key, const std::string& value,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    Result<Response> send_delete_request(
        const std::string& key,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    Result<std::unique_ptr<Response>> send_health_check_request(
        bool include_details = false);

    // Batch operations
    Result<std::unique_ptr<Response>> send_batch_put_request(
        const std::vector<std::pair<std::string, std::string>>& items,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    Result<std::unique_ptr<Response>> send_batch_get_request(
        const std::vector<std::string>& keys,
        ConsistencyLevel consistency = ConsistencyLevel::EVENTUAL);

    // Low-level request sending
    Result<Response> send_request(const Request& request);

    // Connection info
    NodeAddress get_server_address() const { return server_address_; }

   private:
    int socket_fd_;
    bool connected_;
    NodeAddress server_address_;
    bool is_uds_connection_ = false;

    bool prefer_uds_ = true;
    std::string uds_socket_dir_ = "/tmp/trelliskv";

    // Helper methods
    Result<void> create_socket();
    Result<void> try_uds_connect(const NodeAddress& server_address,
                                 std::chrono::milliseconds timeout);
    bool is_local_address(const std::string& hostname) const;
    std::string get_uds_path(uint16_t port) const;
    Result<std::string> receive_message();
    Result<void> send_message(const std::string& message);
    Result<void> set_socket_timeout(std::chrono::milliseconds timeout);
    void close_socket();

    std::string generate_request_id();
};

}  // namespace trelliskv
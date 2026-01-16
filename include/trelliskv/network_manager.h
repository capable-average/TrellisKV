#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "result.h"
#include "types.h"

namespace trelliskv {

class ThreadPool;
class ConnectionPool;
struct Request;
struct Response;
struct NodeAddress;

class NetworkManager {
   public:
    using MessageHandler =
        std::function<std::unique_ptr<Response>(const Request&)>;
    using ConnectionId = int;

    explicit NetworkManager(uint16_t port = 0);
    ~NetworkManager();

    Result<void> start_server(uint16_t port);
    void stop_server();
    bool is_server_running() const;
    uint16_t get_server_port() const;
    void configure_uds(bool enabled, const std::string& socket_dir);

    void set_message_handler(MessageHandler handler);

    Result<std::unique_ptr<Response>> send_request(
        const NodeAddress& target, const Request& request,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

    void send_message_async(const NodeAddress& target,
                            const std::string& message);

    void close_connection(ConnectionId conn_id);
    void close_all_connections();
    size_t get_active_connections_count() const;

    struct NetworkStats {
        size_t total_connections_accepted = 0;
        size_t active_connections = 0;
        size_t total_requests_handled = 0;
        size_t total_requests_sent = 0;
        size_t failed_requests = 0;
        std::chrono::system_clock::time_point start_time;
    };

    NetworkStats get_stats() const;

   private:
    void accept_connections();
    void accept_uds_connections();
    void handle_client_connection(ConnectionId conn_id, int client_socket);
    Result<std::string> receive_message(int socket);
    Result<void> send_message(int socket, const std::string& message);
    void cleanup_connection(ConnectionId conn_id);
    Result<void> set_socket_options(int socket);
    Result<void> set_socket_timeout(int socket,
                                    std::chrono::milliseconds timeout);
    void close_socket(int socket);
    Result<void> start_uds_server();
    void cleanup_uds_socket();

    int server_socket_;
    uint16_t server_port_;
    std::atomic<bool> server_running_;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<ConnectionPool> connection_pool_;
    std::atomic<ConnectionId> next_connection_id_;
    std::thread accept_thread_;

    bool uds_enabled_ = false;
    int uds_socket_ = -1;
    std::string uds_path_;
    std::thread uds_accept_thread_;

    mutable std::mutex connections_mutex_;
    std::unordered_map<ConnectionId, std::thread> active_connections_;

    MessageHandler message_handler_;

    mutable std::mutex stats_mutex_;
    NetworkStats stats_;
};

}  // namespace trelliskv
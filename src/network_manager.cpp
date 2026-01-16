#include "trelliskv/network_manager.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <iostream>
#include <thread>

#include "trelliskv/connection_pool.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/messages.h"
#include "trelliskv/node_info.h"
#include "trelliskv/thread_pool.h"

namespace trelliskv {

class SocketGuard {
    int socket_;
    bool released_ = false;

    void graceful_close() {
        if (socket_ < 0) return;

        shutdown(socket_, SHUT_RDWR);
        close(socket_);
    }

   public:
    explicit SocketGuard(int socket) : socket_(socket) {}
    ~SocketGuard() {
        if (!released_ && socket_ >= 0) {
            graceful_close();
        }
    }
    int get() const { return socket_; }
    void release() { released_ = true; }

    SocketGuard(const SocketGuard&) = delete;
    SocketGuard& operator=(const SocketGuard&) = delete;
};

NetworkManager::NetworkManager(uint16_t port)
    : server_socket_(-1),
      server_port_(port),
      server_running_(false),
      thread_pool_(std::make_unique<ThreadPool>(
          std::thread::hardware_concurrency() * 2)),
      connection_pool_(std::make_unique<ConnectionPool>(20)),
      next_connection_id_(1) {
    stats_.start_time = std::chrono::system_clock::now();
}

NetworkManager::~NetworkManager() { stop_server(); }

Result<void> NetworkManager::start_server(uint16_t port) {
    if (server_running_.load()) {
        return Result<void>::error("Server is already running");
    }

    // Create socket
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0) {
        return Result<void>::error("Failed to create socket: " +
                                   std::string(strerror(errno)));
    }

    // Set socket options
    auto opt_result = set_socket_options(server_socket_);
    if (!opt_result) {
        close_socket(server_socket_);
        return opt_result;
    }

    // Bind
    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_socket_, (struct sockaddr*)&server_addr,
             sizeof(server_addr)) < 0) {
        close_socket(server_socket_);
        return Result<void>::error("Failed to bind socket to port " +
                                   std::to_string(port) + ": " +
                                   std::string(strerror(errno)));
    }

    // Listen for connections
    const int backlog = 128;
    if (listen(server_socket_, backlog) < 0) {
        close_socket(server_socket_);
        return Result<void>::error("Failed to listen on socket: " +
                                   std::string(strerror(errno)));
    }

    if (port == 0) {
        struct sockaddr_in addr{};
        socklen_t addr_len = sizeof(addr);
        if (getsockname(server_socket_, (struct sockaddr*)&addr, &addr_len) ==
            0) {
            server_port_ = ntohs(addr.sin_port);
        }
    } else {
        server_port_ = port;
    }

    server_running_.store(true);

    // Set sockets to non-blocking mode
    int flags = fcntl(server_socket_, F_GETFL, 0);
    fcntl(server_socket_, F_SETFL, flags | O_NONBLOCK);

    // Start accept threads
    accept_thread_ = std::thread(&NetworkManager::accept_connections, this);

    // UDS if enabled
    if (uds_enabled_) {
        auto uds_result = start_uds_server();
        if (!uds_result) {
            std::cerr << "Warning: Failed to start UDS server: " 
                      << uds_result.error() << std::endl;
        }
    }

    return Result<void>::success();
}

void NetworkManager::stop_server() {
    if (!server_running_.load()) {
        return;
    }

    server_running_.store(false);

    if (server_socket_ >= 0) {
        close_socket(server_socket_);
        server_socket_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    close_all_connections();
    thread_pool_->shutdown();
}

bool NetworkManager::is_server_running() const {
    return server_running_.load();
}

uint16_t NetworkManager::get_server_port() const { return server_port_; }

void NetworkManager::set_message_handler(MessageHandler handler) {
    message_handler_ = std::move(handler);
}

Result<std::unique_ptr<Response>> NetworkManager::send_request(
    const NodeAddress& target, const Request& request,
    std::chrono::milliseconds timeout) {
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.total_requests_sent++;
    }

    auto result = connection_pool_->send_request(target, request, timeout);

    if (!result) {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.failed_requests++;
    }

    return result;
}

void NetworkManager::close_connection(ConnectionId conn_id) {
    cleanup_connection(conn_id);
}

void NetworkManager::close_all_connections() {
    std::vector<std::thread> threads_to_join;

    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        threads_to_join.reserve(active_connections_.size());

        for (auto& [conn_id, thread] : active_connections_) {
            if (thread.joinable()) {
                threads_to_join.emplace_back(std::move(thread));
            }
        }

        active_connections_.clear();
    }

    const auto join_timeout = std::chrono::seconds(5);
    auto start_time = std::chrono::steady_clock::now();

    for (auto& thread : threads_to_join) {
        if (thread.joinable()) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed > join_timeout) {
                thread.detach();
            } else {
                try {
                    thread.join();
                } catch (const std::exception& e) {
                    // Silent error handling
                }
            }
        }
    }
}

size_t NetworkManager::get_active_connections_count() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return active_connections_.size();
}

NetworkManager::NetworkStats NetworkManager::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;
    stats.active_connections = get_active_connections_count();
    return stats;
}

void NetworkManager::accept_connections() {
    while (server_running_.load()) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_socket =
            accept(server_socket_, (struct sockaddr*)&client_addr, &client_len);

        if (client_socket < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::yield();
                continue;
            } else if (server_running_.load()) {
                std::cerr << "NetworkManager: Accept failed: "
                          << strerror(errno) << std::endl;
            }
            break;
        }

        auto opt_result = set_socket_options(client_socket);
        if (!opt_result) {
            close_socket(client_socket);
            continue;
        }

        ConnectionId conn_id = next_connection_id_.fetch_add(1);

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_connections_accepted++;
        }

        try {
            thread_pool_->enqueue([this, conn_id, client_socket]() {
                handle_client_connection(conn_id, client_socket);
            });
        } catch (const std::exception& e) {
            close_socket(client_socket);
        }
    }
}

void NetworkManager::handle_client_connection(ConnectionId conn_id,
                                              int client_socket) {
    set_socket_timeout(client_socket, std::chrono::milliseconds(15000));

    while (server_running_.load()) {
        try {
            auto receive_result = receive_message(client_socket);
            if (!receive_result) {
                break;
            }

            auto request_result =
                JsonSerializer::deserialize_request(receive_result.value());
            if (!request_result) {
                Response error_response =
                    Response::error("Invalid request format");
                auto serialized_response_result =
                    JsonSerializer::serialize_response(error_response);
                if (serialized_response_result) {
                    auto send_result = send_message(
                        client_socket, serialized_response_result.value());
                    if (!send_result) {
                        break;
                    }
                } else {
                    break;
                }
                continue;
            }

            // Handle request
            std::string serialized_response;
            if (message_handler_) {
                try {
                    auto response_ptr =
                        message_handler_(*request_result.value());

                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    stats_.total_requests_handled++;

                    auto serialized_response_result =
                        JsonSerializer::serialize_response(*response_ptr);
                    if (!serialized_response_result) {
                        break;
                    }
                    serialized_response = serialized_response_result.value();
                } catch (const std::exception& e) {
                    auto error_response =
                        std::make_unique<Response>(Response::error(
                            "Request handler error: " + std::string(e.what())));
                    auto serialized_response_result =
                        JsonSerializer::serialize_response(*error_response);
                    if (!serialized_response_result) {
                        break;
                    }
                    serialized_response = serialized_response_result.value();
                }
            } else {
                auto error_response = std::make_unique<Response>(
                    Response::error("No message handler configured"));
                auto serialized_response_result =
                    JsonSerializer::serialize_response(*error_response);
                if (!serialized_response_result) {
                    break;
                }
                serialized_response = serialized_response_result.value();
            }

            auto send_result = send_message(client_socket, serialized_response);
            if (!send_result) {
                break;
            }
        } catch (const std::exception& e) {
            break;
        }
    }

    close_socket(client_socket);
    cleanup_connection(conn_id);
}

Result<std::string> NetworkManager::receive_message(int socket) {
    uint32_t message_length = 0;
    ssize_t bytes_received =
        recv(socket, &message_length, sizeof(message_length), MSG_WAITALL);

    if (bytes_received <= 0) {
        if (bytes_received == 0) {
            return Result<std::string>::error("Connection closed by peer");
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return Result<std::string>::error("Receive timeout");
        }
        return Result<std::string>::error("Failed to receive message length: " +
                                          std::string(strerror(errno)));
    }

    if (bytes_received != sizeof(message_length)) {
        return Result<std::string>::error("Incomplete message length received");
    }

    message_length = ntohl(message_length);

    if (message_length > 1024 * 1024) {
        return Result<std::string>::error("Message too large: " +
                                          std::to_string(message_length));
    }

    if (message_length == 0) {
        return Result<std::string>::success("");
    }

    std::string message(message_length, '\0');
    bytes_received = recv(socket, &message[0], message_length, MSG_WAITALL);

    if (bytes_received <= 0) {
        if (bytes_received == 0) {
            return Result<std::string>::error(
                "Connection closed while reading message");
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return Result<std::string>::error("Receive timeout");
        }
        return Result<std::string>::error("Failed to receive message: " +
                                          std::string(strerror(errno)));
    }

    if (static_cast<uint32_t>(bytes_received) != message_length) {
        return Result<std::string>::error("Incomplete message received");
    }

    return Result<std::string>::success(message);
}

Result<void> NetworkManager::send_message(int socket,
                                          const std::string& message) {
    uint32_t message_length = htonl(static_cast<uint32_t>(message.length()));
    ssize_t bytes_sent =
        send(socket, &message_length, sizeof(message_length), MSG_NOSIGNAL);

    if (bytes_sent <= 0) {
        return Result<void>::error("Failed to send message length: " +
                                   std::string(strerror(errno)));
    }

    if (bytes_sent != sizeof(message_length)) {
        return Result<void>::error("Incomplete message length sent");
    }

    // Send the message
    if (!message.empty()) {
        bytes_sent =
            send(socket, message.c_str(), message.length(), MSG_NOSIGNAL);

        if (bytes_sent <= 0) {
            return Result<void>::error("Failed to send message: " +
                                       std::string(strerror(errno)));
        }

        if (static_cast<size_t>(bytes_sent) != message.length()) {
            return Result<void>::error("Incomplete message sent");
        }
    }

    return Result<void>::success();
}

void NetworkManager::cleanup_connection(ConnectionId conn_id) {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    auto it = active_connections_.find(conn_id);
    if (it != active_connections_.end()) {
        if (it->second.joinable()) {
            it->second.detach();
        }
        active_connections_.erase(it);
    }
}

Result<void> NetworkManager::set_socket_options(int socket) {
    int opt = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        return Result<void>::error("Failed to set SO_REUSEADDR: " +
                                   std::string(strerror(errno)));
    }

    return Result<void>::success();
}

Result<void> NetworkManager::set_socket_timeout(
    int socket, std::chrono::milliseconds timeout) {
    struct timeval tv;
    tv.tv_sec = timeout.count() / 1000;
    tv.tv_usec = (timeout.count() % 1000) * 1000;

    if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return Result<void>::error("Failed to set receive timeout: " +
                                   std::string(strerror(errno)));
    }

    if (setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        return Result<void>::error("Failed to set send timeout: " +
                                   std::string(strerror(errno)));
    }

    return Result<void>::success();
}

// Async message sending (fire and forget)
void NetworkManager::send_message_async(const NodeAddress& target,
                                        const std::string& message) {
    thread_pool_->enqueue([this, target, message]() {
        try {
            auto request_result = JsonSerializer::deserialize_request(message);
            if (!request_result) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.failed_requests++;
                return;
            }

            auto result =
                connection_pool_->send_request(target, *request_result.value(),
                                               std::chrono::milliseconds(5000));

            if (result) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.total_requests_sent++;
            } else {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.failed_requests++;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.failed_requests++;
        }
    });
}

void NetworkManager::close_socket(int socket) {
    if (socket < 0) {
        return;
    }

    shutdown(socket, SHUT_RDWR);
    close(socket);
}

void NetworkManager::configure_uds(bool enabled, const std::string& socket_dir) {
    uds_enabled_ = enabled;
    if (enabled && !socket_dir.empty()) {
        uds_path_ = socket_dir + "/node-" + std::to_string(server_port_) + ".sock";
    }
}

Result<void> NetworkManager::start_uds_server() {
    if (uds_path_.empty()) {
        uds_path_ = "/tmp/trelliskv/node-" + std::to_string(server_port_) + ".sock";
    }

    std::filesystem::path socket_dir = std::filesystem::path(uds_path_).parent_path();
    try {
        std::filesystem::create_directories(socket_dir);
    } catch (const std::exception& e) {
        return Result<void>::error("Failed to create UDS directory: " + 
                                   std::string(e.what()));
    }

    // Remove existing socket file if present
    ::unlink(uds_path_.c_str());

    uds_socket_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (uds_socket_ < 0) {
        return Result<void>::error("Failed to create UDS socket: " +
                                   std::string(strerror(errno)));
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (uds_path_.length() >= sizeof(addr.sun_path)) {
        close(uds_socket_);
        uds_socket_ = -1;
        return Result<void>::error("UDS path too long: " + uds_path_);
    }
    strncpy(addr.sun_path, uds_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(uds_socket_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(uds_socket_);
        uds_socket_ = -1;
        return Result<void>::error("Failed to bind UDS socket: " +
                                   std::string(strerror(errno)));
    }

    chmod(uds_path_.c_str(), 0660);

    const int backlog = 128;
    if (listen(uds_socket_, backlog) < 0) {
        close(uds_socket_);
        uds_socket_ = -1;
        ::unlink(uds_path_.c_str());
        return Result<void>::error("Failed to listen on UDS socket: " +
                                   std::string(strerror(errno)));
    }

    // Set non-blocking
    int flags = fcntl(uds_socket_, F_GETFL, 0);
    fcntl(uds_socket_, F_SETFL, flags | O_NONBLOCK);

    uds_accept_thread_ = std::thread(&NetworkManager::accept_uds_connections, this);

    return Result<void>::success();
}

void NetworkManager::accept_uds_connections() {
    while (server_running_.load()) {
        struct sockaddr_un client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_socket =
            accept(uds_socket_, (struct sockaddr*)&client_addr, &client_len);

        if (client_socket < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::yield();
                continue;
            } else if (server_running_.load()) {
                break;
            }
            break;
        }

        ConnectionId conn_id = next_connection_id_.fetch_add(1);

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_connections_accepted++;
        }

        try {
            thread_pool_->enqueue([this, conn_id, client_socket]() {
                handle_client_connection(conn_id, client_socket);
            });
        } catch (const std::exception& e) {
            close_socket(client_socket);
        }
    }
}

void NetworkManager::cleanup_uds_socket() {
    if (!uds_path_.empty()) {
        ::unlink(uds_path_.c_str());
        uds_path_.clear();
    }
}

}  // namespace trelliskv
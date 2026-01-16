#include "trelliskv/connection_pool.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>

#include "trelliskv/json_serializer.h"
#include "trelliskv/messages.h"
#include "trelliskv/node_info.h"

namespace trelliskv {

ConnectionPool::ConnectionPool(size_t max_connections_per_node)
    : max_connections_per_node_(max_connections_per_node) {}

ConnectionPool::~ConnectionPool() { close_all_connections(); }

void ConnectionPool::close_socket_fd(Connection* conn) {
    if (!conn) return;
    if (conn->socket_fd >= 0) {
        ::shutdown(conn->socket_fd, SHUT_RDWR);
        ::close(conn->socket_fd);
        conn->socket_fd = -1;
    }
}

Result<std::unique_ptr<Response>> ConnectionPool::send_request(
    const NodeAddress& target, const Request& request,
    std::chrono::milliseconds timeout) {
    active_requests_++;

    const int max_retries = 2;
    for (int retry = 0; retry < max_retries; ++retry) {
        auto conn_result = get_or_create_connection(target, timeout);
        if (!conn_result) {
            failed_connections_++;
            if (retry == max_retries - 1) {
                active_requests_--;
                return Result<std::unique_ptr<Response>>::error(
                    "Failed to get connection: " + conn_result.error());
            }
            continue;
        }

        Connection* conn = conn_result.value();
        bool connection_valid = true;

        auto serialized_request_result =
            JsonSerializer::serialize_request(request);
        if (!serialized_request_result) {
            failed_connections_++;
            active_requests_--;
            return_connection(target, conn, false);
            return Result<std::unique_ptr<Response>>::error(
                "Failed to serialize request: " +
                serialized_request_result.error());
        }
        std::string serialized_request = serialized_request_result.value();

        uint32_t message_length =
            htonl(static_cast<uint32_t>(serialized_request.length()));
        ssize_t bytes_sent = send(conn->socket_fd, &message_length,
                                  sizeof(message_length), MSG_NOSIGNAL);

        if (bytes_sent != sizeof(message_length)) {
            connection_valid = false;
            failed_connections_++;
            return_connection(target, conn, false);

            if (bytes_sent < 0 &&
                (errno == ENOTCONN || errno == EPIPE || errno == ECONNRESET)) {
                if (retry < max_retries - 1) {
                    continue;
                }
            }

            active_requests_--;
            std::string error_msg = "Failed to send message length";
            if (bytes_sent < 0) {
                error_msg += ": " + std::string(strerror(errno));
            }
            return Result<std::unique_ptr<Response>>::error(error_msg);
        }

        bytes_sent = send(conn->socket_fd, serialized_request.c_str(),
                          serialized_request.length(), MSG_NOSIGNAL);
        if (static_cast<size_t>(bytes_sent) != serialized_request.length()) {
            connection_valid = false;
            failed_connections_++;
            return_connection(target, conn, false);

            if (bytes_sent < 0 &&
                (errno == ENOTCONN || errno == EPIPE || errno == ECONNRESET)) {
                if (retry < max_retries - 1) {
                    continue;
                }
            }

            active_requests_--;
            std::string error_msg = "Failed to send request data";
            if (bytes_sent < 0) {
                error_msg += ": " + std::string(strerror(errno));
            }
            return Result<std::unique_ptr<Response>>::error(error_msg);
        }

        uint32_t response_length = 0;
        ssize_t bytes_received = recv(conn->socket_fd, &response_length,
                                      sizeof(response_length), MSG_WAITALL);

        if (bytes_received != sizeof(response_length)) {
            connection_valid = false;
            failed_connections_++;
            active_requests_--;
            return_connection(target, conn, false);
            std::string error_msg = "Failed to receive response length";
            if (bytes_received < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    error_msg += ": timeout";
                } else {
                    error_msg += ": " + std::string(strerror(errno));
                }
            } else if (bytes_received == 0) {
                error_msg += ": connection closed by peer";
            }
            return Result<std::unique_ptr<Response>>::error(error_msg);
        }

        response_length = ntohl(response_length);

        if (response_length > 10 * 1024 * 1024) {
            connection_valid = false;
            failed_connections_++;
            active_requests_--;
            return_connection(target, conn, false);
            return Result<std::unique_ptr<Response>>::error(
                "Response too large: " + std::to_string(response_length));
        }

        std::string response_data(response_length, '\0');
        bytes_received = recv(conn->socket_fd, &response_data[0],
                              response_length, MSG_WAITALL);

        if (static_cast<uint32_t>(bytes_received) != response_length) {
            connection_valid = false;
            failed_connections_++;
            active_requests_--;
            return_connection(target, conn, false);
            std::string error_msg = "Failed to receive complete response";
            if (bytes_received < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    error_msg += ": timeout";
                } else {
                    error_msg += ": " + std::string(strerror(errno));
                }
            } else if (bytes_received == 0) {
                error_msg += ": connection closed by peer";
            }
            return Result<std::unique_ptr<Response>>::error(error_msg);
        }

        return_connection(target, conn, connection_valid);
        active_requests_--;

        auto response_result =
            JsonSerializer::deserialize_response(response_data);
        if (!response_result) {
            return Result<std::unique_ptr<Response>>::error(
                "Failed to deserialize response: " + response_result.error());
        }

        return Result<std::unique_ptr<Response>>::success(
            std::move(response_result.value()));
    }

    active_requests_--;
    return Result<std::unique_ptr<Response>>::error(
        "All retry attempts failed");
}

void ConnectionPool::close_connections_to_node(const NodeAddress& target) {
    std::string key = address_to_key(target);

    std::unique_lock<std::mutex> lock(pool_mutex_);
    auto it = node_pools_.find(key);
    if (it == node_pools_.end()) {
        return;
    }

    auto node_pool = std::move(it->second);
    node_pools_.erase(it);
    lock.unlock();

    std::lock_guard<std::mutex> pool_lock(node_pool->mutex);
    for (auto& conn : node_pool->connections) {
        if (conn && conn->socket_fd >= 0) {
            close_socket_fd(conn.get());
            if (total_connections_ > 0) {
                total_connections_--;
            }
        }
    }
    node_pool->connections.clear();
}

void ConnectionPool::close_all_connections() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    auto pools = std::move(node_pools_);
    node_pools_.clear();
    lock.unlock();

    for (auto& [key, node_pool] : pools) {
        std::lock_guard<std::mutex> pool_lock(node_pool->mutex);
        for (auto& conn : node_pool->connections) {
            if (conn && conn->socket_fd >= 0) {
                close_socket_fd(conn.get());
            }
        }
        node_pool->connections.clear();
    }

    total_connections_ = 0;
    failed_connections_ = 0;
}

ConnectionPool::PoolStats ConnectionPool::get_stats() const {
    PoolStats result;
    result.total_connections = total_connections_.load();
    result.active_requests = active_requests_.load();
    result.failed_connections = failed_connections_.load();
    return result;
}

std::string ConnectionPool::address_to_key(const NodeAddress& address) const {
    return address.hostname + ":" + std::to_string(address.port);
}

ConnectionPool::NodeConnectionPool* ConnectionPool::get_node_pool(
    const std::string& key) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    auto it = node_pools_.find(key);
    if (it == node_pools_.end()) {
        auto node_pool = std::make_unique<NodeConnectionPool>();
        auto* pool_ptr = node_pool.get();
        node_pools_[key] = std::move(node_pool);
        return pool_ptr;
    }

    return it->second.get();
}

Result<ConnectionPool::Connection*> ConnectionPool::get_or_create_connection(
    const NodeAddress& target, std::chrono::milliseconds timeout) {
    std::string key = address_to_key(target);
    NodeConnectionPool* node_pool = get_node_pool(key);

    std::unique_lock<std::mutex> lock(node_pool->mutex);

    auto now = std::chrono::system_clock::now();
    const auto stale_timeout = std::chrono::seconds(30);

    node_pool->connections.erase(
        std::remove_if(node_pool->connections.begin(),
                       node_pool->connections.end(),
                       [now, stale_timeout,
                        this](const std::unique_ptr<Connection>& conn) {
                           if (!conn || conn->socket_fd < 0) {
                               return true;
                           }

                           if (!conn->in_use.load()) {
                               auto age = now - conn->last_used;
                               if (age > stale_timeout) {
                                   close_socket_fd(conn.get());
                                   if (total_connections_ > 0) {
                                       total_connections_--;
                                   }
                                   return true;
                               }
                           }
                           return false;
                       }),
        node_pool->connections.end());

    for (auto& conn : node_pool->connections) {
        if (conn && conn->socket_fd >= 0 && !conn->in_use.load()) {
            if (!is_connection_valid(conn.get())) {
                close_socket_fd(conn.get());
                if (total_connections_ > 0) {
                    total_connections_--;
                }
                continue;
            }
            conn->in_use.store(true);
            conn->last_used = std::chrono::system_clock::now();
            node_pool->active_count++;
            return Result<Connection*>::success(conn.get());
        }
    }

    if (node_pool->connections.size() < max_connections_per_node_) {
        lock.unlock();

        auto new_conn_result = create_new_connection(target);
        if (!new_conn_result) {
            return Result<Connection*>::error(new_conn_result.error());
        }

        auto new_conn = std::move(new_conn_result.value());
        new_conn->in_use.store(true);
        Connection* conn_ptr = new_conn.get();

        lock.lock();
        node_pool->connections.push_back(std::move(new_conn));
        node_pool->active_count++;
        total_connections_++;

        return Result<Connection*>::success(conn_ptr);
    }

    auto wait_until = std::chrono::steady_clock::now() + timeout;
    while (true) {
        if (node_pool->cv.wait_until(lock, wait_until) ==
            std::cv_status::timeout) {
            return Result<Connection*>::error(
                "Connection pool timeout: no available connections");
        }

        for (auto& conn : node_pool->connections) {
            if (conn && conn->socket_fd >= 0 && !conn->in_use.load()) {
                if (!is_connection_valid(conn.get())) {
                    close_socket_fd(conn.get());
                    if (total_connections_ > 0) {
                        total_connections_--;
                    }
                    continue;
                }
                conn->in_use.store(true);
                conn->last_used = std::chrono::system_clock::now();
                node_pool->active_count++;
                return Result<Connection*>::success(conn.get());
            }
        }
    }
}

void ConnectionPool::return_connection(const NodeAddress& target,
                                       Connection* conn, bool valid) {
    if (!conn) {
        return;
    }

    std::string key = address_to_key(target);
    NodeConnectionPool* node_pool = get_node_pool(key);

    std::lock_guard<std::mutex> lock(node_pool->mutex);

    if (valid && conn->socket_fd >= 0) {
        conn->in_use.store(false);
        conn->last_used = std::chrono::system_clock::now();
    } else {
        if (conn->socket_fd >= 0) {
            close_socket_fd(conn);
            if (total_connections_ > 0) {
                total_connections_--;
            }
        }
        conn->in_use.store(false);
    }

    if (node_pool->active_count > 0) {
        node_pool->active_count--;
    }

    node_pool->cv.notify_one();
}

bool ConnectionPool::is_connection_valid(Connection* conn) {
    if (!conn || conn->socket_fd < 0) {
        return false;
    }

    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(conn->socket_fd, SOL_SOCKET, SO_ERROR, &error, &len) != 0 ||
        error != 0) {
        return false;
    }

    struct pollfd pfd;
    pfd.fd = conn->socket_fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    int ret = poll(&pfd, 1, 0);

    if (ret < 0) {
        return false;
    }

    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return false;
    }

    return true;
}

Result<std::unique_ptr<ConnectionPool::Connection>>
ConnectionPool::create_new_connection(const NodeAddress& target) {
    if (uds_enabled_ && is_local_node(target)) {
        auto uds_result = create_uds_connection(target);
        if (uds_result) {
            return uds_result;
        }
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return Result<std::unique_ptr<Connection>>::error(
            "Failed to create socket: " + std::string(strerror(errno)));
    }

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));

    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct addrinfo hints{}, *result;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int status =
        getaddrinfo(target.hostname.c_str(),
                    std::to_string(target.port).c_str(), &hints, &result);
    if (status != 0) {
        close(sock);
        return Result<std::unique_ptr<Connection>>::error(
            "Failed to resolve hostname " + target.hostname + ": " +
            std::string(gai_strerror(status)));
    }

    int connect_result = connect(sock, result->ai_addr, result->ai_addrlen);
    int connect_errno = errno;
    freeaddrinfo(result);

    if (connect_result < 0) {
        close(sock);
        return Result<std::unique_ptr<Connection>>::error(
            "Failed to connect to " + target.to_string() + ": " +
            std::string(strerror(connect_errno)));
    }

    return Result<std::unique_ptr<Connection>>::success(
        std::make_unique<Connection>(sock, false));
}

void ConnectionPool::configure_uds(bool enabled, const std::string& local_hostname,
                                   uint16_t local_port, const std::string& uds_socket_dir) {
    uds_enabled_ = enabled;
    local_hostname_ = local_hostname;
    local_port_ = local_port;
    uds_socket_dir_ = uds_socket_dir;
}

bool ConnectionPool::is_local_node(const NodeAddress& target) const {
    // Check for common localhost indicators
    if (target.hostname == "localhost" ||
        target.hostname == "127.0.0.1" ||
        target.hostname == "::1") {
        return true;
    }
    // Check if target matches our own hostname
    if (target.hostname == local_hostname_) {
        return true;
    }
    return false;
}

std::string ConnectionPool::get_uds_path(const NodeAddress& target) const {
    return uds_socket_dir_ + "/node-" + std::to_string(target.port) + ".sock";
}

Result<std::unique_ptr<ConnectionPool::Connection>>
ConnectionPool::create_uds_connection(const NodeAddress& target) {
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        return Result<std::unique_ptr<Connection>>::error(
            "Failed to create UDS socket: " + std::string(strerror(errno)));
    }

    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::string socket_path = get_uds_path(target);
    
    if (socket_path.length() >= sizeof(addr.sun_path)) {
        close(sock);
        return Result<std::unique_ptr<Connection>>::error(
            "UDS path too long: " + socket_path);
    }
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return Result<std::unique_ptr<Connection>>::error(
            "Failed to connect to UDS " + socket_path + ": " +
            std::string(strerror(errno)));
    }

    return Result<std::unique_ptr<Connection>>::success(
        std::make_unique<Connection>(sock, true));
}

}  // namespace trelliskv
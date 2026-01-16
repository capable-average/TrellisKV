#include "trelliskv/tcp_client.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>
#include <random>
#include <sstream>

#include "trelliskv/json_serializer.h"
#include "trelliskv/messages.h"

namespace trelliskv {

TcpClient::TcpClient() : socket_fd_(-1), connected_(false) {}

TcpClient::~TcpClient() { disconnect(); }

Result<void> TcpClient::connect(const NodeAddress& server_address,
                                std::chrono::milliseconds timeout) {
    if (connected_) {
        disconnect();
    }

    server_address_ = server_address;

    if (prefer_uds_ && is_local_address(server_address.hostname)) {
        auto uds_result = try_uds_connect(server_address, timeout);
        if (uds_result.is_success()) {
            is_uds_connection_ = true;
            connected_ = true;
            return Result<void>::success();
        }
    }

    auto socket_result = create_socket();
    if (!socket_result.is_success()) {
        return socket_result;
    }

    auto timeout_result = set_socket_timeout(timeout);
    if (!timeout_result.is_success()) {
        close_socket();
        return timeout_result;
    }

    struct addrinfo hints{}, *result;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int status = getaddrinfo(server_address.hostname.c_str(),
                             std::to_string(server_address.port).c_str(),
                             &hints, &result);
    if (status != 0) {
        close_socket();
        return Result<void>::error("Failed to resolve hostname: " +
                                   std::string(gai_strerror(status)));
    }

    int connect_result =
        ::connect(socket_fd_, result->ai_addr, result->ai_addrlen);
    freeaddrinfo(result);

    if (connect_result < 0) {
        close_socket();
        return Result<void>::error("Failed to connect to server: " +
                                   std::string(strerror(errno)));
    }

    is_uds_connection_ = false;
    connected_ = true;
    return Result<void>::success();
}

void TcpClient::disconnect() {
    if (connected_) {
        close_socket();
        connected_ = false;
        is_uds_connection_ = false;
    }
}

bool TcpClient::is_connected() const { return connected_; }

Result<Response> TcpClient::send_get_request(const std::string& key,
                                             ConsistencyLevel consistency) {
    GetRequest request(key, consistency);
    request.request_id = generate_request_id();
    request.sender_id = "cli";
    return send_request(request);
}

Result<Response> TcpClient::send_put_request(const std::string& key,
                                             const std::string& value,
                                             ConsistencyLevel consistency) {
    PutRequest request(key, value, consistency);
    request.request_id = generate_request_id();
    request.sender_id = "cli";
    return send_request(request);
}

Result<Response> TcpClient::send_delete_request(const std::string& key,
                                                ConsistencyLevel consistency) {
    DeleteRequest request(key, consistency);
    request.request_id = generate_request_id();
    request.sender_id = "cli";
    return send_request(request);
}

Result<std::unique_ptr<Response>> TcpClient::send_batch_put_request(
    const std::vector<std::pair<std::string, std::string>>& items,
    ConsistencyLevel consistency) {
    if (!connected_) {
        return Result<std::unique_ptr<Response>>::error(
            "Not connected to server");
    }

    BatchPutRequest request;
    request.request_id = generate_request_id();
    request.sender_id = "cli";
    request.consistency = consistency;

    for (const auto& [key, value] : items) {
        request.items.push_back({key, value});
    }

    // Serialize request
    auto serialized_request_result = JsonSerializer::serialize_request(request);
    if (!serialized_request_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to serialize request: " +
            serialized_request_result.error());
    }
    std::string serialized_request = serialized_request_result.value();

    // Send request
    auto send_result = send_message(serialized_request);
    if (!send_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to send request: " + send_result.error());
    }

    // Receive response
    auto receive_result = receive_message();
    if (!receive_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to receive response: " + receive_result.error());
    }

    // Deserialize response
    auto response_result =
        JsonSerializer::deserialize_response(receive_result.value());
    if (!response_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to deserialize response: " + response_result.error());
    }

    return Result<std::unique_ptr<Response>>::success(
        std::move(response_result.value()));
}

Result<std::unique_ptr<Response>> TcpClient::send_batch_get_request(
    const std::vector<std::string>& keys, ConsistencyLevel consistency) {
    if (!connected_) {
        return Result<std::unique_ptr<Response>>::error(
            "Not connected to server");
    }

    BatchGetRequest request;
    request.request_id = generate_request_id();
    request.sender_id = "cli";
    request.keys = keys;
    request.consistency = consistency;

    // Serialize request
    auto serialized_request_result = JsonSerializer::serialize_request(request);
    if (!serialized_request_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to serialize request: " +
            serialized_request_result.error());
    }
    std::string serialized_request = serialized_request_result.value();

    // Send request
    auto send_result = send_message(serialized_request);
    if (!send_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to send request: " + send_result.error());
    }

    // Receive response
    auto receive_result = receive_message();
    if (!receive_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to receive response: " + receive_result.error());
    }

    // Deserialize response
    auto response_result =
        JsonSerializer::deserialize_response(receive_result.value());
    if (!response_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to deserialize response: " + response_result.error());
    }

    return Result<std::unique_ptr<Response>>::success(
        std::move(response_result.value()));
}

Result<std::unique_ptr<Response>> TcpClient::send_health_check_request(
    bool include_details) {
    if (!connected_) {
        return Result<std::unique_ptr<Response>>::error(
            "Not connected to server");
    }

    HealthCheckRequest request(include_details);
    request.request_id = generate_request_id();
    request.sender_id = "cli";

    auto serialized_request_result = JsonSerializer::serialize_request(request);
    if (!serialized_request_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to serialize request: " +
            serialized_request_result.error());
    }
    std::string serialized_request = serialized_request_result.value();

    auto send_result = send_message(serialized_request);
    if (!send_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to send request: " + send_result.error());
    }

    auto receive_result = receive_message();
    if (!receive_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to receive response: " + receive_result.error());
    }

    auto response_result =
        JsonSerializer::deserialize_response(receive_result.value());
    if (!response_result.is_success()) {
        return Result<std::unique_ptr<Response>>::error(
            "Failed to deserialize response: " + response_result.error());
    }

    return Result<std::unique_ptr<Response>>::success(
        std::move(response_result.value()));
}

Result<Response> TcpClient::send_request(const Request& request) {
    if (!connected_) {
        return Result<Response>::error("Not connected to server");
    }

    // Serialize request
    auto serialized_request_result = JsonSerializer::serialize_request(request);
    if (!serialized_request_result.is_success()) {
        return Result<Response>::error("Failed to serialize request: " +
                                       serialized_request_result.error());
    }
    std::string serialized_request = serialized_request_result.value();

    // Send request
    auto send_result = send_message(serialized_request);
    if (!send_result.is_success()) {
        return Result<Response>::error("Failed to send request: " +
                                       send_result.error());
    }

    // Receive response
    auto receive_result = receive_message();
    if (!receive_result.is_success()) {
        return Result<Response>::error("Failed to receive response: " +
                                       receive_result.error());
    }

    // Deserialize response
    auto response_result =
        JsonSerializer::deserialize_response(receive_result.value());
    if (!response_result.is_success()) {
        return Result<Response>::error("Failed to deserialize response: " +
                                       response_result.error());
    }

    return Result<Response>::success(*response_result.value());
}

Result<void> TcpClient::create_socket() {
    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        return Result<void>::error("Failed to create socket: " +
                                   std::string(strerror(errno)));
    }

    // Disable Nagle's algorithm for lower latency
    int opt = 1;
    if (setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        close_socket();
        return Result<void>::error("Failed to set TCP_NODELAY: " +
                                   std::string(strerror(errno)));
    }

    return Result<void>::success();
}

Result<std::string> TcpClient::receive_message() {
    uint32_t message_length = 0;
    ssize_t bytes_received =
        recv(socket_fd_, &message_length, sizeof(message_length), MSG_WAITALL);
    if (bytes_received != sizeof(message_length)) {
        if (bytes_received == 0) {
            return Result<std::string>::error("Connection closed by server");
        } else if (bytes_received < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return Result<std::string>::error("Receive timeout");
            }
            return Result<std::string>::error(
                "Failed to receive message length: " +
                std::string(strerror(errno)));
        } else {
            return Result<std::string>::error(
                "Incomplete message length received");
        }
    }

    message_length = ntohl(message_length);

    if (message_length == 0 || message_length > 1024 * 1024) {
        return Result<std::string>::error("Invalid message length: " +
                                          std::to_string(message_length));
    }

    std::string message(message_length, '\0');
    bytes_received = recv(socket_fd_, &message[0], message_length, MSG_WAITALL);
    if (bytes_received != static_cast<ssize_t>(message_length)) {
        if (bytes_received == 0) {
            return Result<std::string>::error("Connection closed by server");
        } else if (bytes_received < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return Result<std::string>::error("Receive timeout");
            }
            return Result<std::string>::error("Failed to receive message: " +
                                              std::string(strerror(errno)));
        } else {
            return Result<std::string>::error("Incomplete message received");
        }
    }

    return Result<std::string>::success(message);
}

Result<void> TcpClient::send_message(const std::string& message) {
    uint32_t message_length = htonl(static_cast<uint32_t>(message.length()));
    ssize_t bytes_sent =
        send(socket_fd_, &message_length, sizeof(message_length), 0);
    if (bytes_sent != sizeof(message_length)) {
        return Result<void>::error("Failed to send message length: " +
                                   std::string(strerror(errno)));
    }

    bytes_sent = send(socket_fd_, message.c_str(), message.length(), 0);
    if (bytes_sent != static_cast<ssize_t>(message.length())) {
        return Result<void>::error("Failed to send message: " +
                                   std::string(strerror(errno)));
    }

    return Result<void>::success();
}

Result<void> TcpClient::set_socket_timeout(std::chrono::milliseconds timeout) {
    struct timeval tv;
    tv.tv_sec = timeout.count() / 1000;
    tv.tv_usec = (timeout.count() % 1000) * 1000;

    if (setsockopt(socket_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        return Result<void>::error("Failed to set send timeout: " +
                                   std::string(strerror(errno)));
    }

    if (setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        return Result<void>::error("Failed to set receive timeout: " +
                                   std::string(strerror(errno)));
    }

    return Result<void>::success();
}

void TcpClient::close_socket() {
    if (socket_fd_ < 0) {
        return;
    }

    ::shutdown(socket_fd_, SHUT_WR);

    char buffer[4096];
    ssize_t bytes_read;
    int drain_attempts = 0;
    const int max_drain_attempts = 10;

    int flags = fcntl(socket_fd_, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK);
    }

    while (drain_attempts++ < max_drain_attempts) {
        bytes_read = ::recv(socket_fd_, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            break;
        }
    }

    ::shutdown(socket_fd_, SHUT_RD);
    ::close(socket_fd_);
    socket_fd_ = -1;
}

std::string TcpClient::generate_request_id() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t id = dis(gen);
    std::stringstream ss;
    ss << "req_" << std::hex << id;
    return ss.str();
}

void TcpClient::set_prefer_uds(bool prefer, const std::string& uds_dir) {
    prefer_uds_ = prefer;
    uds_socket_dir_ = uds_dir;
}

bool TcpClient::is_local_address(const std::string& hostname) const {
    return hostname == "localhost" ||
           hostname == "127.0.0.1" ||
           hostname == "::1";
}

std::string TcpClient::get_uds_path(uint16_t port) const {
    return uds_socket_dir_ + "/node-" + std::to_string(port) + ".sock";
}

Result<void> TcpClient::try_uds_connect(const NodeAddress& server_address,
                                        std::chrono::milliseconds timeout) {
    std::string socket_path = get_uds_path(server_address.port);

    socket_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        return Result<void>::error("Failed to create UDS socket: " +
                                   std::string(strerror(errno)));
    }

    auto timeout_result = set_socket_timeout(timeout);
    if (!timeout_result.is_success()) {
        close_socket();
        return timeout_result;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (socket_path.length() >= sizeof(addr.sun_path)) {
        close_socket();
        return Result<void>::error("UDS path too long: " + socket_path);
    }
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (::connect(socket_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close_socket();
        return Result<void>::error("Failed to connect to UDS " + socket_path +
                                   ": " + std::string(strerror(errno)));
    }

    return Result<void>::success();
}

}  // namespace trelliskv
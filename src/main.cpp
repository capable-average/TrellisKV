#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

#include "trelliskv/logger.h"
#include "trelliskv/node_config.h"
#include "trelliskv/trellis_node.h"

void print_usage(const char* program_name) {
    std::cout << "TrellisKV - Distributed Key-Value Store" << std::endl;
    std::cout << "Version 1.0.0" << std::endl;
    std::cout << "Usage: " << program_name
              << " <port> [seed_host:seed_port] [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --replication-factor N  Set replication factor (default: 3)"
              << std::endl;
    std::cout << "  --hostname HOST         Set hostname for cluster "
                 "communication (default: localhost)"
              << std::endl;
    std::cout << "  --heartbeat-interval MS Set heartbeat interval in ms "
                 "(default: 1000)"
              << std::endl;
    std::cout << "  --failure-timeout MS    Set failure timeout in ms "
                 "(default: 5000)"
              << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " 5000" << std::endl;
    std::cout << "  " << program_name << " 5001 localhost:5000" << std::endl;
    std::cout << "  " << program_name
              << " 5002 localhost:5000 --replication-factor 2" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2 || (argc >= 2 && (std::string(argv[1]) == "--help" ||
                                   std::string(argv[1]) == "-h"))) {
        print_usage(argv[0]);
        return (argc < 2) ? 1 : 0;
    }

    try {
        uint16_t port = static_cast<uint16_t>(std::stoi(argv[1]));
        std::string hostname = "localhost";

        std::string node_id = "node_" + std::to_string(port);

        auto config =
            trelliskv::TrellisNode::create_default_config(hostname, port);

        int arg_index = 2;
        if (argc >= 3 && argv[2][0] != '-') {
            std::string seed_arg = argv[2];
            size_t colon_pos = seed_arg.find(':');
            if (colon_pos != std::string::npos) {
                std::string seed_host = seed_arg.substr(0, colon_pos);
                uint16_t seed_port = static_cast<uint16_t>(
                    std::stoi(seed_arg.substr(colon_pos + 1)));
                config.seed_nodes.push_back(
                    trelliskv::NodeAddress(seed_host, seed_port));
            }
            arg_index = 3;
        }

        for (int i = arg_index; i < argc; i++) {
            std::string arg = argv[i];

            if (arg == "--replication-factor" && i + 1 < argc) {
                int rf = std::stoi(argv[++i]);
                if (rf < 1) {
                    std::cerr << "Error: Replication factor must be >= 1"
                              << std::endl;
                    return 1;
                }
                config.replication_factor = static_cast<size_t>(rf);
            } else if (arg == "--hostname" && i + 1 < argc) {
                hostname = argv[++i];
                config.address = trelliskv::NodeAddress(hostname, port);
            } else if (arg == "--heartbeat-interval" && i + 1 < argc) {
                config.heartbeat_interval =
                    std::chrono::milliseconds(std::stoi(argv[++i]));
            } else if (arg == "--failure-timeout" && i + 1 < argc) {
                config.failure_timeout =
                    std::chrono::milliseconds(std::stoi(argv[++i]));
            } else {
                std::cerr << "Unknown option: " << arg << std::endl;
                return 1;
            }
        }

        trelliskv::Logger& logger = trelliskv::Logger::instance();
        logger.set_level(trelliskv::LogLevel::INFO);

        trelliskv::TrellisNode node(node_id, config);

        auto start_result = node.start();
        if (!start_result) {
            std::cerr << "Failed to start node: " << start_result.error()
                      << std::endl;
            return 1;
        }
        if (!config.seed_nodes.empty()) {
            auto join_result = node.join_cluster();
            if (!join_result) {
                std::cerr << "Failed to join cluster: " << join_result.error()
                          << std::endl;
                return 1;
            }
        }

        std::cout << "TrellisKV node is running. Press Ctrl+C to stop."
                  << std::endl;

        while (node.is_running()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
#pragma once

#include <chrono>
#include <optional>
#include <string>

#include "types.h"

namespace trelliskv {

struct VersionedValue {
    std::string value;
    TimestampVersion version;
    NodeId last_writer;
    Timestamp timestamp;
    std::optional<int64_t> ttl_ms;  // TTL in milliseconds, nullopt = no expiry

    VersionedValue()
        : value(""),
          version(),
          last_writer(""),
          timestamp(std::chrono::system_clock::now()) {}

    VersionedValue(const std::string& val, const TimestampVersion& ver,
                   const NodeId& writer)
        : value(val),
          version(ver),
          last_writer(writer),
          timestamp(std::chrono::system_clock::now()) {}

    VersionedValue(const std::string& val, const NodeId& writer)
        : value(val),
          version(TimestampVersion::now(writer)),
          last_writer(writer),
          timestamp(std::chrono::system_clock::now()) {}

    // for conflict detection
    bool is_newer_than(const VersionedValue& other) const {
        return version.is_newer_than(other.version);
    }

    bool is_older_than(const VersionedValue& other) const {
        return version.is_older_than(other.version);
    }

    bool has_same_version(const VersionedValue& other) const {
        return version.equals(other.version);
    }

    // Update version for new write
    void update_version(const NodeId& writer) {
        last_writer = writer;
        version = TimestampVersion::now(writer);
        timestamp = std::chrono::system_clock::now();
    }

    std::string to_string() const {
        return "VersionedValue{value=" + value +
               ", version=TimestampVersion{ts=" +
               std::to_string(version.timestamp) +
               ", writer=" + version.last_writer + "}" +
               ", writer=" + last_writer + "}";
    }
};

}  // namespace trelliskv
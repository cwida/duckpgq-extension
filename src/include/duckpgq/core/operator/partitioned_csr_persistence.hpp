#pragma once

#include "duckpgq/core/utils/compressed_sparse_row.hpp"

namespace duckdb {

class ClientContext;

static constexpr uint32_t PARTITIONED_CSR_FORMAT_VERSION = 1;

//! Creates the versioned persistence tables when they do not exist yet.
void InitializePartitionedCSRPersistence(ClientContext &context);

//! Atomically writes a complete forward-CSR generation and makes it active.
//! The supplied index is validated before any durable state is published.
void PersistPartitionedCSR(ClientContext &context, const string &logical_key, const PartitionedCSRIndex &index);

//! Loads and validates the active generation. Invalid, incomplete, incompatible,
//! or count-mismatched artifacts are treated as a cache miss and are never published.
shared_ptr<PartitionedCSRIndex> TryLoadPersistedPartitionedCSR(ClientContext &context, const string &logical_key,
                                                               idx_t expected_vertex_count, idx_t expected_edge_count,
                                                               PartitionedCSRCapabilities required_capabilities);

} // namespace duckdb

#pragma once

#include "duckpgq/core/utils/compressed_sparse_row.hpp"

namespace duckdb {

class ClientContext;
struct CreatePropertyGraphInfo;

static constexpr uint32_t PARTITIONED_CSR_FORMAT_VERSION = 1;

//! Creates the versioned persistence tables when they do not exist yet.
void InitializePartitionedCSRPersistence(ClientContext &context);

//! Atomically writes a complete forward-CSR generation and makes it active.
//! The supplied index is validated before any durable state is published.
uint64_t PersistPartitionedCSR(ClientContext &context, const string &logical_key, const string &base_cache_key,
                               uint64_t expected_generation, const PartitionedCSRIndex &index);

//! Returns the committed registry generation, or zero when the logical artifact does not exist.
uint64_t GetPartitionedCSRGeneration(ClientContext &context, const string &logical_key);

//! Loads and validates the active generation. Invalid, incomplete, incompatible,
//! or count-mismatched artifacts are treated as a cache miss and are never published.
shared_ptr<PartitionedCSRIndex> TryLoadPersistedPartitionedCSR(ClientContext &context, const string &logical_key,
                                                               idx_t expected_vertex_count, idx_t expected_edge_count,
                                                               PartitionedCSRCapabilities required_capabilities);

//! Returns true only when the in-memory index still matches the committed active generation.
bool IsPersistedPartitionedCSRCurrent(ClientContext &context, const string &logical_key,
                                      const PartitionedCSRIndex &index);

//! Transaction-local invalidation is set by owned table triggers. It remains set until commit or rollback.
void MarkPartitionedCSRInvalidated(ClientContext &context, const string &logical_key);
bool IsPartitionedCSRInvalidated(ClientContext &context, const string &logical_key);
bool IsExplicitPartitionedCSRTransaction(ClientContext &context);

//! Removes every persisted generation, dependency and owned trigger belonging exclusively to a property graph.
void DropPartitionedCSRArtifacts(ClientContext &context, const CreatePropertyGraphInfo &property_graph);

} // namespace duckdb

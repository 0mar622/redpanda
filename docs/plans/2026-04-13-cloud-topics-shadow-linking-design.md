# Cloud Topics Shadow Linking

## Summary

Add a per-link storage mode override to shadow linking so that all shadow
topics created by a link use cloud storage, regardless of the source topic's
storage mode. This gives low-TCO disaster recovery: the shadow cluster stores
only placeholders locally and keeps data in object storage via the existing
cloud topics L0/L1 pipeline.

## Motivation

Today, shadow topics inherit the source topic's `redpanda.storage.mode`. A
shadow of a `local` topic is `local`; a shadow of a `cloud` topic is `cloud`.
For DR use cases where the shadow cluster only needs to hold a copy of the
data (not serve low-latency reads), forcing all shadows to cloud storage mode
dramatically reduces the disk and compute footprint of the secondary cluster.

## API Surface

### Proto changes (`shadow_link.proto`)

New enum:

```protobuf
enum ShadowTopicStorageMode {
    SHADOW_TOPIC_STORAGE_MODE_UNSPECIFIED = 0; // inherit source storage mode
    SHADOW_TOPIC_STORAGE_MODE_CLOUD = 1;
    SHADOW_TOPIC_STORAGE_MODE_TIERED_CLOUD = 2;
}
```

New field on `TopicMetadataSyncOptions`:

```protobuf
ShadowTopicStorageMode shadow_topic_storage_mode = 10;
```

### Semantics

- Defaults to `UNSPECIFIED` (existing behavior, inherit from source).
- Immutable after link creation. `UpdateShadowLink` rejects changes to this
  field.
- When set, all shadow topics created by the link use the specified storage
  mode, overriding whatever the source reports.

### Validation at link creation

If set to `CLOUD` or `TIERED_CLOUD`:

- `cloud_topics_enabled` must not be restricted (enterprise license required).
- Object storage must be configured on the shadow cluster.
- Reject link creation with a clear error if either precondition fails.

### Internal model

`topic_metadata_mirroring_config` gets a new field:

```cpp
std::optional<model::redpanda_storage_mode> storage_mode_override;
```

Populated from the proto enum at link creation. Serialized in the controller
log as part of `link_configuration`.

## Control Plane: Override in source_topic_syncer

### At topic creation (config collection)

`source_topic_syncer` already applies per-link overrides to topic configs in
`validate_and_get_configs_from_response()` (the `topic_configuration_overrides`
map sets `remote_allow_gaps=true` for all shadow topics).

When the link has a `storage_mode_override`:

1. After collecting configs from `DescribeConfigs`, inject
   `redpanda.storage.mode` = the override value into the config map (same
   pattern as the `remote_allow_gaps` override).
2. Remove `redpanda.storage.mode` from the set of properties eligible for
   ongoing sync so the reconciler never overwrites the override with the
   source's value.

### During config update sync

In `enqueue_update_mirror_topic_commands()`, when the link has a storage mode
override, skip `redpanda.storage.mode` when comparing source configs to local
configs. The reconciler must not "fix" the storage mode back to the source's
value.

### What stays unchanged

- `topic_reconciler::maybe_create_mirror_topic` -- creates topics with
  whatever configs it receives. If the config map says `storage_mode=cloud`,
  the topic is created as a cloud topic.
- The data path -- `ct_exact_offset_replicator` in
  `cloud_topic_partition.cc:183-221` already handles cloud topic shadow
  replication via `frontend::replicate_at_offset`.
- All other synced properties (retention, cleanup policy, compression, etc.)
  flow through unchanged.

## Data Flow

```
Source Cluster                    Shadow Cluster

  topic "foo"
  storage_mode=local
       |
       |  source_topic_syncer
       |  (periodic ~30s)
       |
       v
  DescribeConfigs --------------> Collect configs
                                  |
                                  | Override: storage_mode=cloud
                                  | (+ remote_allow_gaps=true)
                                  |
                                  v
                                  add_mirror_topic_cmd
                                  |
                                  v
                            topic_reconciler
                            creates topic "foo"
                            with storage_mode=cloud
                                  |
                                  v
                            cloud topic partition
                            (ctp_stm, L0/L1 pipeline)

  --- data replication ---

  Kafka Fetch API <------------- mux_remote_consumer
       |                              |
       | batches                      |
       v                              v
                            local_partition_sink
                                  |
                                  | (partition is cloud topic)
                                  v
                            ct_exact_offset_replicator
                                  |
                                  +-> frontend::replicate_at_offset
                                  |     |
                                  |     +-> upload to object storage (L0)
                                  |     +-> generate placeholders
                                  |     +-> replicate placeholders via ctp_stm
                                  |
                                  v
                            Consumers read via L0 materialization
```

Data still flows via the Kafka Fetch API from source, same as regular shadow
linking. The difference is only in the sink: instead of replicating raw batches
via Raft, it goes through the cloud topics write pipeline.

## Failover: Automatic Promotion to tiered_cloud

When a shadow topic created with `storage_mode_override=cloud` completes
failover (`failing_over → failed_over`), the `link_status_reconciler`
automatically issues an `AlterConfig` to change `storage_mode` from `cloud`
to `tiered_cloud`. This gives the now-primary cluster low-latency local
reads/writes instead of the pure-cloud path used during shadow mode.

The rule is implicit: **cloud shadows always become tiered_cloud on failover.**
No additional configuration is needed. If the link was not created with a
storage mode override, or the override was `tiered_cloud`, no action is taken.

The promotion is safe because the partition runtime supports live storage mode
transitions — new writes go through the local Raft + tiered upload path while
existing data remains accessible via L0/L1.

## Validation and Error Handling

**At link creation:** Preconditions checked (license, object storage). Clear
error on failure.

**At topic creation:** The existing topic creation path in `topics_frontend.cc`
already validates cloud topic preconditions (enterprise feature gate). If
something goes wrong, `topic_reconciler` logs the failure and retries on the
next reconciliation tick (~30s).

**Edge cases:**

- Source topic uses compaction: cloud topics support compaction via L1.
- Source topic has transactions: cloud topics support aborted transaction
  tracking via `ctp_stm`.
- License expires after link creation: topic creation starts failing at the
  enterprise feature gate. Existing shadow topics continue operating.

## Scope

### In scope

- `ShadowTopicStorageMode` enum and field in proto
- Internal model update to `topic_metadata_mirroring_config`
- Immutability enforcement on `UpdateShadowLink`
- Validation at link creation
- Override injection in `source_topic_syncer` (creation + sync suppression)
- Automatic `cloud → tiered_cloud` promotion on failover
- Unit tests for override logic
- Integration tests: cloud shadow creation, data replication, failover promotion

### Not in scope (future work)

- **Per-topic config overlays:** If more per-link property overrides are
  needed, consider introducing a `config_overrides` field on
  `mirror_topic_metadata` that takes precedence over synced `topic_configs`
  during creation and updates.
- **Cloud-specific retention derivation:** Deriving cloud topic retention
  settings from traditional `retention.bytes`/`retention.ms` values.
- **Retroactive migration:** Changing storage mode on existing shadow topics
  created before the override was set.

## Key Files

| Component | File |
|-----------|------|
| Proto API | `proto/redpanda/core/admin/v2/shadow_link.proto` |
| Config override injection | `src/v/cluster_link/source_topic_syncer.cc` |
| Property lists | `src/v/cluster_link/model/types.h` |
| Topic creation | `src/v/cluster_link/topic_reconciler.cc` |
| Cloud topic replicator | `src/v/kafka/data/cloud_topic_partition.cc` |
| Cloud topics frontend | `src/v/cloud_topics/frontend/frontend.h` |
| Link config model | `src/v/cluster_link/model/types.h` |
| Link validation | `src/v/cluster/cluster_link/frontend.cc` |
| Failover promotion | `src/v/cluster_link/link_status_reconciler.cc` |

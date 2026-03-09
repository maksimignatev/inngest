# Multi-Instance Deployment: Race Condition Prevention

This document explains how Inngest prevents race conditions when multiple instances share the same Redis server and Postgres database. All guarantees described below are grounded in actual code.

## Overview

Inngest is designed for horizontal scaling with multiple instances (workers) sharing a single Redis + Postgres backend. The system uses a **shared-nothing worker architecture** with **distributed leasing** to prevent race conditions. The core mechanisms are:

1. **Atomic Redis Lua scripts** for all queue operations
2. **Distributed partition leasing** for exclusive partition access
3. **Idempotency checks** with `SET NX` for function run deduplication
4. **Sequential/scavenger leasing** for singleton worker responsibilities
5. **Tombstone markers** for completed run tracking

---

## Question 1: Can Multiple Executors Work on the Same Function Run?

**No.** Queue items (steps within a function run) are protected by a **lease-based locking mechanism** implemented as an **atomic Redis Lua script**.

### Queue Item Leasing

When a worker wants to process a queue item, it must first acquire an exclusive lease. This is enforced atomically in a single Redis Lua script ([`pkg/execution/state/redis_state/lua/queue/lease.lua`](/pkg/execution/state/redis_state/lua/queue/lease.lua)):

```lua
-- lease.lua (lines 82-95)
-- First, get the queue item and bail early if not found.
local item = get_queue_item(keyQueueMap, queueID)
if item == nil then
    return -1  -- Item not found
end

-- Check if the item is already leased by another worker.
if item.leaseID ~= nil and item.leaseID ~= cjson.null
   and decode_ulid_time(item.leaseID) > currentTime then
    -- Already leased; don't let this requester lease the item.
    return -2  -- Already leased
end

-- Only if unleased: set the new lease and remove from ready queue.
item.leaseID = newLeaseID
redis.call("HSET", keyQueueMap, queueID, cjson.encode(item))
redis.call("ZREM", keyReadyQueue, item.id)
```

**Why this is safe:** Redis executes Lua scripts atomically in a single-threaded manner. The check-and-set for the lease happens within one indivisible operation—there is no window for a second worker to also acquire the lease.

### Partition-Level Leasing

Before a worker can even peek at queue items within a partition (a partition typically maps to one function), it must lease the entire partition. This is also atomic via Lua ([`pkg/execution/state/redis_state/lua/queue/partitionLease.lua`](/pkg/execution/state/redis_state/lua/queue/partitionLease.lua)):

```lua
-- partitionLease.lua (lines 41-49)
local existing = get_partition_item(keyPartitionMap, partitionID)
if existing == nil or existing == false then
    return { -3 }  -- Partition not found
end

-- Check for an existing lease.
if existing.leaseID ~= nil and existing.leaseID ~= cjson.null
   and decode_ulid_time(existing.leaseID) > currentTime then
    return { -4 }  -- Partition already leased
end

-- Claim the partition by setting the lease.
existing.leaseID = leaseID
redis.call("HSET", keyPartitionMap, partitionID, cjson.encode(existing))
```

The partition lease duration is **4 seconds** ([`pkg/execution/queue/consts.go`](/pkg/execution/queue/consts.go)):

```go
// PartitionLeaseDuration dictates how long a worker holds the lease for
// a partition. This gives the worker a right to scan all queue items
// for that partition to schedule the execution of jobs.
PartitionLeaseDuration = 4 * time.Second
```

When a worker encounters a partition that is already leased, it records a contention metric and moves on ([`pkg/execution/queue/process_partition.go`](/pkg/execution/queue/process_partition.go), lines 85-92):

```go
if errors.Is(err, ErrPartitionAlreadyLeased) {
    metrics.IncrQueuePartitionLeaseContentionCounter(ctx, ...)
    q.removeContinue(ctx, p, false)
    return nil
}
```

### Lease Extension During Processing

While a queue item is being processed, the worker continually extends the lease to prevent another worker from reclaiming it ([`pkg/execution/queue/process.go`](/pkg/execution/queue/process.go), line 54):

```go
// Continually extend the lease while this job is being processed.
extendLeaseTick := q.Clock().NewTicker(QueueLeaseDuration / 2)
```

The lease duration for individual queue items is **30 seconds** (`QueueLeaseDuration`), renewed every 15 seconds.

---

## Question 2: Can Multiple Runners Create Duplicate Function Runs for the Same Event?

**No.** Function run creation is protected by a **two-layer idempotency mechanism**.

### Layer 1: Redis `SET NX` Idempotency Check

Before creating any function run state, the system performs an atomic idempotency check using Redis `SET NX` (set-if-not-exists) with a **24-hour TTL** ([`pkg/execution/state/redis_state/redis_state.go`](/pkg/execution/state/redis_state/redis_state.go), lines 360-395):

```go
func (m shardedMgr) idempotencyCheck(ctx context.Context, rc RetriableClient,
    key string, id state.Identifier) (*ulid.ULID, error) {

    prev, err := rc.Do(ctx, func(c rueidis.Client) rueidis.Completed {
        return c.B().
            Set().
            Key(key).
            Value(id.RunID.String()).
            Nx().   // Only set if key doesn't exist (atomic)
            Get().  // Retrieve the previous value if exists
            Ex(consts.FunctionIdempotencyPeriod). // 24h TTL
            Build()
    }).ToString()

    if err == rueidis.Nil {
        return nil, nil  // No previous state exists, entirely new
    }
    // ...
}
```

The idempotency key is derived from the event's internal ID (or a custom idempotency key) combined with the function ID ([`pkg/execution/executor/executor.go`](/pkg/execution/executor/executor.go), lines 547-570). This ensures:

- The **same event** triggering the **same function** will always produce the **same idempotency key**
- Even if two instances receive the same event simultaneously, only **one** will succeed with `SET NX`

### Layer 2: Atomic Lua Script for State Creation

The actual state creation also has an atomic duplicate check via the `new.lua` Lua script ([`pkg/execution/state/redis_state/lua/new.lua`](/pkg/execution/state/redis_state/lua/new.lua)):

```lua
-- new.lua (lines 20-23)
-- State is already created
if redis.call("EXISTS", eventsKey) == 1 then
  return 1  -- Run ID already exists
end

-- Only if new: save all metadata atomically
redis.call("HSET", metadataKey, ...)
redis.call("SETNX", eventsKey, events)
return 0
```

### How Duplicates Are Handled

When a duplicate is detected, the system returns `state.ErrIdentifierExists` and the runner silently treats it as a success—no duplicate run is created ([`pkg/execution/runner/runner.go`](/pkg/execution/runner/runner.go), line 731):

```go
case executor.ErrFunctionSkipped,
    executor.ErrFunctionSkippedIdempotency,
    state.ErrIdentifierExists:
    return nil, nil  // Silently succeed (idempotent)
```

### Tombstone Markers for Completed Runs

When a function run finishes, its idempotency key is prefixed with a tombstone marker (`-`). This prevents retried scheduling operations from re-creating already-completed runs ([`pkg/execution/state/redis_state/redis_state.go`](/pkg/execution/state/redis_state/redis_state.go), lines 378-385):

```go
// When a run finishes, we prefix the run ID with the tombstone marker.
if len(prev) > 0 && prev[0] == consts.FunctionIdempotencyTombstone {
    return nil, state.ErrIdentifierTombstone
}
```

The tombstone constant and idempotency period are defined in [`pkg/consts/consts.go`](/pkg/consts/consts.go):

```go
FunctionIdempotencyPeriod   = 24 * time.Hour
FunctionIdempotencyTombstone = '-'
```

---

## Queue Enqueue Idempotency

When enqueuing a new queue item (e.g., a step to execute), the enqueue Lua script also checks for duplicates ([`pkg/execution/state/redis_state/lua/queue/enqueue.lua`](/pkg/execution/state/redis_state/lua/queue/enqueue.lua), lines 64-77):

```lua
-- Check idempotency exists
if redis.call("EXISTS", idempotencyKey) ~= 0 then
  return 1  -- Already enqueued
end

-- Atomically set the queue item (HSETNX = set if not exists)
if redis.call("HSETNX", queueKey, queueID, queueItem) == 0 then
    return 1  -- Already exists
end
```

And upon dequeue, an idempotency TTL is set to prevent re-processing ([`pkg/execution/state/redis_state/lua/queue/dequeue.lua`](/pkg/execution/state/redis_state/lua/queue/dequeue.lua), lines 83-85):

```lua
if idempotencyTTL > 0 then
    redis.call("SETEX", keyIdempotency, idempotencyTTL, "")
end
```

The default dequeue idempotency TTL is **12 hours** ([`pkg/execution/queue/consts.go`](/pkg/execution/queue/consts.go)):

```go
defaultIdempotencyTTL = 12 * time.Hour
```

---

## Singleton Worker Responsibilities

Certain responsibilities should only be handled by a single worker across all instances. Inngest uses **config leasing** to ensure this.

### Sequential Lease

Only **one worker** at a time can process partitions sequentially. This is managed by a config lease ([`pkg/execution/queue/config_lease.go`](/pkg/execution/queue/config_lease.go), lines 14-65):

```go
// claimSequentialLease is a process which continually runs while listening
// to the queue, attempting to claim a lease on sequential processing.
// Only one worker is allowed to work on partitions sequentially;
// this reduces contention.
func (q *queueProcessor) claimSequentialLease(ctx context.Context) {
    leaseID, err := q.primaryQueueShard.ConfigLease(
        ctx, "sequential", ConfigLeaseDuration, q.SequentialLease())
    // ...
}
```

The config lease is implemented atomically in Lua ([`pkg/execution/state/redis_state/lua/queue/configLease.lua`](/pkg/execution/state/redis_state/lua/queue/configLease.lua)):

```lua
-- configLease.lua
local fetched = redis.call("GET", leaseKey)
if fetched == false
   or decode_ulid_time(fetched) < currentTime
   or fetched == existingLeaseID then
    -- Nil, expired, or renewal: safe to claim
    redis.call("SET", leaseKey, newLeaseID)
    return 0
end
return 1  -- Already leased by another worker
```

The lease duration is **10 seconds**, renewed every ~3.3 seconds:

```go
ConfigLeaseDuration = 10 * time.Second
tick := q.Clock().NewTicker(ConfigLeaseDuration / 3)
```

### Scavenger Lease

A dedicated **scavenger worker** reclaims queue items with expired leases (e.g., from crashed workers). Only one worker holds the scavenger lease at a time, using the same config lease mechanism.

---

## Summary: Race Condition Prevention by Layer

| Layer | Mechanism | Atomicity | Code Location |
|-------|-----------|-----------|---------------|
| **Partition access** | Distributed lease via Lua | ✅ Atomic | `lua/queue/partitionLease.lua` |
| **Queue item processing** | Lease-based locking via Lua | ✅ Atomic | `lua/queue/lease.lua` |
| **Function run creation** | `SET NX` with 24h TTL | ✅ Atomic | `redis_state.go` `idempotencyCheck()` |
| **State creation** | Lua script `EXISTS` check | ✅ Atomic | `lua/new.lua` |
| **Queue enqueue** | `HSETNX` + idempotency key | ✅ Atomic | `lua/queue/enqueue.lua` |
| **Queue dequeue** | `SETEX` idempotency TTL | ✅ Atomic | `lua/queue/dequeue.lua` |
| **Sequential processing** | Config lease via Lua | ✅ Atomic | `lua/queue/configLease.lua` |
| **Completed run protection** | Tombstone prefix markers | ✅ Atomic | `redis_state.go` tombstone check |

All critical operations use **Redis Lua scripts**, which execute atomically within Redis's single-threaded execution model. This eliminates "check-then-act" race windows that would exist with separate Redis commands.

## Lease Expiry and Worker Failure

If a worker crashes while holding a lease:

- **Partition leases** expire after **4 seconds**, allowing another worker to reclaim the partition
- **Queue item leases** expire after **30 seconds**, allowing the scavenger to reclaim the item
- **Config leases** (sequential, scavenger) expire after **10 seconds**, allowing another worker to take over the responsibility

This means in the worst case, a crashed worker causes a brief delay (seconds) before work is resumed by another instance—but never duplicate execution.

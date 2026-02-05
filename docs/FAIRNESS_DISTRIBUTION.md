# Fairness Distribution Layer

## Overview

The fairness distribution layer implements weighted round-robin scheduling across tenants (accounts) and users within each tenant. It ensures fair resource distribution based on:

1. **Plan Tier**: Higher-tier plans (Enterprise > Pro > Starter > Free) get higher priority
2. **Recent Consumption**: Accounts/users that have consumed many jobs recently are deprioritized
3. **Weighted Round-Robin**: Ensures fair distribution even among accounts with the same plan tier

## Architecture

### Components

#### 1. FairnessTracker (In-Memory)
- Tracks job consumption in time windows (default: 1 hour)
- Maintains separate counters for accounts and users
- Automatically expires old windows
- Thread-safe

#### 2. RedisFairnessTracker (Distributed)
- Redis-backed consumption tracking
- Uses sorted sets for efficient time-windowed queries
- Automatically cleans up old data
- Suitable for multi-instance deployments

#### 3. CalculateAccountWeight
- Combines plan tier weight and consumption decay
- Formula: `weight = planWeight * exp(-decayFactor * consumption)`
- Higher weights = higher priority

#### 4. WeightedRoundRobinSelector
- Implements fair selection among candidates
- Tracks selection counts to ensure fairness
- Uses ratio-based selection: `ratio = weight / (selections + 1)`

### Integration Points

```
┌──────────────────────────────────────────────────────────────┐
│                      Queue System Flow                        │
└──────────────────────────────────────────────────────────────┘

1. Queue Initialization
   ├─ Create RedisFairnessTracker
   ├─ Define PlanTierGetter
   ├─ Create FairnessConfig
   └─ Pass FairnessAccountPriorityFinder to queue options

2. Account Selection (scan.go - executionScan)
   ├─ AccountPeek() is called
   ├─ For each account, AccountPriorityFinder is invoked
   ├─ FairnessAccountPriorityFinder calculates priority based on:
   │  ├─ Plan tier (via PlanTierGetter)
   │  ├─ Recent consumption (via tracker)
   │  └─ Returns priority (0-10 scale, 0 = highest)
   └─ Accounts are weighted-shuffled based on priorities

3. Job Processing (process.go - ProcessPartition)
   ├─ Job is successfully processed
   ├─ Record consumption:
   │  └─ tracker.RecordAccountConsumption(ctx, accountID, 1)
   └─ Optionally record user-level:
      └─ tracker.RecordUserConsumption(ctx, accountID, userID, 1)

4. Partition Processing
   └─ Can implement user-level fairness within partitions
```

## Configuration

### Default Configuration

```go
config := FairnessConfig{
    EnableFairness: true,
    WindowDuration: time.Hour,
    PlanWeights: map[PlanTier]float64{
        PlanTierFree:       1.0,
        PlanTierStarter:    2.0,
        PlanTierPro:        4.0,
        PlanTierEnterprise: 8.0,
    },
    ConsumptionDecayFactor: 0.001,
}
```

### Tuning Parameters

- **WindowDuration**: Time window for tracking consumption (default: 1 hour)
  - Shorter windows = more responsive to recent consumption
  - Longer windows = smoother fairness distribution

- **PlanWeights**: Relative priority for each plan tier
  - Higher values = more priority
  - Recommend exponential scaling (1x, 2x, 4x, 8x)

- **ConsumptionDecayFactor**: How much consumption affects priority
  - Higher values = consumption has stronger impact
  - Recommended range: 0.0001 - 0.01
  - Calculate based on expected job volumes:
    - For 100-1000 jobs/hour: 0.001
    - For 1000-10000 jobs/hour: 0.0001

## Usage Examples

### Basic Setup

```go
// 1. Create fairness tracker (in-memory for single instance)
tracker := queue.NewFairnessTracker(time.Hour)

// 2. Define plan tier getter
planGetter := func(ctx context.Context, accountID uuid.UUID) queue.PlanTier {
    // Query your database/cache for account plan
    account := db.GetAccount(ctx, accountID)
    return mapToPlanTier(account.PlanLevel)
}

// 3. Create priority finder
config := queue.DefaultFairnessConfig()
priorityFinder := queue.FairnessAccountPriorityFinder(tracker, planGetter, config)

// 4. Pass to queue options
queueOpts := []queue.QueueOpt{
    queue.WithAccountPriorityFinder(priorityFinder),
    // ... other options
}
```

### Redis-Backed Setup (Distributed)

```go
// Use RedisFairnessTracker for multi-instance deployments
tracker := redis_state.NewRedisFairnessTracker(
    redisClient,
    keyGenerator,
    time.Hour,
)

priorityFinder := redis_state.FairnessAccountPriorityFinder(
    tracker,
    planGetter,
    config,
)
```

### Recording Consumption

```go
// After processing a job successfully
func (p *processor) afterJobComplete(ctx context.Context, item queue.QueueItem) {
    accountID := item.Data.Identifier.AccountID
    
    // Record account-level consumption
    if err := tracker.RecordAccountConsumption(ctx, accountID, 1); err != nil {
        logger.Error("failed to record consumption", "error", err)
    }
    
    // Optionally record user-level consumption
    if userID := getUserIDFromItem(item); userID != nil {
        tracker.RecordUserConsumption(ctx, accountID, *userID, 1)
    }
}
```

## How It Works

### Priority Calculation

For each account during AccountPeek:

1. **Get Plan Weight**: Look up plan tier and get base weight
   ```
   Enterprise: 8.0
   Pro: 4.0
   Starter: 2.0
   Free: 1.0
   ```

2. **Apply Consumption Penalty**: Recent consumption reduces weight
   ```
   penalty = exp(-0.001 * consumption)
   
   Example:
   - 10 jobs:   penalty = 0.99  (minimal impact)
   - 100 jobs:  penalty = 0.90  (10% reduction)
   - 1000 jobs: penalty = 0.37  (63% reduction)
   ```

3. **Calculate Final Weight**:
   ```
   weight = planWeight * penalty
   
   Example (1000 jobs consumed):
   - Enterprise: 8.0 * 0.37 = 2.96
   - Pro:        4.0 * 0.37 = 1.48
   - Free:       1.0 * 0.37 = 0.37
   ```

4. **Convert to Priority** (0-10 scale, 0 = highest):
   ```
   priority = 10 - min(weight, 10)
   ```

### Weighted Sampling

The existing AccountPeek uses weighted sampling (via gonum's sampleuv.Weighted):
- Accounts with lower priority values get higher selection probability
- This creates the weighted round-robin effect

## Benefits

1. **Fair Resource Distribution**
   - Prevents resource starvation
   - Balances load across accounts

2. **Plan-Based Prioritization**
   - Higher-tier customers get better service
   - Aligns with business model

3. **Consumption-Aware**
   - Prevents any single account from dominating
   - Ensures everyone gets a fair share

4. **Flexible & Configurable**
   - Tune parameters for your workload
   - Enable/disable per environment

5. **No Breaking Changes**
   - Integrates with existing AccountPriorityFinder interface
   - Can be gradually rolled out

## Testing

Run tests:
```bash
go test ./pkg/execution/queue -run TestFairness -v
go test ./pkg/execution/queue -run TestCalculateAccountWeight -v
go test ./pkg/execution/queue -run TestWeightedRoundRobinSelector -v
```

## Monitoring

Key metrics to track:

1. **Consumption Distribution**: Monitor job counts per account in window
2. **Priority Distribution**: Track calculated priorities across accounts
3. **Selection Frequency**: Verify weighted round-robin behavior
4. **Plan Tier Distribution**: Ensure higher tiers get proportionally more jobs

## Future Enhancements

1. **User-Level Fairness**: Extend to users within accounts
2. **Dynamic Weights**: Adjust plan weights based on real-time load
3. **Per-Function Fairness**: Track consumption per function, not just account
4. **Backpressure Integration**: Coordinate with rate limiting and throttling
5. **Metrics & Observability**: Add Prometheus metrics for fairness tracking

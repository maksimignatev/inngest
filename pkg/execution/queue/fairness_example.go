package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Example integration of fairness distribution layer
//
// This file shows how to integrate the fairness distribution layer into the queue system.

// ExampleFairnessIntegration shows how to set up fairness-aware queue processing
func ExampleFairnessIntegration() {
	// 1. Create a fairness tracker
	tracker := NewFairnessTracker(time.Hour)

	// 2. Define a plan tier getter
	// This should be implemented to fetch the actual plan tier from your database
	planGetter := func(ctx context.Context, accountID uuid.UUID) PlanTier {
		// Example: Look up account in database and return their plan tier
		// In reality, this would query your database/cache
		return PlanTierPro // Placeholder
	}

	// 3. Configure fairness settings
	config := FairnessConfig{
		EnableFairness:         true,
		WindowDuration:         time.Hour,
		PlanWeights: map[PlanTier]float64{
			PlanTierFree:       1.0,
			PlanTierStarter:    2.0,
			PlanTierPro:        4.0,
			PlanTierEnterprise: 8.0,
		},
		ConsumptionDecayFactor: 0.001,
	}

	// 4. Create the fairness-aware account priority finder
	priorityFinder := FairnessAccountPriorityFinder(tracker, planGetter, config)

	// 5. Use the priority finder when initializing queue
	// This would be passed to WithAccountPriorityFinder() when creating the queue
	_ = priorityFinder

	// 6. Record consumption when processing jobs
	// This should be called after successfully processing a job
	accountID := uuid.New()
	ctx := context.Background()
	tracker.RecordAccountConsumption(ctx, accountID, 1)

	fmt.Println("Fairness integration example complete")
	// Output: Fairness integration example complete
}

// ExampleFairnessWithRedis shows how to use Redis-backed fairness tracking
func ExampleFairnessWithRedis() {
	// This example shows the pattern, but requires actual Redis client initialization
	// which is omitted for clarity
	
	fmt.Println("Use RedisFairnessTracker for distributed fairness tracking")
	fmt.Println("Initialize with: NewRedisFairnessTracker(redisClient, keyGenerator, windowDuration)")
	fmt.Println("Record consumption: tracker.RecordAccountConsumption(ctx, accountID, count)")
	fmt.Println("Get consumption: consumption, err := tracker.GetAccountConsumption(ctx, accountID)")
	
	// Output: 
	// Use RedisFairnessTracker for distributed fairness tracking
	// Initialize with: NewRedisFairnessTracker(redisClient, keyGenerator, windowDuration)
	// Record consumption: tracker.RecordAccountConsumption(ctx, accountID, count)
	// Get consumption: consumption, err := tracker.GetAccountConsumption(ctx, accountID)
}

// Integration points in the queue system:
//
// 1. Queue Initialization (in pkg/execution/state/redis_state or similar):
//    - Create RedisFairnessTracker
//    - Pass fairness-aware AccountPriorityFinder to queue options
//
// 2. Job Processing (in processor.go or similar):
//    - After successfully processing a job, record consumption:
//      tracker.RecordAccountConsumption(ctx, accountID, 1)
//
// 3. Account Selection (in scan.go - AccountPeek):
//    - The AccountPriorityFinder is already used in AccountPeek
//    - With fairness enabled, it will now consider consumption and plan tier
//
// 4. Configuration:
//    - Add FairnessConfig to queue options
//    - Allow enabling/disabling via environment variables or config files

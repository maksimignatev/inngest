package redis_state

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/telemetry/redis_telemetry"
	"github.com/redis/rueidis"
)

// RedisFairnessTracker implements fairness tracking using Redis for persistence
type RedisFairnessTracker struct {
	client         rueidis.Client
	kg             QueueKeyGenerator
	windowDuration time.Duration
}

// NewRedisFairnessTracker creates a Redis-backed fairness tracker
func NewRedisFairnessTracker(client rueidis.Client, kg QueueKeyGenerator, windowDuration time.Duration) *RedisFairnessTracker {
	if windowDuration == 0 {
		windowDuration = time.Hour
	}
	
	return &RedisFairnessTracker{
		client:         client,
		kg:             kg,
		windowDuration: windowDuration,
	}
}

// RecordAccountConsumption records job consumption for an account
func (rft *RedisFairnessTracker) RecordAccountConsumption(ctx context.Context, accountID uuid.UUID, count int64) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "recordAccountConsumption"), redis_telemetry.ScopeQueue)
	
	key := rft.kg.FairnessAccountConsumption(accountID)
	now := time.Now()
	
	// Use a sliding window approach with Redis sorted set
	// Score is the timestamp in milliseconds
	score := float64(now.UnixMilli())
	
	// Add current consumption with timestamp as score
	member := fmt.Sprintf("%d:%d", now.UnixMilli(), count)
	
	cmds := []rueidis.Completed{
		// Add new entry
		rft.client.B().Zadd().Key(key).ScoreMember().ScoreMember(score, member).Build(),
		// Remove entries older than window
		rft.client.B().Zremrangebyscore().
			Key(key).
			Min(fmt.Sprintf("0")).
			Max(fmt.Sprintf("%f", float64(now.Add(-rft.windowDuration).UnixMilli()))).
			Build(),
		// Set expiry on the key (2x window duration for safety)
		rft.client.B().Expire().Key(key).Seconds(int64(rft.windowDuration.Seconds() * 2)).Build(),
	}
	
	for _, cmd := range cmds {
		if err := rft.client.Do(ctx, cmd).Error(); err != nil {
			return fmt.Errorf("error recording account consumption: %w", err)
		}
	}
	
	return nil
}

// GetAccountConsumption retrieves total job consumption for an account in the current window
func (rft *RedisFairnessTracker) GetAccountConsumption(ctx context.Context, accountID uuid.UUID) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "getAccountConsumption"), redis_telemetry.ScopeQueue)
	
	key := rft.kg.FairnessAccountConsumption(accountID)
	now := time.Now()
	windowStart := now.Add(-rft.windowDuration)
	
	// Get all entries within the window
	cmd := rft.client.B().Zrangebyscore().
		Key(key).
		Min(fmt.Sprintf("%f", float64(windowStart.UnixMilli()))).
		Max(fmt.Sprintf("%f", float64(now.UnixMilli()))).
		Build()
	
	members, err := rft.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return 0, fmt.Errorf("error getting account consumption: %w", err)
	}
	
	// Parse and sum consumption values
	var total int64
	for _, member := range members {
		// Parse member format: "timestamp:count"
		var ts, count int64
		_, err := fmt.Sscanf(member, "%d:%d", &ts, &count)
		if err != nil {
			continue // Skip malformed entries
		}
		total += count
	}
	
	return total, nil
}

// RecordUserConsumption records job consumption for a user within an account
func (rft *RedisFairnessTracker) RecordUserConsumption(ctx context.Context, accountID, userID uuid.UUID, count int64) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "recordUserConsumption"), redis_telemetry.ScopeQueue)
	
	key := rft.kg.FairnessUserConsumption(accountID, userID)
	now := time.Now()
	score := float64(now.UnixMilli())
	member := fmt.Sprintf("%d:%d", now.UnixMilli(), count)
	
	cmds := []rueidis.Completed{
		rft.client.B().Zadd().Key(key).ScoreMember().ScoreMember(score, member).Build(),
		rft.client.B().Zremrangebyscore().
			Key(key).
			Min(fmt.Sprintf("0")).
			Max(fmt.Sprintf("%f", float64(now.Add(-rft.windowDuration).UnixMilli()))).
			Build(),
		rft.client.B().Expire().Key(key).Seconds(int64(rft.windowDuration.Seconds() * 2)).Build(),
	}
	
	for _, cmd := range cmds {
		if err := rft.client.Do(ctx, cmd).Error(); err != nil {
			return fmt.Errorf("error recording user consumption: %w", err)
		}
	}
	
	return nil
}

// GetUserConsumption retrieves total job consumption for a user within an account
func (rft *RedisFairnessTracker) GetUserConsumption(ctx context.Context, accountID, userID uuid.UUID) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "getUserConsumption"), redis_telemetry.ScopeQueue)
	
	key := rft.kg.FairnessUserConsumption(accountID, userID)
	now := time.Now()
	windowStart := now.Add(-rft.windowDuration)
	
	cmd := rft.client.B().Zrangebyscore().
		Key(key).
		Min(fmt.Sprintf("%f", float64(windowStart.UnixMilli()))).
		Max(fmt.Sprintf("%f", float64(now.UnixMilli()))).
		Build()
	
	members, err := rft.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return 0, fmt.Errorf("error getting user consumption: %w", err)
	}
	
	var total int64
	for _, member := range members {
		var ts, count int64
		_, err := fmt.Sscanf(member, "%d:%d", &ts, &count)
		if err != nil {
			continue
		}
		total += count
	}
	
	return total, nil
}

// FairnessAccountPriorityFinder creates an AccountPriorityFinder using Redis-based fairness tracking
func FairnessAccountPriorityFinder(
	tracker *RedisFairnessTracker,
	planGetter osqueue.PlanTierGetter,
	config osqueue.FairnessConfig,
) osqueue.AccountPriorityFinder {
	return func(ctx context.Context, accountID uuid.UUID) uint {
		if !config.EnableFairness {
			return osqueue.PriorityDefault
		}
		
		// Get account consumption
		consumption, err := tracker.GetAccountConsumption(ctx, accountID)
		if err != nil {
			// On error, return default priority
			return osqueue.PriorityDefault
		}
		
		// Get plan tier
		planTier := planGetter(ctx, accountID)
		
		// Calculate fairness weight
		weight := osqueue.CalculateAccountWeight(ctx, accountID, planTier, consumption, config)
		
		// Convert weight to priority (0-10 scale)
		// Higher weights map to lower priority values (closer to 0 = higher priority)
		// We invert by doing 10 - weight, clamped to 0-10 range
		priority := uint(10 - min(weight, 10))
		
		return priority
	}
}

// IncrementAccountJobCount increments the job counter for an account atomically
// This is a simplified version that just increments by 1 for each job processed
func (rft *RedisFairnessTracker) IncrementAccountJobCount(ctx context.Context, accountID uuid.UUID) error {
	return rft.RecordAccountConsumption(ctx, accountID, 1)
}

// IncrementUserJobCount increments the job counter for a user within an account
func (rft *RedisFairnessTracker) IncrementUserJobCount(ctx context.Context, accountID, userID uuid.UUID) error {
	return rft.RecordUserConsumption(ctx, accountID, userID, 1)
}

// min returns the minimum of two float64 values
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

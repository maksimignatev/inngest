package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFairnessTracker_AccountConsumption(t *testing.T) {
	ctx := context.Background()
	tracker := NewFairnessTracker(time.Minute)
	
	accountID := uuid.New()
	
	// Record consumption
	tracker.RecordAccountConsumption(ctx, accountID, 100)
	
	// Verify consumption
	consumption := tracker.GetAccountConsumption(ctx, accountID)
	assert.Equal(t, int64(100), consumption)
	
	// Record more consumption
	tracker.RecordAccountConsumption(ctx, accountID, 50)
	consumption = tracker.GetAccountConsumption(ctx, accountID)
	assert.Equal(t, int64(150), consumption)
}

func TestFairnessTracker_UserConsumption(t *testing.T) {
	ctx := context.Background()
	tracker := NewFairnessTracker(time.Minute)
	
	accountID := uuid.New()
	userID1 := uuid.New()
	userID2 := uuid.New()
	
	// Record consumption for different users
	tracker.RecordUserConsumption(ctx, accountID, userID1, 100)
	tracker.RecordUserConsumption(ctx, accountID, userID2, 50)
	
	// Verify consumption per user
	assert.Equal(t, int64(100), tracker.GetUserConsumption(ctx, accountID, userID1))
	assert.Equal(t, int64(50), tracker.GetUserConsumption(ctx, accountID, userID2))
}

func TestFairnessTracker_WindowExpiry(t *testing.T) {
	ctx := context.Background()
	tracker := NewFairnessTracker(100 * time.Millisecond)
	
	accountID := uuid.New()
	
	// Record consumption
	tracker.RecordAccountConsumption(ctx, accountID, 100)
	assert.Equal(t, int64(100), tracker.GetAccountConsumption(ctx, accountID))
	
	// Wait for window to expire
	time.Sleep(150 * time.Millisecond)
	
	// Consumption should be reset
	consumption := tracker.GetAccountConsumption(ctx, accountID)
	assert.Equal(t, int64(0), consumption)
}

func TestCalculateAccountWeight(t *testing.T) {
	ctx := context.Background()
	config := DefaultFairnessConfig()
	accountID := uuid.New()
	
	tests := []struct {
		name        string
		planTier    PlanTier
		consumption int64
		minWeight   float64
		maxWeight   float64
	}{
		{
			name:        "Enterprise with low consumption",
			planTier:    PlanTierEnterprise,
			consumption: 10,
			minWeight:   7.0,
			maxWeight:   8.0,
		},
		{
			name:        "Enterprise with high consumption",
			planTier:    PlanTierEnterprise,
			consumption: 1000,
			minWeight:   2.0,
			maxWeight:   4.0,
		},
		{
			name:        "Free with low consumption",
			planTier:    PlanTierFree,
			consumption: 10,
			minWeight:   0.9,
			maxWeight:   1.0,
		},
		{
			name:        "Free with high consumption",
			planTier:    PlanTierFree,
			consumption: 1000,
			minWeight:   MinimumWeight,
			maxWeight:   0.5,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			weight := CalculateAccountWeight(ctx, accountID, tt.planTier, tt.consumption, config)
			assert.GreaterOrEqual(t, weight, tt.minWeight, "Weight should be >= minWeight")
			assert.LessOrEqual(t, weight, tt.maxWeight, "Weight should be <= maxWeight")
			assert.GreaterOrEqual(t, weight, MinimumWeight, "Weight should never go below minimum threshold")
		})
	}
}

func TestCalculateAccountWeight_PlanTierOrdering(t *testing.T) {
	ctx := context.Background()
	config := DefaultFairnessConfig()
	accountID := uuid.New()
	
	// With same consumption, higher plan tiers should have higher weights
	consumption := int64(100)
	
	freeWeight := CalculateAccountWeight(ctx, accountID, PlanTierFree, consumption, config)
	starterWeight := CalculateAccountWeight(ctx, accountID, PlanTierStarter, consumption, config)
	proWeight := CalculateAccountWeight(ctx, accountID, PlanTierPro, consumption, config)
	enterpriseWeight := CalculateAccountWeight(ctx, accountID, PlanTierEnterprise, consumption, config)
	
	assert.Less(t, freeWeight, starterWeight, "Starter should have higher weight than Free")
	assert.Less(t, starterWeight, proWeight, "Pro should have higher weight than Starter")
	assert.Less(t, proWeight, enterpriseWeight, "Enterprise should have higher weight than Pro")
}

func TestCalculateAccountWeight_ConsumptionImpact(t *testing.T) {
	ctx := context.Background()
	config := DefaultFairnessConfig()
	accountID := uuid.New()
	
	// For same plan tier, higher consumption should lead to lower weight
	planTier := PlanTierPro
	
	lowConsumptionWeight := CalculateAccountWeight(ctx, accountID, planTier, 10, config)
	highConsumptionWeight := CalculateAccountWeight(ctx, accountID, planTier, 1000, config)
	
	assert.Greater(t, lowConsumptionWeight, highConsumptionWeight,
		"Lower consumption should result in higher weight")
}

func TestWeightedRoundRobinSelector_Select(t *testing.T) {
	ctx := context.Background()
	selector := NewWeightedRoundRobinSelector(time.Hour)
	
	// Create test candidates with different weights
	candidate1 := uuid.New()
	candidate2 := uuid.New()
	candidate3 := uuid.New()
	
	candidates := []uuid.UUID{candidate1, candidate2, candidate3}
	weights := []float64{4.0, 2.0, 1.0} // 4:2:1 ratio
	
	// Select multiple times and track distribution
	selections := make(map[int]int)
	iterations := 700
	
	for i := 0; i < iterations; i++ {
		idx, err := selector.Select(ctx, candidates, weights)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, len(candidates))
		selections[idx]++
	}
	
	// Verify weighted distribution (with some tolerance)
	// Expected ratio is 4:2:1, so out of 700:
	// candidate1: ~400, candidate2: ~200, candidate3: ~100
	
	total := float64(iterations)
	ratio1 := float64(selections[0]) / total
	ratio2 := float64(selections[1]) / total
	ratio3 := float64(selections[2]) / total
	
	// Allow 10% tolerance
	assert.InDelta(t, 4.0/7.0, ratio1, 0.1, "Candidate 1 should get ~57% of selections")
	assert.InDelta(t, 2.0/7.0, ratio2, 0.1, "Candidate 2 should get ~29% of selections")
	assert.InDelta(t, 1.0/7.0, ratio3, 0.1, "Candidate 3 should get ~14% of selections")
}

func TestWeightedRoundRobinSelector_EqualWeights(t *testing.T) {
	ctx := context.Background()
	selector := NewWeightedRoundRobinSelector(time.Hour)
	
	// Create test candidates with equal weights
	candidate1 := uuid.New()
	candidate2 := uuid.New()
	
	candidates := []uuid.UUID{candidate1, candidate2}
	weights := []float64{1.0, 1.0}
	
	// Select multiple times
	selections := make(map[int]int)
	iterations := 200
	
	for i := 0; i < iterations; i++ {
		idx, err := selector.Select(ctx, candidates, weights)
		assert.NoError(t, err)
		selections[idx]++
	}
	
	// With equal weights, distribution should be roughly equal
	assert.InDelta(t, float64(iterations)/2, float64(selections[0]), 20)
	assert.InDelta(t, float64(iterations)/2, float64(selections[1]), 20)
}

func TestWeightedRoundRobinSelector_EmptyCandidates(t *testing.T) {
	ctx := context.Background()
	selector := NewWeightedRoundRobinSelector(time.Hour)
	
	_, err := selector.Select(ctx, []uuid.UUID{}, []float64{})
	assert.Error(t, err, "Should error on empty candidates")
}

func TestWeightedRoundRobinSelector_MismatchedLengths(t *testing.T) {
	ctx := context.Background()
	selector := NewWeightedRoundRobinSelector(time.Hour)
	
	candidates := []uuid.UUID{uuid.New(), uuid.New()}
	weights := []float64{1.0}
	
	_, err := selector.Select(ctx, candidates, weights)
	assert.Error(t, err, "Should error on mismatched lengths")
}

func TestFairnessAccountPriorityFinder(t *testing.T) {
	ctx := context.Background()
	tracker := NewFairnessTracker(time.Hour)
	config := DefaultFairnessConfig()
	
	planGetter := func(ctx context.Context, accountID uuid.UUID) PlanTier {
		// Simple mock: return Enterprise for all
		return PlanTierEnterprise
	}
	
	finder := FairnessAccountPriorityFinder(tracker, planGetter, config)
	
	accountID := uuid.New()
	
	// Low consumption should result in higher priority (lower number)
	tracker.RecordAccountConsumption(ctx, accountID, 10)
	lowConsumptionPriority := finder(ctx, accountID)
	
	// High consumption should result in lower priority (higher number)
	tracker.RecordAccountConsumption(ctx, accountID, 990) // Total: 1000
	highConsumptionPriority := finder(ctx, accountID)
	
	assert.Less(t, lowConsumptionPriority, highConsumptionPriority,
		"Higher consumption should result in lower priority (higher number)")
}

func TestFairnessAccountPriorityFinder_Disabled(t *testing.T) {
	ctx := context.Background()
	tracker := NewFairnessTracker(time.Hour)
	config := DefaultFairnessConfig()
	config.EnableFairness = false
	
	planGetter := func(ctx context.Context, accountID uuid.UUID) PlanTier {
		return PlanTierEnterprise
	}
	
	finder := FairnessAccountPriorityFinder(tracker, planGetter, config)
	
	accountID := uuid.New()
	tracker.RecordAccountConsumption(ctx, accountID, 1000)
	
	priority := finder(ctx, accountID)
	assert.Equal(t, PriorityDefault, priority, "Should return default priority when disabled")
}

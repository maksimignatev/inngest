package queue

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// MaxPriorityValue is the maximum value in the priority scale (0-10)
	// where 0 represents highest priority
	MaxPriorityValue = 10
	
	// MinimumWeight is the minimum weight threshold to prevent complete starvation
	// Even accounts with very high consumption will maintain this minimum weight
	MinimumWeight = 0.1
)

// FairnessTracker tracks job consumption metrics for accounts and users
// to enable fair scheduling based on recent usage and plan tier.
type FairnessTracker struct {
	mu sync.RWMutex
	
	// accountConsumption tracks jobs consumed per account in the current time window
	accountConsumption map[uuid.UUID]*ConsumptionWindow
	
	// userConsumption tracks jobs consumed per user within each account
	// map[accountID]map[userID]*ConsumptionWindow
	userConsumption map[uuid.UUID]map[uuid.UUID]*ConsumptionWindow
	
	// windowDuration is the time window for tracking consumption (default: 1 hour)
	windowDuration time.Duration
	
	// lastCleanup tracks when we last cleaned up old windows
	lastCleanup time.Time
}

// ConsumptionWindow tracks job consumption within a time window
type ConsumptionWindow struct {
	Count      int64
	WindowStart time.Time
}

// PlanTier represents the billing plan tier for an account
type PlanTier int

const (
	PlanTierFree PlanTier = iota
	PlanTierStarter
	PlanTierPro
	PlanTierEnterprise
)

// FairnessConfig contains configuration for the fairness distribution algorithm
type FairnessConfig struct {
	// EnableFairness enables/disables fairness distribution
	EnableFairness bool
	
	// WindowDuration is the time window for tracking job consumption (default: 1 hour)
	WindowDuration time.Duration
	
	// PlanWeights maps plan tiers to their priority weights (higher = more priority)
	PlanWeights map[PlanTier]float64
	
	// ConsumptionDecayFactor controls how much recent consumption affects priority
	// Higher values mean consumption has more impact on deprioritization
	ConsumptionDecayFactor float64
}

// DefaultFairnessConfig returns a sensible default configuration
func DefaultFairnessConfig() FairnessConfig {
	return FairnessConfig{
		EnableFairness: true,
		WindowDuration: time.Hour,
		PlanWeights: map[PlanTier]float64{
			PlanTierFree:       1.0,
			PlanTierStarter:    2.0,
			PlanTierPro:        4.0,
			PlanTierEnterprise: 8.0,
		},
		ConsumptionDecayFactor: 0.001, // Adjust based on expected job volumes
	}
}

// NewFairnessTracker creates a new fairness tracker
func NewFairnessTracker(windowDuration time.Duration) *FairnessTracker {
	if windowDuration == 0 {
		windowDuration = time.Hour
	}
	
	return &FairnessTracker{
		accountConsumption: make(map[uuid.UUID]*ConsumptionWindow),
		userConsumption:    make(map[uuid.UUID]map[uuid.UUID]*ConsumptionWindow),
		windowDuration:     windowDuration,
		lastCleanup:        time.Now(),
	}
}

// RecordAccountConsumption records that an account consumed jobs
func (ft *FairnessTracker) RecordAccountConsumption(ctx context.Context, accountID uuid.UUID, count int64) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	
	now := time.Now()
	
	// Get or create consumption window
	window, exists := ft.accountConsumption[accountID]
	if !exists || now.Sub(window.WindowStart) > ft.windowDuration {
		window = &ConsumptionWindow{
			Count:      0,
			WindowStart: now.Truncate(ft.windowDuration),
		}
		ft.accountConsumption[accountID] = window
	}
	
	window.Count += count
	
	// Periodically clean up old windows
	if now.Sub(ft.lastCleanup) > ft.windowDuration {
		ft.cleanupOldWindows(now)
	}
}

// RecordUserConsumption records that a user within an account consumed jobs
func (ft *FairnessTracker) RecordUserConsumption(ctx context.Context, accountID, userID uuid.UUID, count int64) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	
	now := time.Now()
	
	// Get or create account-level user map
	if _, exists := ft.userConsumption[accountID]; !exists {
		ft.userConsumption[accountID] = make(map[uuid.UUID]*ConsumptionWindow)
	}
	
	// Get or create user consumption window
	window, exists := ft.userConsumption[accountID][userID]
	if !exists || now.Sub(window.WindowStart) > ft.windowDuration {
		window = &ConsumptionWindow{
			Count:      0,
			WindowStart: now.Truncate(ft.windowDuration),
		}
		ft.userConsumption[accountID][userID] = window
	}
	
	window.Count += count
}

// GetAccountConsumption returns the job consumption count for an account in the current window
func (ft *FairnessTracker) GetAccountConsumption(ctx context.Context, accountID uuid.UUID) int64 {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	
	window, exists := ft.accountConsumption[accountID]
	if !exists {
		return 0
	}
	
	// Check if window is still valid
	if time.Since(window.WindowStart) > ft.windowDuration {
		return 0
	}
	
	return window.Count
}

// GetUserConsumption returns the job consumption count for a user within an account
func (ft *FairnessTracker) GetUserConsumption(ctx context.Context, accountID, userID uuid.UUID) int64 {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	
	accountUsers, exists := ft.userConsumption[accountID]
	if !exists {
		return 0
	}
	
	window, exists := accountUsers[userID]
	if !exists {
		return 0
	}
	
	// Check if window is still valid
	if time.Since(window.WindowStart) > ft.windowDuration {
		return 0
	}
	
	return window.Count
}

// cleanupOldWindows removes consumption windows that are no longer relevant
func (ft *FairnessTracker) cleanupOldWindows(now time.Time) {
	// Clean up account consumption
	for accountID, window := range ft.accountConsumption {
		if now.Sub(window.WindowStart) > ft.windowDuration*2 {
			delete(ft.accountConsumption, accountID)
		}
	}
	
	// Clean up user consumption
	for accountID, users := range ft.userConsumption {
		for userID, window := range users {
			if now.Sub(window.WindowStart) > ft.windowDuration*2 {
				delete(users, userID)
			}
		}
		if len(users) == 0 {
			delete(ft.userConsumption, accountID)
		}
	}
	
	ft.lastCleanup = now
}

// CalculateAccountWeight calculates the fairness weight for an account based on
// plan tier and recent consumption. Higher weights mean higher priority.
func CalculateAccountWeight(
	ctx context.Context,
	accountID uuid.UUID,
	planTier PlanTier,
	consumption int64,
	config FairnessConfig,
) float64 {
	// Start with base weight from plan tier
	planWeight, exists := config.PlanWeights[planTier]
	if !exists {
		planWeight = 1.0
	}
	
	// Apply consumption-based decay
	// As consumption increases, weight decreases exponentially
	consumptionPenalty := math.Exp(-config.ConsumptionDecayFactor * float64(consumption))
	
	// Combined weight: plan weight * consumption penalty
	// Higher plan tiers get more weight, but high consumption reduces it
	weight := planWeight * consumptionPenalty
	
	// Ensure minimum weight to prevent starvation
	if weight < MinimumWeight {
		weight = MinimumWeight
	}
	
	return weight
}

// PlanTierGetter is a function that returns the plan tier for an account
type PlanTierGetter func(ctx context.Context, accountID uuid.UUID) PlanTier

// UserIDGetter is a function that extracts the user ID from a partition or context
type UserIDGetter func(ctx context.Context, accountID uuid.UUID, partition QueuePartition) *uuid.UUID

// FairnessAccountPriorityFinder creates an AccountPriorityFinder that uses fairness-based weighting
func FairnessAccountPriorityFinder(
	tracker *FairnessTracker,
	planGetter PlanTierGetter,
	config FairnessConfig,
) AccountPriorityFinder {
	return func(ctx context.Context, accountID uuid.UUID) uint {
		if !config.EnableFairness {
			return PriorityDefault
		}
		
		// Get account consumption and plan tier
		consumption := tracker.GetAccountConsumption(ctx, accountID)
		planTier := planGetter(ctx, accountID)
		
		// Calculate fairness weight
		weight := CalculateAccountWeight(ctx, accountID, planTier, consumption, config)
		
		// Convert weight to priority (0-MaxPriorityValue scale, where 0 is highest priority)
		// Higher weights should map to lower priority values (closer to 0)
		priority := uint(MaxPriorityValue - math.Min(weight, float64(MaxPriorityValue)))
		
		return priority
	}
}

// WeightedRoundRobinSelector implements weighted round-robin selection
type WeightedRoundRobinSelector struct {
	mu sync.Mutex
	
	// counters tracks selection counts for each item
	counters map[string]int64
	
	// lastReset tracks when counters were last reset
	lastReset time.Time
	
	// resetInterval determines how often to reset counters
	resetInterval time.Duration
}

// NewWeightedRoundRobinSelector creates a new weighted round-robin selector
func NewWeightedRoundRobinSelector(resetInterval time.Duration) *WeightedRoundRobinSelector {
	if resetInterval == 0 {
		resetInterval = time.Hour
	}
	
	return &WeightedRoundRobinSelector{
		counters:      make(map[string]int64),
		lastReset:     time.Now(),
		resetInterval: resetInterval,
	}
}

// Select chooses the next item from candidates based on weighted round-robin
// Returns the index of the selected item
func (w *WeightedRoundRobinSelector) Select(
	ctx context.Context,
	candidates []uuid.UUID,
	weights []float64,
) (int, error) {
	if len(candidates) == 0 {
		return -1, fmt.Errorf("no candidates provided")
	}
	
	if len(candidates) != len(weights) {
		return -1, fmt.Errorf("candidates and weights length mismatch")
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Reset counters if needed
	now := time.Now()
	if now.Sub(w.lastReset) > w.resetInterval {
		w.counters = make(map[string]int64)
		w.lastReset = now
	}
	
	// Find the candidate with the highest (weight / (selections + 1)) ratio
	bestIdx := 0
	bestRatio := -1.0
	
	for i, candidate := range candidates {
		key := candidate.String()
		selections := w.counters[key]
		
		// Calculate selection ratio: weight / (selections + 1)
		// This ensures items with higher weights are selected more often
		// while still giving chances to lower-weight items
		ratio := weights[i] / float64(selections+1)
		
		if ratio > bestRatio {
			bestRatio = ratio
			bestIdx = i
		}
	}
	
	// Increment selection counter for the chosen candidate
	w.counters[candidates[bestIdx].String()]++
	
	return bestIdx, nil
}

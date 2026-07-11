package federated_learning

import (
	"encoding/json"
	"os"
)

type AggregatorState struct {
	LastCompletedRound int             `json:"last_completed_round"`
	GlobalAverages     map[int]float64 `json:"global_averages"`
}

func SaveState(path string, state AggregatorState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func LoadState(path string) (AggregatorState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return AggregatorState{}, err
	}

	var state AggregatorState
	if err := json.Unmarshal(data, &state); err != nil {
		return AggregatorState{}, err
	}
	return state, nil
}

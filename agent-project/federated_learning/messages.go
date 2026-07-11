package federated_learning

import "agent-project/actor_framework"

type StartTrainingEpoch struct {
	Round       int
	TotalRounds int
}

type GlobalModelBroadcast struct {
	GlobalAverage float64
	Round         int
}

type CalculateLocalMetrics struct {
	FilePath    string
	Round       int
	TotalRounds int
}

type LocalMetricsUpdate struct {
	Average float64
	Count   int
	Round   int
}

type RegisterTrainer struct {
	TrainerPID *actor_framework.PID
}

type EpochTimeout struct {
	Round int
}

func init() {
	actor_framework.RegisterMessageTip(StartTrainingEpoch{})
	actor_framework.RegisterMessageTip(GlobalModelBroadcast{})
	actor_framework.RegisterMessageTip(CalculateLocalMetrics{})
	actor_framework.RegisterMessageTip(LocalMetricsUpdate{})
	actor_framework.RegisterMessageTip(RegisterTrainer{})
	actor_framework.RegisterMessageTip(EpochTimeout{})
}

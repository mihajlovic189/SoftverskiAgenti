package federated_learning

import (
	"agent-project/actor_framework"
	"agent-project/pb"
)

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

type RegisterAck struct {
	Accepted bool
}

type EpochTimeout struct {
	Round int
}

func init() {
	actor_framework.RegisterMessageCodec(
		func(m StartTrainingEpoch) *pb.StartTrainingEpoch {
			return &pb.StartTrainingEpoch{Round: int32(m.Round), TotalRounds: int32(m.TotalRounds)}
		},
		func(p *pb.StartTrainingEpoch) StartTrainingEpoch {
			return StartTrainingEpoch{Round: int(p.Round), TotalRounds: int(p.TotalRounds)}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m GlobalModelBroadcast) *pb.GlobalModelBroadcast {
			return &pb.GlobalModelBroadcast{GlobalAverage: m.GlobalAverage, Round: int32(m.Round)}
		},
		func(p *pb.GlobalModelBroadcast) GlobalModelBroadcast {
			return GlobalModelBroadcast{GlobalAverage: p.GlobalAverage, Round: int(p.Round)}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m CalculateLocalMetrics) *pb.CalculateLocalMetrics {
			return &pb.CalculateLocalMetrics{FilePath: m.FilePath, Round: int32(m.Round), TotalRounds: int32(m.TotalRounds)}
		},
		func(p *pb.CalculateLocalMetrics) CalculateLocalMetrics {
			return CalculateLocalMetrics{FilePath: p.FilePath, Round: int(p.Round), TotalRounds: int(p.TotalRounds)}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m LocalMetricsUpdate) *pb.LocalMetricsUpdate {
			return &pb.LocalMetricsUpdate{Average: m.Average, Count: int32(m.Count), Round: int32(m.Round)}
		},
		func(p *pb.LocalMetricsUpdate) LocalMetricsUpdate {
			return LocalMetricsUpdate{Average: p.Average, Count: int(p.Count), Round: int(p.Round)}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m RegisterTrainer) *pb.RegisterTrainer {
			return &pb.RegisterTrainer{TrainerPid: actor_framework.PidToProto(m.TrainerPID)}
		},
		func(p *pb.RegisterTrainer) RegisterTrainer {
			return RegisterTrainer{TrainerPID: actor_framework.PidFromProto(p.TrainerPid)}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m RegisterAck) *pb.RegisterAck {
			return &pb.RegisterAck{Accepted: m.Accepted}
		},
		func(p *pb.RegisterAck) RegisterAck {
			return RegisterAck{Accepted: p.Accepted}
		},
	)

	actor_framework.RegisterMessageCodec(
		func(m EpochTimeout) *pb.EpochTimeout {
			return &pb.EpochTimeout{Round: int32(m.Round)}
		},
		func(p *pb.EpochTimeout) EpochTimeout {
			return EpochTimeout{Round: int(p.Round)}
		},
	)
}

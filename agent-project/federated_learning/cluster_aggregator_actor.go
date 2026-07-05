package federated_learning

import (
	"agent-project/actor_framework"
	"fmt"
	"time"
)

type ClusterAggregatorActor struct {
	trainers          []*actor_framework.PID
	responsesReceived int
	accumulatedSum    float64
	accumulatedCount  int
	epochActive       bool
	currentRound      int
	maxRounds         int
}

func NewClusterAggregatorActor() actor_framework.Actor {
	return &ClusterAggregatorActor{
		trainers:          make([]*actor_framework.PID, 0),
		responsesReceived: 0,
		accumulatedSum:    0,
		accumulatedCount:  0,
		epochActive:       false,
		currentRound:      1,
		maxRounds:         3,
	}
}

func (a *ClusterAggregatorActor) Receive(ctx actor_framework.Context) {
	switch msg := ctx.Message().(type) {

	case RegisterTrainer:
		if a.currentRound > 1 || a.epochActive {
			return
		}

		a.trainers = append(a.trainers, msg.TrainerPID)
		fmt.Printf("[Aggregator] Registrovan novi trener: %s (Ukupno: %d/4)\n", msg.TrainerPID.String(), len(a.trainers))

		if len(a.trainers) == 4 {
			fmt.Println("\n========================================================")
			fmt.Printf("[Aggregator] SVI ČVOROVI PRISUTNI (4/4)! Pokrećem Federated Learning proces...\n")
			fmt.Println("========================================================")

			a.startRound(ctx, 1)
		}

	case StartTrainingEpoch:
		if msg.Round > 1 {
			a.startRound(ctx, msg.Round)
		}

	case LocalMetricsUpdate:
		if !a.epochActive || msg.Round != a.currentRound {
			fmt.Printf("[Aggregator] ZANEMARUJEM zakasnele podatke od trenera za RUNDU %d (Trenutna aktivna runda: %d, Aktivna: %t)\n",
				msg.Round, a.currentRound, a.epochActive)
			return
		}

		a.responsesReceived++
		a.accumulatedSum += msg.Average * float64(msg.Count)
		a.accumulatedCount += msg.Count

		fmt.Printf("[Aggregator] Stigli podaci za RUNDU %d od trenera. Progres: (%d/%d)\n",
			a.currentRound, a.responsesReceived, len(a.trainers))

		if a.responsesReceived == len(a.trainers) {
			a.epochActive = false
			a.finishRound(ctx, "SVI ČVOROVI ODGOVORILI")
		}

	case EpochTimeout:
		if msg.Round != a.currentRound || !a.epochActive {
			return
		}

		a.epochActive = false
		a.finishRound(ctx, "ISTEKLO VREME (TIMEOUT)")
	}
}

func (a *ClusterAggregatorActor) startRound(ctx actor_framework.Context, round int) {
	a.responsesReceived = 0
	a.accumulatedSum = 0
	a.accumulatedCount = 0
	a.epochActive = true
	a.currentRound = round

	fmt.Printf("\n>>> [Aggregator] ZAPOČINJE RUNDA %d od %d <<<\n", a.currentRound, a.maxRounds)

	selfPID := ctx.Self()
	time.AfterFunc(10*time.Second, func() {
		ctx.Send(selfPID, EpochTimeout{Round: round})
	})

	for _, trainerPID := range a.trainers {
		ctx.Send(trainerPID, StartTrainingEpoch{Round: round})
	}
}

func (a *ClusterAggregatorActor) finishRound(ctx actor_framework.Context, status string) {
	a.epochActive = false

	globalAverage := 0.0
	if a.accumulatedCount > 0 {
		globalAverage = a.accumulatedSum / float64(a.accumulatedCount)
	}

	fmt.Println("\n========================================================")
	fmt.Printf("[Aggregator] RUNDA %d ZAVRŠENA! Status: %s\n", a.currentRound, status)
	fmt.Printf("-> Uspešno odgovorilo čvorova: %d od 4\n", a.responsesReceived)
	fmt.Printf("-> Ukupno uzoraka u ovoj rundi: %d\n", a.accumulatedCount)
	fmt.Printf("-> GLOBALNI PONDERISANI PROSEK ZA RUNDU %d: %.4f kWh\n", a.currentRound, globalAverage)
	fmt.Println("========================================================")

	for _, trainerPID := range a.trainers {
		ctx.Send(trainerPID, GlobalModelBroadcast{
			GlobalAverage: globalAverage,
			Round:         a.currentRound,
		})
	}

	if a.currentRound < a.maxRounds {
		nextRound := a.currentRound + 1

		fmt.Printf("[Aggregator] Pravim pauzu od 1 sekunde da mreža isporuči novi model pre RUNDE %d...\n", nextRound)

		time.AfterFunc(1*time.Second, func() {
			a.startRound(ctx, nextRound)
		})

	} else {
		fmt.Println("\n!!!!! FEDERATED LEARNING PROCES UKUPNO ZAVRŠEN ZA SVE RUNDE !!!!!")
		fmt.Println("Sistem je stabilan. Možete ugasiti klaster sa Ctrl+C.\n")

		filePaths := []string{"data/node1.json", "data/node2.json", "data/node3.json", "data/node4.json"}
		EvaluateDistributedResult(globalAverage, filePaths)
	}
}

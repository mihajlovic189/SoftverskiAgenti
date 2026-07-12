package federated_learning

import (
	"agent-project/actor_framework"
	"fmt"
	"time"
)

type AggregatorActor struct {
	trainers          []*actor_framework.PID
	responsesReceived int
	accumulatedSum    float64
	accumulatedCount  int
	epochActive       bool
	currentRound      int
	maxRounds         int
	expectedNodes     int
	started           bool
	statePath         string
	globalAverages    map[int]float64
	lastGlobalAverage float64
}

func NewAggregatorActor(expectedNodes int, statePath string) *AggregatorActor {
	a := &AggregatorActor{
		trainers:          make([]*actor_framework.PID, 0),
		responsesReceived: 0,
		accumulatedSum:    0,
		accumulatedCount:  0,
		epochActive:       false,
		currentRound:      1,
		maxRounds:         3,
		expectedNodes:     expectedNodes,
		statePath:         statePath,
		globalAverages:    make(map[int]float64),
	}

	if state, err := LoadState(statePath); err == nil && state.LastCompletedRound > 0 {
		a.currentRound = state.LastCompletedRound + 1
		if state.GlobalAverages != nil {
			a.globalAverages = state.GlobalAverages
		}
		fmt.Printf("[Aggregator] Pronađeno sačuvano stanje: poslednja završena runda %d. Nastavljam od runde %d.\n", state.LastCompletedRound, a.currentRound)
	}

	return a
}

func (a *AggregatorActor) Receive(ctx actor_framework.Context) {
	switch msg := ctx.Message().(type) {

	case actor_framework.Started:
		fmt.Printf("[Aggregator] Aktor kreiran (Started), čekam registraciju %d trenera...\n", a.expectedNodes)

	case RegisterTrainer:
		a.handleRegisterTrainer(ctx, msg)

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

func (a *AggregatorActor) handleRegisterTrainer(ctx actor_framework.Context, msg RegisterTrainer) {
	for _, known := range a.trainers {
		if known.String() == msg.TrainerPID.String() {
			ctx.Send(ctx.Sender(), RegisterAck{Accepted: true})
			return
		}
	}

	if a.started {
		fmt.Printf("[Aggregator] Odbijam kasnu registraciju trenera %s - trening je već u toku.\n", msg.TrainerPID.String())
		ctx.Send(ctx.Sender(), RegisterAck{Accepted: false})
		return
	}

	a.trainers = append(a.trainers, msg.TrainerPID)
	fmt.Printf("[Aggregator] Registrovan novi trener: %s (Ukupno: %d/%d)\n", msg.TrainerPID.String(), len(a.trainers), a.expectedNodes)
	ctx.Send(ctx.Sender(), RegisterAck{Accepted: true})

	if len(a.trainers) == a.expectedNodes {
		a.started = true

		if a.currentRound > a.maxRounds {
			fmt.Printf("[Aggregator] Sve runde su već ranije završene (obnovljeno stanje). Nema novog treninga.\n")
			return
		}

		fmt.Printf("[Aggregator] SVI ČVOROVI PRISUTNI (%d/%d)! Pokrećem Federated Learning proces od runde %d...\n", len(a.trainers), a.expectedNodes, a.currentRound)
		a.startRound(ctx, a.currentRound)
	}
}

func (a *AggregatorActor) startRound(ctx actor_framework.Context, round int) {
	a.responsesReceived = 0
	a.accumulatedSum = 0
	a.accumulatedCount = 0
	a.epochActive = true
	a.currentRound = round

	fmt.Printf("\n[Aggregator] ZAPOČINJE RUNDA %d od %d \n", a.currentRound, a.maxRounds)

	selfPID := ctx.Self()
	time.AfterFunc(18*time.Second, func() {
		ctx.Send(selfPID, EpochTimeout{Round: round})
	})

	for _, trainerPID := range a.trainers {
		ctx.Send(trainerPID, StartTrainingEpoch{Round: round, TotalRounds: a.maxRounds})
	}
}

func (a *AggregatorActor) finishRound(ctx actor_framework.Context, status string) {
	a.epochActive = false

	globalAverage := computeGlobalAverage(a.accumulatedSum, a.accumulatedCount)
	a.lastGlobalAverage = globalAverage
	a.globalAverages[a.currentRound] = globalAverage

	fmt.Printf("[Aggregator] Runda %d završena! Status: %s\n", a.currentRound, status)
	fmt.Printf("Uspešno odgovorilo čvorova: %d od %d\n", a.responsesReceived, a.expectedNodes)
	fmt.Printf("Ukupno uzoraka u ovoj rundi: %d\n", a.accumulatedCount)
	fmt.Printf("Globalni ponderisani prosek za rundu %d: %.4f kWh\n", a.currentRound, globalAverage)

	a.saveState()

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
		fmt.Println("\n!!!!! Federated learning proces ukupno završen za sve runde !!!!!")
		fmt.Println("Sistem je stabilan. Možete ugasiti klaster sa Ctrl+C.")

		filePaths := make([]string, 0, a.expectedNodes)
		for i := 1; i <= a.expectedNodes; i++ {
			filePaths = append(filePaths, fmt.Sprintf("data/node%d.json", i))
		}
		EvaluateDistributedResult(globalAverage, filePaths)
	}
}

func computeGlobalAverage(sum float64, count int) float64 {
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func (a *AggregatorActor) saveState() {
	state := AggregatorState{
		LastCompletedRound: a.currentRound,
		GlobalAverages:     a.globalAverages,
	}
	if err := SaveState(a.statePath, state); err != nil {
		fmt.Printf("[Aggregator] Upozorenje: nisam uspeo da sačuvam stanje na disk: %v\n", err)
	}
}

package federated_learning

import (
	"agent-project/actor_framework"
	"fmt"
	"log"
	"time"
)

type WorkerTimeout struct {
	Round      int
	Generation int
}

type FederatedTrainerActor struct {
	filePath          string
	workerPID         *actor_framework.PID
	currentAggregator *actor_framework.PID
	localModelWeight  float64
	currentRound      int
	workerGeneration  int
	retryCount        int
}

func NewFederatedTrainerActor(filePath string) actor_framework.Actor {
	return &FederatedTrainerActor{
		filePath:         filePath,
		localModelWeight: 0.0,
		currentRound:     0,
		workerGeneration: 0,
		retryCount:       0,
	}
}

func (a *FederatedTrainerActor) Receive(ctx actor_framework.Context) {
	switch msg := ctx.Message().(type) {

	case StartTrainingEpoch:
		if msg.Round <= 0 {
			return
		}

		a.currentRound = msg.Round
		a.currentAggregator = ctx.Sender()
		a.retryCount = 0

		log.Printf("[Trainer %s] Započinje RUNDU %d. Aktiviram aktivni nadzor radnika...\n", ctx.Self().ID, msg.Round)
		a.startWorkerWithSupervision(ctx)

	case LocalMetricsUpdate:
		if ctx.Sender() != nil && a.workerPID != nil && ctx.Sender().String() == a.workerPID.String() {

			log.Printf("[Trainer %s] Radnik uspešno završio u roku. Gasim nadzorni tajmer.\n", ctx.Self().ID)
			a.workerGeneration++

			if a.currentAggregator == nil {
				log.Printf("[Trainer %s] Greška: Nemam adresu agregatora!\n", ctx.Self().ID)
				return
			}

			finalAverageToSend := msg.Average

			if msg.Round > 1 {
				if a.localModelWeight > 0.0 {
					oldWeight := a.localModelWeight
					finalAverageToSend = (oldWeight + msg.Average) / 2.0
					log.Printf("[Trainer %s] FL KOREKCIJA (Runda %d): Kombinujem stari model (%.2f) i novi batch (%.2f) -> %.2f\n",
						ctx.Self().ID, msg.Round, oldWeight, msg.Average, finalAverageToSend)
				}
			}

			a.localModelWeight = finalAverageToSend

			log.Printf("[Trainer %s] Šaljem korigovane podatke AGREGATORU za RUNDU %d\n", ctx.Self().ID, msg.Round)
			ctx.Send(a.currentAggregator, LocalMetricsUpdate{
				Average: finalAverageToSend,
				Count:   msg.Count,
				Round:   msg.Round,
			})
		}

	case GlobalModelBroadcast:
		if msg.Round >= a.currentRound {
			a.localModelWeight = msg.GlobalAverage
			fmt.Println("--------------------------------------------------------")
			log.Printf("[Trainer %s] Lokalni model ažuriran globalnim prosekom: %.4f kWh\n", ctx.Self().ID, a.localModelWeight)
			fmt.Println("--------------------------------------------------------")
		}

	case WorkerTimeout:
		if msg.Round == a.currentRound && msg.Generation == a.workerGeneration {
			a.workerGeneration++
			a.retryCount++

			log.Printf("[Trainer %s] ⚠️ TIMEOUT (Pokušaj %d/3) u rundi %d!\n", ctx.Self().ID, a.retryCount, msg.Round)

			if a.retryCount >= 3 {
				log.Printf("[Trainer %s] ❌ SUPERVIZOR ODUSTAO: Radnik je zakazao 3 puta za redom. Šaljem fallback (0.0) agregatoru da ne blokiram sistem.\n", ctx.Self().ID)

				if a.currentAggregator != nil {
					ctx.Send(a.currentAggregator, LocalMetricsUpdate{
						Average: a.localModelWeight,
						Count:   0,
						Round:   a.currentRound,
					})
				}
				return
			}

			log.Printf("[Trainer %s] Supervizor: Pokušavam ponovo podizanje radnika...\n", ctx.Self().ID)
			a.startWorkerWithSupervision(ctx)
		}

	default:
		log.Printf("[Trainer %s] Primio nepoznatu poruku: %T", ctx.Self().ID, msg)
	}
}

func (a *FederatedTrainerActor) startWorkerWithSupervision(ctx actor_framework.Context) {
	workerProps := actor_framework.NewProps(func() actor_framework.Actor {
		return &DataWorkerActor{}
	})

	a.workerPID = ctx.Spawn(workerProps)
	currentGen := a.workerGeneration

	ctx.Send(a.workerPID, CalculateLocalMetrics{
		FilePath: a.filePath,
		Round:    a.currentRound,
	})

	selfPID := ctx.Self()
	currentRound := a.currentRound

	time.AfterFunc(5*time.Second, func() {
		ctx.Send(selfPID, WorkerTimeout{
			Round:      currentRound,
			Generation: currentGen,
		})
	})
}

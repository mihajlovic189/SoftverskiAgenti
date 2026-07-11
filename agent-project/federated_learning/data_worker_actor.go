package federated_learning

import (
	"agent-project/actor_framework"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type EnergyReading struct {
	Timestamp      string  `json:"timestamp"`
	ConsumptionKwh float64 `json:"consumption_kwh"`
}

type SmartMeterData struct {
	NodeID   string          `json:"node_id"`
	Readings []EnergyReading `json:"readings"`
}

type DataWorkerActor struct{}

func (a *DataWorkerActor) Receive(ctx actor_framework.Context) {
	switch msg := ctx.Message().(type) {

	case CalculateLocalMetrics:
		fmt.Printf("[DataWorker %s] Pokrećem računanje za fajl: %s\n", ctx.Self().ID, msg.FilePath)

		// Simulacija zastoja/kašnjenja čvora
		if msg.FilePath == "data/node1.json" {
			fmt.Printf("[DataWorker %s] !!! SIMULACIJA !!! Spavam 12 sekundi da bih testirao timeout supervizije...\n", ctx.Self().ID)
			time.Sleep(12 * time.Second)
		}

		fileData, err := os.ReadFile(msg.FilePath)
		if err != nil {
			fmt.Printf("[DataWorker %s] Greška pri čitanju fajla: %v\n", ctx.Self().ID, err)
			return
		}

		var smd SmartMeterData
		if err := json.Unmarshal(fileData, &smd); err != nil {
			fmt.Printf("[DataWorker %s] Greška pri parsiranju JSON-a: %v\n", ctx.Self().ID, err)
			return
		}

		totalReadings := len(smd.Readings)
		if totalReadings == 0 {
			ctx.Send(ctx.Sender(), LocalMetricsUpdate{Average: 0, Count: 0, Round: msg.Round})
			return
		}

		pageSize := totalReadings / msg.TotalRounds
		if pageSize == 0 {
			pageSize = 1
		}

		startIdx := (msg.Round - 1) * pageSize
		endIdx := startIdx + pageSize

		if msg.Round == msg.TotalRounds || endIdx > totalReadings {
			endIdx = totalReadings
		}

		if startIdx >= totalReadings {
			startIdx = totalReadings - 1
		}
		if startIdx < 0 {
			startIdx = 0
		}

		batch := smd.Readings[startIdx:endIdx]
		count := len(batch)

		sum := 0.0
		for _, val := range batch {
			sum += val.ConsumptionKwh
		}

		avg := 0.0
		if count > 0 {
			avg = sum / float64(count)
		}

		fmt.Printf("[DataWorker %s] Batch završen -> Prosek: %.2f (Uzoraka: %d)\n", ctx.Self().ID, avg, count)

		ctx.Send(ctx.Sender(), LocalMetricsUpdate{
			Average: avg,
			Count:   count,
			Round:   msg.Round,
		})
	}
}

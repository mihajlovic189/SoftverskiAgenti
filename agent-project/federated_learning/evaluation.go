package federated_learning

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
)

func EvaluateDistributedResult(finalGlobalAverage float64, filePaths []string) {
	log.Println("\n--- Započinjem centralizovanu evaluaciju rezultata ---")

	var totalSum float64
	var totalCount int

	for _, path := range filePaths {
		fileData, err := os.ReadFile(path)
		if err != nil {
			log.Printf("[Evaluacija] Greška pri čitanju fajla %s: %v\n", path, err)
			continue
		}

		var smd SmartMeterData
		if err := json.Unmarshal(fileData, &smd); err != nil {
			log.Printf("[Evaluacija] Greška pri parsiranju fajla %s: %v\n", path, err)
			continue
		}

		for _, reading := range smd.Readings {
			totalSum += reading.ConsumptionKwh
			totalCount++
		}
	}

	if totalCount == 0 {
		log.Println("[Evaluacija] Greška: Nema sirovih podataka za evaluaciju!")
		return
	}

	controlAverage := totalSum / float64(totalCount)

	absoluteError := math.Abs(controlAverage - finalGlobalAverage)

	accuracyPercentage := 100.0
	if controlAverage > 0 {
		accuracyPercentage = (1.0 - (absoluteError / controlAverage)) * 100.0
	}

	fmt.Println("Rezultati evaluacije proračuna")
	log.Printf("Dobijeni Distribuirani Globalni Prosek: %.4f kWh\n", finalGlobalAverage)
	log.Printf("Kontrolni Centralizovani Prosek (Sirovo): %.4f kWh\n", controlAverage)
	log.Printf("Apsolutno Odstupanje (Greška): %.4f kWh\n", absoluteError)

	if accuracyPercentage >= 95.0 {
		log.Printf("Tačnost distribuiranog modela: %.2f%% (USPEH)\n", accuracyPercentage)
	} else {
		log.Printf("Tačnost distribuiranog modela: %.2f%% (Odstupanje zbog simulacije ispada/timeout-a)\n", accuracyPercentage)
	}
}

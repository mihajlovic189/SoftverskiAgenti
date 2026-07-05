package main

import (
	"agent-project/actor_framework"
	"agent-project/federated_learning"
	"flag"
	"fmt"
)

func main() {
	role := flag.String("role", "", "Uloga u sistemu: 'aggregator' ili 'trainer'")
	nodeFile := flag.String("file", "", "Putanja do JSON fajla (samo za trainer)")

	listenAddr := flag.String("listen-addr", "0.0.0.0:8080", "Adresa na kojoj mrežni server sluša")
	publicAddr := flag.String("public-addr", "localhost:8080", "Javna adresa koju drugi čvorovi koriste da nas kontaktiraju")

	aggAddress := flag.String("agg-addr", "localhost:8080", "Mrežna adresa agregatora")
	flag.Parse()

	if *role == "" {
		fmt.Println("Greška: Morate navesti ulogu kroz parametar --role.")
		return
	}

	const fixedAggregatorID = "central-aggregator"

	if *role == "aggregator" {
		fmt.Printf("--- Pokretanje Čvora: CENTRALNI AGREGATOR ---\n")
		fmt.Printf("-> Slušam na: %s, Identifikujem se kao: %s\n", *listenAddr, *publicAddr)

		system := actor_framework.NewActorSystem(*publicAddr)

		props := actor_framework.NewProps(func() actor_framework.Actor {
			return federated_learning.NewClusterAggregatorActor()
		})

		aggregatorPID := system.SpawnNamed(props, fixedAggregatorID)
		fmt.Printf("[Aggregator] Podignut sa PID-om: %s\n", aggregatorPID.String())

		err := system.StartListeningOn(*listenAddr)
		if err != nil {
			panic(err)
		}

		select {}
	}

	if *role == "trainer" {
		if *nodeFile == "" {
			fmt.Println("Greška: Za 'trainer' ulogu morate uneti i --file parametar")
			return
		}

		fmt.Printf("--- Pokretanje Čvora: TRAINER (%s) ---\n", *publicAddr)

		system := actor_framework.NewActorSystem(*publicAddr)

		props := actor_framework.NewProps(func() actor_framework.Actor {
			return federated_learning.NewFederatedTrainerActor(*nodeFile)
		})
		trainerPID := system.Spawn(props)

		err := system.StartListeningOn(*listenAddr)
		if err != nil {
			panic(err)
		}

		aggregatorPID := actor_framework.NewPID(*aggAddress, fixedAggregatorID)

		fmt.Printf("[Trainer] Automatska registracija na agregator: %s\n", aggregatorPID.String())
		system.Send(aggregatorPID, federated_learning.RegisterTrainer{TrainerPID: trainerPID}, nil)

		select {}
	}
}

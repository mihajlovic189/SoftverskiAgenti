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

	expectedNodes := flag.Int("expected-nodes", 4, "Broj trenera koje agregator čeka pre pokretanja treninga (samo za aggregator)")
	stateFile := flag.String("state-file", "aggregator_state.json", "Putanja gde agregator čuva stanje runde radi oporavka posle pada (samo za aggregator)")
	flag.Parse()

	if *role == "" {
		fmt.Println("Greška: Morate navesti ulogu kroz parametar --role.")
		return
	}

	const fixedAggregatorID = "central-aggregator"

	if *role == "aggregator" {
		fmt.Printf("Pokretanje Čvora: CENTRALNI AGREGATOR\n")
		fmt.Printf("-> Slušam na: %s, Identifikujem se kao: %s\n", *listenAddr, *publicAddr)

		system := actor_framework.NewActorSystem(*publicAddr)

		aggActor := federated_learning.NewAggregatorActor(*expectedNodes, *stateFile)

		props := actor_framework.NewProps(func() actor_framework.Actor {
			return aggActor
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

		fmt.Printf("Pokretanje Čvora: TRAINER (%s)\n", *publicAddr)

		system := actor_framework.NewActorSystem(*publicAddr)

		aggregatorPID := actor_framework.NewPID(*aggAddress, fixedAggregatorID)

		props := actor_framework.NewProps(func() actor_framework.Actor {
			return federated_learning.NewFederatedTrainerActor(*nodeFile, aggregatorPID)
		})
		system.Spawn(props)

		err := system.StartListeningOn(*listenAddr)
		if err != nil {
			panic(err)
		}

		fmt.Printf("[Trainer] Pokrenut. Aktor se sam registruje (i periodično re-registruje) na agregatoru %s preko lifecycle Started poruke - otporno na restart agregatora.\n", aggregatorPID.String())

		select {}
	}
}

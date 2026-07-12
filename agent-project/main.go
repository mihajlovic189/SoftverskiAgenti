package main

import (
	"agent-project/actor_framework"
	"agent-project/federated_learning"
	"flag"
	"fmt"
	"strings"
)

func main() {
	role := flag.String("role", "", "Uloga u sistemu: 'aggregator' (kandidat za hostovanje Aggregator grain-a) ili 'trainer' (klijent klastera)")
	nodeFile := flag.String("file", "", "Putanja do JSON fajla (samo za trainer)")

	listenAddr := flag.String("listen-addr", "0.0.0.0:8080", "Adresa na kojoj mrežni server sluša")
	publicAddr := flag.String("public-addr", "localhost:8080", "Javna adresa koju drugi čvorovi koriste da nas kontaktiraju")

	seedsFlag := flag.String("seeds", "", "Zarezom razdvojena lista adresa svih 'aggregator' čvorova (kandidata za Aggregator grain) - isti spisak na svim čvorovima")

	expectedNodes := flag.Int("expected-nodes", 4, "Broj trenera koje agregator čeka pre pokretanja treninga (samo za aggregator)")
	stateFile := flag.String("state-file", "aggregator_state.json", "Putanja gde agregator čuva stanje runde radi oporavka posle pada (samo za aggregator)")
	flag.Parse()

	if *role == "" {
		fmt.Println("Greška: Morate navesti ulogu kroz parametar --role.")
		return
	}

	if *seedsFlag == "" {
		fmt.Println("Greška: Morate navesti --seeds (spisak adresa svih 'aggregator' kandidata, uklj. sebe ako je uloga 'aggregator').")
		return
	}
	seeds := strings.Split(*seedsFlag, ",")

	if *role == "aggregator" {
		fmt.Printf("Pokretanje Čvora: KANDIDAT ZA AGREGATORA\n")
		fmt.Printf("-> Slušam na: %s, Identifikujem se kao: %s\n", *listenAddr, *publicAddr)
		fmt.Printf("-> Kandidati za 'Aggregator' grain: %v\n", seeds)

		system := actor_framework.NewActorSystem(*publicAddr)

		err := system.StartListeningOn(*listenAddr)
		if err != nil {
			panic(err)
		}

		cluster := actor_framework.NewCluster(system, *publicAddr, seeds)

		aggProps := actor_framework.NewProps(func() actor_framework.Actor {
			return federated_learning.NewAggregatorActor(*expectedNodes, *stateFile)
		})
		cluster.RegisterKind(federated_learning.AggregatorKind, aggProps)

		cluster.StartMember(seeds)

		fmt.Printf("[Cluster] Član klastera. 'Aggregator' grain će se aktivirati lenjo (lazy) na čvoru koji trenutno vlasništvo dodeli preko konzistentnog heš-a nad živim kandidatima.\n")

		select {}
	}

	if *role == "trainer" {
		if *nodeFile == "" {
			fmt.Println("Greška: Za 'trainer' ulogu morate uneti i --file parametar")
			return
		}

		fmt.Printf("Pokretanje Čvora: TRAINER (%s)\n", *publicAddr)
		fmt.Printf("-> Kandidati za 'Aggregator' grain: %v\n", seeds)

		system := actor_framework.NewActorSystem(*publicAddr)

		err := system.StartListeningOn(*listenAddr)
		if err != nil {
			panic(err)
		}

		cluster := actor_framework.NewCluster(system, *publicAddr, seeds)
		cluster.StartClient(seeds)

		props := actor_framework.NewProps(func() actor_framework.Actor {
			return federated_learning.NewFederatedTrainerActor(*nodeFile, cluster)
		})
		system.Spawn(props)

		fmt.Printf("[Trainer] Pokrenut. Aktor sam razrešava trenutnog vlasnika Aggregator grain-a preko klastera i periodično se re-registruje - otporno i na restart procesa i na pad fizičkog čvora koji ga hostuje.\n")

		select {}
	}
}

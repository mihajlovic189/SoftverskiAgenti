package actor_framework

import (
	"context"
	"fmt"
	"net"
	"time"

	"agent-project/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type transportServer struct {
	pb.UnimplementedActorTransportServer
	system *ActorSystem
}

func (s *transportServer) Send(ctx context.Context, env *pb.Envelope) (*pb.Ack, error) {
	msg, err := DecodeMessage(env.Payload)
	if err != nil {
		fmt.Printf("[NetworkServer] Greška pri dekodiranju poruke: %v\n", err)
		return &pb.Ack{Ok: false, Error: err.Error()}, nil
	}

	s.system.localSend(PidFromProto(env.Target), msg, PidFromProto(env.Sender))
	return &pb.Ack{Ok: true}, nil
}

func (s *ActorSystem) StartListeningOn(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterActorTransportServer(grpcServer, &transportServer{system: s})

	fmt.Printf("[NetworkServer] ActorSystem sluša (gRPC) na lokalnoj adresi: %s\n", addr)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			fmt.Printf("[NetworkServer] gRPC server je stao: %v\n", err)
		}
	}()

	return nil
}

func (s *ActorSystem) getOrDialClient(address string) (pb.ActorTransportClient, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if client, ok := s.connections[address]; ok {
		return client, nil
	}

	fmt.Printf("[NetworkClient] Otvaram novu gRPC konekciju ka %s...\n", address)

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewActorTransportClient(conn)
	s.connections[address] = client
	return client, nil
}

func (s *ActorSystem) sendRemote(targetPID *PID, message interface{}, sender *PID) {
	client, err := s.getOrDialClient(targetPID.Address)
	if err != nil {
		fmt.Printf("[NetworkClient] Greška pri povezivanju na %s: %v\n", targetPID.Address, err)
		return
	}

	payload, err := EncodeMessage(message)
	if err != nil {
		fmt.Printf("[NetworkClient] Greška pri serijalizaciji poruke %T: %v\n", message, err)
		return
	}

	env := &pb.Envelope{
		Target:  PidToProto(targetPID),
		Sender:  PidToProto(sender),
		Payload: payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := client.Send(ctx, env)
	if err != nil {
		fmt.Printf("[NetworkClient] Greška pri slanju poruke ka %s: %v\n", targetPID.Address, err)
		return
	}
	if !ack.Ok {
		fmt.Printf("[NetworkClient] Udaljeni sistem je odbio poruku ka %s: %s\n", targetPID.Address, ack.Error)
	}
}

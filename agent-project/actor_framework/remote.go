package actor_framework

import (
	"encoding/gob"
	"fmt"
	"net"
)

type NetworkMessage struct {
	TargetPID *PID
	SenderPID *PID
	Payload   interface{}
}

func RegisterMessageTip(value interface{}) {
	gob.Register(value)
}

func init() {
	gob.Register(PID{})
	gob.Register(NetworkMessage{})
	gob.Register(Failure{})
	gob.Register(ChildStarted{})
	gob.Register(Restart{})
	gob.Register(Stop{})
}

func (s *ActorSystem) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)

	for {
		var netMsg NetworkMessage
		err := decoder.Decode(&netMsg)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("[NetworkServer] Greška pri dekodiranju poruke: %v\n", err)
			}
			break
		}

		s.localSend(netMsg.TargetPID, netMsg.Payload, netMsg.SenderPID)
	}
}

func (s *ActorSystem) sendRemote(targetPID *PID, message interface{}, sender *PID) {
	s.connMu.Lock()
	client, exists := s.connections[targetPID.Address]
	var err error

	if !exists {
		fmt.Printf("[NetworkClient] Otvaram novu TCP konekciju ka %s...\n", targetPID.Address)
		conn, err := net.Dial("tcp", targetPID.Address)
		if err != nil {
			fmt.Printf("[NetworkClient] Greška pri povezivanju na %s: %v\n", targetPID.Address, err)
			s.connMu.Unlock()
			return
		}

		encoder := gob.NewEncoder(conn)

		client = &remoteClient{
			conn:    conn,
			encoder: encoder,
		}

		s.connections[targetPID.Address] = client
	}
	s.connMu.Unlock()

	netMsg := NetworkMessage{
		TargetPID: targetPID,
		SenderPID: sender,
		Payload:   message,
	}

	err = client.encoder.Encode(netMsg)
	if err != nil {
		fmt.Printf("[NetworkClient] Greška pri slanju poruke ka %s: %v\n", targetPID.Address, err)

		s.connMu.Lock()
		client.conn.Close()
		delete(s.connections, targetPID.Address)
		s.connMu.Unlock()
	}
}

func (s *ActorSystem) StartListeningOn(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("[NetworkServer] ActorSystem sluša na lokalnoj adresi: %s\n", addr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Printf("[NetworkServer] Greška pri prihvatanju konekcije: %v\n", err)
				continue
			}
			go s.handleConnection(conn)
		}
	}()

	return nil
}

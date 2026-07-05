package actor_framework

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type ActorSystem struct {
	address  string
	registry map[string]ActorRef
	mu       sync.RWMutex

	connections map[string]*remoteClient
	connMu      sync.Mutex
}

func NewActorSystem(address string) *ActorSystem {
	return &ActorSystem{
		address:     address,
		registry:    make(map[string]ActorRef),
		connections: make(map[string]*remoteClient),
	}
}

func (s *ActorSystem) Address() string {
	return s.address
}

func (s *ActorSystem) Spawn(props *Props) *PID {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.New().String()
	pid := NewPID(s.address, id)

	proc := newProcess(pid, props, s)
	s.registry[id] = proc

	proc.start()

	return pid
}

func (s *ActorSystem) localSend(pid *PID, message interface{}, sender *PID) {
	s.mu.RLock()
	ref, ok := s.registry[pid.ID]
	s.mu.RUnlock()

	if ok {
		ref.Send(message, sender)
	} else {
		fmt.Printf("[ActorSystem] Greška: Lokalni aktor %s nije pronađen!\n", pid.ID)
	}
}

func (s *ActorSystem) Send(pid *PID, message interface{}, sender *PID) {
	if pid.Address == s.address {
		s.localSend(pid, message, sender)
	} else {
		s.sendRemote(pid, message, sender)
	}
}

func (s *ActorSystem) SpawnNamed(props *Props, name string) *PID {
	s.mu.Lock()
	defer s.mu.Unlock()

	pid := NewPID(s.address, name)

	proc := newProcess(pid, props, s)
	s.registry[name] = proc

	proc.start()

	return pid
}

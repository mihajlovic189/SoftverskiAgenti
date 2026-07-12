package actor_framework

import (
	"fmt"
	"sync"

	"agent-project/pb"

	"github.com/google/uuid"
)

type ActorSystem struct {
	address  string
	registry map[string]ActorRef
	mu       sync.RWMutex

	connections map[string]pb.ActorTransportClient
	connMu      sync.Mutex
}

func NewActorSystem(address string) *ActorSystem {
	return &ActorSystem{
		address:     address,
		registry:    make(map[string]ActorRef),
		connections: make(map[string]pb.ActorTransportClient),
	}
}

func (s *ActorSystem) Address() string {
	return s.address
}

func (s *ActorSystem) Spawn(props *Props) *PID {
	return s.SpawnChild(props, nil)
}

func (s *ActorSystem) SpawnChild(props *Props, parent *PID) *PID {
	s.mu.Lock()

	id := uuid.New().String()
	pid := NewPID(s.address, id)
	pid.Parent = parent

	proc := newProcess(pid, props, s)
	s.registry[id] = proc

	proc.Send(Started{}, nil)
	proc.start()

	s.mu.Unlock()

	if parent != nil {
		s.Send(parent, ChildStarted{Child: pid}, pid)
	}

	return pid
}

func (s *ActorSystem) unregister(pid *PID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.registry, pid.ID)
}

func (s *ActorSystem) localSend(pid *PID, message interface{}, sender *PID) {
	s.mu.RLock()
	ref, ok := s.registry[pid.ID]
	s.mu.RUnlock()

	if ok {
		ref.Send(message, sender)
		return
	}

	fmt.Printf("[ActorSystem] Greška: Lokalni aktor %s nije pronađen! Poruka %T odbačena (dead letter).\n", pid.ID, message)

	if sender != nil {
		if _, isDeadLetter := message.(DeadLetter); !isDeadLetter {
			s.Send(sender, DeadLetter{Target: pid, Sender: sender, Original: message}, nil)
		}
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

	proc.Send(Started{}, nil)
	proc.start()

	return pid
}

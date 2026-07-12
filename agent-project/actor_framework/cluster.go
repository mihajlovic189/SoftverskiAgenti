package actor_framework

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"agent-project/pb"
)

type ClusterPing struct{ From string }
type ClusterPong struct{ From string }

func init() {
	RegisterMessageCodec(
		func(m ClusterPing) *pb.ClusterPing { return &pb.ClusterPing{From: m.From} },
		func(p *pb.ClusterPing) ClusterPing { return ClusterPing{From: p.From} },
	)
	RegisterMessageCodec(
		func(m ClusterPong) *pb.ClusterPong { return &pb.ClusterPong{From: m.From} },
		func(p *pb.ClusterPong) ClusterPong { return ClusterPong{From: p.From} },
	)
}

const (
	pingInterval = 2 * time.Second
	memberTTL    = 6 * time.Second
)

type Membership struct {
	self  string
	seeds []string

	mu    sync.RWMutex
	alive map[string]time.Time

	system *ActorSystem
}

func NewMembership(system *ActorSystem, self string, seeds []string) *Membership {
	return &Membership{
		self:   self,
		seeds:  seeds,
		alive:  map[string]time.Time{self: time.Now()},
		system: system,
	}
}

func (m *Membership) Start() {
	props := NewProps(func() Actor { return &membershipActor{m: m} })
	m.system.SpawnNamed(props, "cluster-membership")

	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for range ticker.C {
			m.tick()
		}
	}()
}

func (m *Membership) tick() {
	m.mu.Lock()
	m.alive[m.self] = time.Now()
	for addr, last := range m.alive {
		if addr != m.self && time.Since(last) > memberTTL {
			delete(m.alive, addr)
			fmt.Printf("[Cluster] Član %s se smatra MRTVIM (nema odgovora %v).\n", addr, memberTTL)
		}
	}
	m.mu.Unlock()

	self := NewPID(m.self, "cluster-membership")
	for _, seed := range m.seeds {
		if seed == m.self {
			continue
		}
		target := NewPID(seed, "cluster-membership")
		m.system.Send(target, ClusterPing{From: m.self}, self)
	}
}

func (m *Membership) markAlive(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, existed := m.alive[addr]; !existed {
		fmt.Printf("[Cluster] Član %s je JAVIO SE ŽIV.\n", addr)
	}
	m.alive[addr] = time.Now()
}

func (m *Membership) AliveAmong(candidates []string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, c := range candidates {
		if _, ok := m.alive[c]; ok {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

type membershipActor struct{ m *Membership }

func (a *membershipActor) Receive(ctx Context) {
	switch msg := ctx.Message().(type) {
	case ClusterPing:
		a.m.markAlive(msg.From)
		ctx.Send(ctx.Sender(), ClusterPong{From: a.m.self})
	case ClusterPong:
		a.m.markAlive(msg.From)
	}
}

type Kind struct {
	Name  string
	Props *Props
}

type Cluster struct {
	system     *ActorSystem
	membership *Membership
	self       string
	candidates []string

	mu        sync.Mutex
	kinds     map[string]*Kind
	activated map[string]*PID
}

func NewCluster(system *ActorSystem, self string, candidates []string) *Cluster {
	return &Cluster{
		system:     system,
		self:       self,
		candidates: candidates,
		kinds:      make(map[string]*Kind),
		activated:  make(map[string]*PID),
	}
}

func (c *Cluster) RegisterKind(name string, props *Props) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kinds[name] = &Kind{Name: name, Props: props}
}

func (c *Cluster) StartMember(seeds []string) {
	c.membership = NewMembership(c.system, c.self, seeds)
	c.membership.Start()
	c.system.cluster = c
}

func (c *Cluster) StartClient(seeds []string) {
	c.membership = NewMembership(c.system, c.self, seeds)
	c.membership.Start()
}

func grainID(kind, identity string) string { return kind + "$" + identity }

func parseGrainID(pidID string) (kind string, ok bool) {
	i := strings.Index(pidID, "$")
	if i < 0 {
		return "", false
	}
	return pidID[:i], true
}

func ownerOf(identity string, alive []string) string {
	if len(alive) == 0 {
		return ""
	}
	h := fnv.New32a()
	h.Write([]byte(identity))
	idx := int(h.Sum32() % uint32(len(alive)))
	return alive[idx]
}

func (c *Cluster) Get(kind, identity string) *PID {
	alive := c.membership.AliveAmong(c.candidates)
	owner := ownerOf(identity, alive)
	if owner == "" {
		return nil
	}

	id := grainID(kind, identity)
	if owner != c.self {
		return NewPID(owner, id)
	}
	return c.activateLocally(kind, id)
}

func (c *Cluster) activateLocally(kind, id string) *PID {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pid, ok := c.activated[id]; ok {
		return pid
	}

	k, ok := c.kinds[kind]
	if !ok {
		return nil
	}

	pid := c.system.SpawnNamed(k.Props, id)
	c.activated[id] = pid
	fmt.Printf("[Cluster] Aktiviran grain '%s' na ovom čvoru (%s).\n", id, c.self)
	return pid
}

func (c *Cluster) activateOnArrival(pidID string) *PID {
	kind, ok := parseGrainID(pidID)
	if !ok {
		return nil
	}
	return c.activateLocally(kind, pidID)
}

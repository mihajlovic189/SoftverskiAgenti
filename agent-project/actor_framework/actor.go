package actor_framework

import (
	"encoding/gob"
	"fmt"
	"net"
)

type Actor interface {
	Receive(context Context)
}

type Context interface {
	Message() interface{}
	Sender() *PID
	Self() *PID
	Send(pid *PID, message interface{})
	Spawn(props *Props) *PID
}

type PID struct {
	Address string
	ID      string
	Parent  *PID // Dodato polje za roditelja/supervizora
}

// NewPID kreira novi Process ID.
func NewPID(address, id string) *PID {
	return &PID{Address: address, ID: id}
}

func (pid *PID) String() string {
	return fmt.Sprintf("%s/%s", pid.Address, pid.ID)
}

type Props struct {
	producer func() Actor
}

func NewProps(producer func() Actor) *Props {
	return &Props{producer: producer}
}

type ActorRef interface {
	Send(message interface{}, sender *PID)
}

type messageEnvelope struct {
	message interface{}
	sender  *PID
}

type process struct {
	pid     *PID
	mailbox chan messageEnvelope
	actor   Actor
	system  *ActorSystem
}

func newProcess(pid *PID, props *Props, system *ActorSystem) *process {
	actor := props.producer()
	return &process{
		pid:     pid,
		mailbox: make(chan messageEnvelope, 1024),
		actor:   actor,
		system:  system,
	}
}

func (p *process) Send(message interface{}, sender *PID) {
	p.mailbox <- messageEnvelope{message: message, sender: sender}
}

func (p *process) start() {
	go func() {
		for envelope := range p.mailbox {
			context := &actorContext{
				system:  p.system,
				message: envelope.message,
				self:    p.pid,
				sender:  envelope.sender,
			}
			p.actor.Receive(context)
		}
	}()
}

type actorContext struct {
	system  *ActorSystem
	message interface{}
	self    *PID
	sender  *PID
}

func (ctx *actorContext) Message() interface{} {
	return ctx.message
}

func (ctx *actorContext) Sender() *PID {
	return ctx.sender
}

func (ctx *actorContext) Self() *PID {
	return ctx.self
}

func (ctx *actorContext) Send(pid *PID, message interface{}) {
	ctx.system.Send(pid, message, ctx.self)
}

func (ctx *actorContext) Spawn(props *Props) *PID {
	return ctx.system.Spawn(props)
}

type remoteClient struct {
	conn    net.Conn
	encoder *gob.Encoder
}

// --- Sistemske poruke za superviziju ---

// Failure je poruka koja se šalje supervizoru kada se dete sruši.
type Failure struct {
	Who      *PID
	Reason   error
	Metadata map[string]any
}

// ChildStarted je poruka koja se šalje supervizoru kada je dete uspešno pokrenuto.
type ChildStarted struct {
	Child *PID
}

// Restart je poruka koja se može poslati da se restartuje pali aktor.
type Restart struct{}

// Stop je poruka koja se može poslati da se trajno zaustavi aktor.
type Stop struct{}

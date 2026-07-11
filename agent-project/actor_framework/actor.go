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
	Become(behavior ReceiveFunc)
}

type ReceiveFunc func(Context)

type PID struct {
	Address string
	ID      string
	Parent  *PID
}

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
	pid      *PID
	mailbox  chan messageEnvelope
	actor    Actor
	system   *ActorSystem
	props    *Props
	behavior ReceiveFunc
}

func newProcess(pid *PID, props *Props, system *ActorSystem) *process {
	actor := props.producer()
	return &process{
		pid:     pid,
		mailbox: make(chan messageEnvelope, 1024),
		actor:   actor,
		system:  system,
		props:   props,
	}
}

func (p *process) Send(message interface{}, sender *PID) {
	p.mailbox <- messageEnvelope{message: message, sender: sender}
}

func (p *process) start() {
	go func() {
		for envelope := range p.mailbox {
			switch envelope.message.(type) {
			case Stop:
				fmt.Printf("[ActorSystem] Aktor %s zaustavljen (Stop poruka).\n", p.pid.ID)
				p.system.unregister(p.pid)
				return
			case Restart:
				fmt.Printf("[ActorSystem] Aktor %s se restartuje (Restart poruka).\n", p.pid.ID)
				p.actor = p.props.producer()
				p.behavior = nil
			default:
				p.dispatch(envelope)
			}
		}
	}()
}

func (p *process) dispatch(envelope messageEnvelope) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[ActorSystem] Aktor %s je pukao pri obradi poruke %T: %v\n", p.pid.ID, envelope.message, r)
			if p.pid.Parent != nil {
				p.system.Send(p.pid.Parent, Failure{Who: p.pid, Reason: fmt.Sprintf("%v", r)}, p.pid)
			}
		}
	}()

	context := &actorContext{
		system:  p.system,
		message: envelope.message,
		self:    p.pid,
		sender:  envelope.sender,
		proc:    p,
	}

	handler := ReceiveFunc(p.actor.Receive)
	if p.behavior != nil {
		handler = p.behavior
	}
	handler(context)
}

type actorContext struct {
	system  *ActorSystem
	message interface{}
	self    *PID
	sender  *PID
	proc    *process
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
	return ctx.system.SpawnChild(props, ctx.self)
}

func (ctx *actorContext) Become(behavior ReceiveFunc) {
	ctx.proc.behavior = behavior
}

type remoteClient struct {
	conn    net.Conn
	encoder *gob.Encoder
}

type Failure struct {
	Who      *PID
	Reason   string
	Metadata map[string]any
}

type ChildStarted struct {
	Child *PID
}

type Restart struct{}

type Stop struct{}

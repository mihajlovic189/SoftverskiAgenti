package actor_framework

import (
	"fmt"
	"reflect"

	"agent-project/pb"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type MessageCodec interface {
	CanEncode(msg interface{}) bool
	Encode(msg interface{}) (*anypb.Any, error)
	TypeURL() string
	Decode(any *anypb.Any) (interface{}, error)
}

var (
	codecs      []MessageCodec
	codecsByURL = map[string]MessageCodec{}
)

func RegisterCodec(c MessageCodec) {
	codecs = append(codecs, c)
	codecsByURL[c.TypeURL()] = c
}

type typedCodec[T any, P proto.Message] struct {
	typeURL   string
	toProto   func(T) P
	fromProto func(P) T
}

func (c typedCodec[T, P]) CanEncode(msg interface{}) bool {
	_, ok := msg.(T)
	return ok
}

func (c typedCodec[T, P]) TypeURL() string { return c.typeURL }

func (c typedCodec[T, P]) Encode(msg interface{}) (*anypb.Any, error) {
	return anypb.New(c.toProto(msg.(T)))
}

func (c typedCodec[T, P]) Decode(a *anypb.Any) (interface{}, error) {
	p := newProtoInstance[P]()
	if err := a.UnmarshalTo(p); err != nil {
		return nil, err
	}
	return c.fromProto(p), nil
}

func newProtoInstance[P proto.Message]() P {
	var zero P
	return reflect.New(reflect.TypeOf(zero).Elem()).Interface().(P)
}

func RegisterMessageCodec[T any, P proto.Message](toProto func(T) P, fromProto func(P) T) {
	typeURL := "type.googleapis.com/" + string(newProtoInstance[P]().ProtoReflect().Descriptor().FullName())
	RegisterCodec(typedCodec[T, P]{typeURL: typeURL, toProto: toProto, fromProto: fromProto})
}

func EncodeMessage(msg interface{}) (*anypb.Any, error) {
	for _, c := range codecs {
		if c.CanEncode(msg) {
			return c.Encode(msg)
		}
	}
	return nil, fmt.Errorf("nema registrovanog kodeka za tip poruke %T - pozovi actor_framework.RegisterMessageCodec pre slanja preko mreže", msg)
}

func DecodeMessage(a *anypb.Any) (interface{}, error) {
	c, ok := codecsByURL[a.TypeUrl]
	if !ok {
		return nil, fmt.Errorf("nema registrovanog kodeka za primljeni tip poruke %s", a.TypeUrl)
	}
	return c.Decode(a)
}

func PidToProto(p *PID) *pb.PID {
	if p == nil {
		return nil
	}
	return &pb.PID{
		Address: p.Address,
		Id:      p.ID,
		Parent:  PidToProto(p.Parent),
	}
}

func PidFromProto(p *pb.PID) *PID {
	if p == nil {
		return nil
	}
	return &PID{
		Address: p.Address,
		ID:      p.Id,
		Parent:  PidFromProto(p.Parent),
	}
}

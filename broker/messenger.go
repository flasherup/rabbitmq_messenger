package broker

import (
	"context"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
)

const (
	typeError = "error"
)

type Messenger struct {
	Broker   *Broker
	ctx      context.Context
	messages map[string]chan BaseMessage
}

func NewMessenger(ctx context.Context, url, responseQ, requestQ string) (*Messenger, error) {
	ctx = context.WithValue(ctx, CtxKeyURL, url)
	ctx = context.WithValue(ctx, CtxKeyResponseQueue, responseQ)
	ctx = context.WithValue(ctx, CtxKeyRequestQueue, requestQ)

	b, err := New(ctx)
	if err != nil {
		return nil, err
	}

	r := Messenger{
		Broker:   b,
		ctx:      ctx,
		messages: map[string]chan BaseMessage{},
	}

	r.consume()

	return &r, nil
}

func (r Messenger) consume() {
	messages := r.Broker.GetRequestsChan()
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				r.close()
				return
			case deliver, ok := <-messages:
				if !ok {
					return
				}
				err := r.handleMessage(deliver)
				if err != nil {
					log.Printf("message %s value unmarshal error: %s", deliver.MessageId, err.Error())
				}
			}
		}
	}()
}

func (r *Messenger) ListenMessage(name string) <-chan BaseMessage {
	ch := make(chan BaseMessage)
	r.messages[name] = ch
	return ch
}

func (r Messenger) close() {
	for _, c := range r.messages {
		close(c)
	}
}

func (r Messenger) handleMessage(d amqp.Delivery) error {
	m := GetBaseMessage(d)
	if messageChan, exist := r.messages[d.Type]; exist {
		messageChan <- *m

	}
	return nil
}

func (r Messenger) SendResponseMessage(rsp interface{}, message BaseMessage) error {
	msg, err := json.Marshal(rsp)
	if err != nil {
		return err
	}

	return r.Broker.SendMessage(message.MessageId, message.Type, msg)
}

func (r Messenger) SendError(msg string, error string, sender BaseMessage) error {
	e := ErrorRsp{
		Message: msg,
		Error:   error,
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return r.Broker.SendMessage(sender.MessageId, typeError, data)
}

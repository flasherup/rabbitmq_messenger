package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

type ReplyTo struct {
	ctx      context.Context
	conn     *amqp.Connection
	channel  *amqp.Channel
	counter  uint
	uniqueID string
}

type Event struct {
	Data      []byte
	RespChan  chan []byte
	ErrorChan chan error
	Err       error
}

func NewReplyTo(ctx context.Context, url string) (*ReplyTo, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	rt := ReplyTo{
		ctx:      ctx,
		conn:     conn,
		channel:  channel,
		uniqueID: randStringRunes(5),
	}

	go rt.serve()

	return &rt, nil
}

func (r *ReplyTo) serve() {
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				if err := r.channel.Close(); err != nil {
					panic("rabbitmq failed to close chanel")
				}
				if err := r.conn.Close(); err != nil {
					panic("rabbitmq failed to close connection")
				}
				return
			}
		}
	}()
}

func (r *ReplyTo) Listen(queueName string) (<-chan Event, error) {
	q, err := r.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	err = r.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, err
	}

	messages, err := r.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	msgChan := make(chan Event)

	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case msg := <-messages:
				resp := make(chan []byte)
				errorChane := make(chan error)
				msgChan <- Event{
					Data:      msg.Body,
					RespChan:  resp,
					ErrorChan: errorChane,
				}

				select {
				case response := <-resp:
					r.reply(response, msg.ReplyTo, msg.CorrelationId, false)
				case err := <-errorChane:
					r.reply([]byte(err.Error()), msg.ReplyTo, msg.CorrelationId, false)
				}

				close(errorChane)
				close(resp)

				err = msg.Ack(false)
				if err != nil {
					msgChan <- Event{
						Err: err,
					}
				}
			}
		}
	}()
	return msgChan, err
}

func (r *ReplyTo) Request(request string, data interface{}) ([]byte, error) {
	r.counter++
	q, err := r.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	msgs, err := r.channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	corID := fmt.Sprintf("%s:%d", r.uniqueID, r.counter)
	src, err := json.Marshal(data)
	if err != nil {
		log.Fatal("Marshal error", err)
	}

	err = r.channel.Publish(
		"",      // exchange
		request, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corID,
			ReplyTo:       q.Name,
			Body:          src,
		})
	if err != nil {
		return nil, err
	}

	select {
	case <-r.ctx.Done():
		return nil, errors.New("context closed")
	case msg := <-msgs:
		if corID == msg.CorrelationId {
			if msg.Type == typeError {
				return nil, errors.New(string(msg.Body))
			}
			return msg.Body, nil
		}
	}

	return nil, nil
}

func (r *ReplyTo) reply(response []byte, replyTo, corId string, isError bool) {
	mType := ""
	if isError {
		mType = typeError
	}
	err := r.channel.Publish(
		"",      // exchange
		replyTo, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			Type:          mType,
			ContentType:   "text/plain",
			CorrelationId: corId,
			Body:          response,
		})

	if err != nil {
		panic(err)
	}
}

func (r *ReplyTo) GetUniqueID() string {
	return r.uniqueID
}

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

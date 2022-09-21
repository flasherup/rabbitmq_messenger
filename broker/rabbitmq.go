package broker

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"log"
)

const (
	CtxKeyURL           = "ctx_key_url"
	CtxKeyRequestQueue  = "ctx_key_request_queue"
	CtxKeyResponseQueue = "ctx_key_response_queue"
)

var (
	ErrorCtxUrlValue           = errors.New("wrong context url")
	ErrorCtxRequestQueueValue  = errors.New("wrong context request queue")
	ErrorCtxResponseQueueValue = errors.New("wrong context response queue")
)

type Broker struct {
	ctx      context.Context
	conn     *amqp.Connection
	channel  *amqp.Channel
	messages <-chan amqp.Delivery
	cfg      configuration
}

type configuration struct {
	Url           string
	RequestQueue  string
	ResponseQueue string
}

func New(ctx context.Context) (*Broker, error) {
	cfg, err := getConfig(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(cfg.Url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = migration(channel, cfg)
	if err != nil {
		return nil, err
	}

	messages, err := channel.Consume(
		cfg.RequestQueue,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	b := Broker{
		ctx:      ctx,
		conn:     conn,
		channel:  channel,
		messages: messages,
		cfg:      cfg,
	}

	return &b, nil
}

func (b Broker) GetRequestsChan() <-chan amqp.Delivery {
	return b.messages
}

func (b *Broker) serve() {
	go func() {
		for {
			select {
			case <-b.ctx.Done():
				if err := b.channel.Close(); err != nil {
					log.Println("rabbitmq failed to close chanel:", err)
					panic("rabbitmq failed to close chanel")
				}
				if err := b.conn.Close(); err != nil {
					panic("rabbitmq failed to close connection")
				}
				return
			}
		}
	}()
}

func (b Broker) SendMessage(messageId, messageType string, data []byte) error {
	return b.SendMessageRaw(
		amqp.Publishing{
			ContentType: "text/x-json",
			MessageId:   messageId,
			Type:        messageType,
			Body:        data,
		})
}

func (b Broker) SendMessageRaw(message amqp.Publishing) error {
	return b.channel.Publish(
		"",
		b.cfg.ResponseQueue,
		false,
		false,
		message,
	)
}

func migration(channel *amqp.Channel, cfg configuration) error {
	_, err := channel.QueueDeclare(
		cfg.ResponseQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(
		cfg.RequestQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func getConfig(ctx context.Context) (configuration, error) {
	cfg := configuration{}
	var ok bool

	cfg.Url, ok = getCtxValue(ctx, CtxKeyURL)
	if !ok {
		return cfg, ErrorCtxUrlValue
	}

	cfg.RequestQueue, ok = getCtxValue(ctx, CtxKeyRequestQueue)
	if !ok {
		return cfg, ErrorCtxRequestQueueValue
	}

	cfg.ResponseQueue, ok = getCtxValue(ctx, CtxKeyResponseQueue)
	if !ok {
		return cfg, ErrorCtxResponseQueueValue
	}

	return cfg, nil
}

func getCtxValue(ctx context.Context, key string) (string, bool) {
	urlValue := ctx.Value(key)
	if urlValue == nil {
		return "", false
	}
	value, ok := urlValue.(string)
	return value, ok
}

func GetBaseMessage(d amqp.Delivery) *BaseMessage {
	return &BaseMessage{
		Delivery: d,
	}
}

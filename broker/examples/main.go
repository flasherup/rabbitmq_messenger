package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/flasherup/rabbitmq_messenger/broker"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Message struct {
	Counter uint
}

func main() {
	url := "amqp://localhost:5672"
	response := "response_test"
	request := "request_test"

	ctx, cancel := context.WithCancel(context.Background())

	ctx1 := getContext(ctx, url, response, request)

	b1, err := broker.New(ctx1)
	if err != nil {
		log.Fatal("Broker 1 create error", err)
	}

	ctx2 := getContext(ctx, url, request, response)

	b2, err := broker.New(ctx2)
	if err != nil {
		log.Fatal("Broker 2 create error", err)
	}

	pinPong(ctx, b1, b2)

	b, err := json.Marshal(Message{0})
	if err != nil {
		log.Fatal("Marshal error", err)
	}
	err = b1.SendMessageRaw(amqp.Publishing{
		ContentType: "text/x-json",
		MessageId:   "messageVeryFirst",
		Body:        b,
	})

	time.Sleep(time.Second)

	cancel()
	log.Println("Done")
}

func getContext(parent context.Context, url, response, request string) context.Context {
	ctx := context.WithValue(parent, broker.CtxKeyURL, url)
	ctx = context.WithValue(ctx, broker.CtxKeyResponseQueue, response)
	ctx = context.WithValue(ctx, broker.CtxKeyRequestQueue, request)

	return ctx
}

func pinPong(ctx context.Context, b1, b2 *broker.Broker) {
	c := 0
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("End pin pong")
			case msg := <-b1.GetRequestsChan():
				mId := fmt.Sprintf("message:%d", c)
				c++
				readSend(msg, b2, mId)
			case msg := <-b2.GetRequestsChan():
				mId := fmt.Sprintf("message:%d", c)
				c++
				readSend(msg, b1, mId)
			}
		}
	}()
}

func readSend(msg amqp.Delivery, broker *broker.Broker, messageId string) {
	log.Println("Got message", msg.MessageId, "type", msg.Type)
	m := Message{}
	err := json.Unmarshal(msg.Body, &m)
	if err != nil {
		log.Fatal("Broken message", err)
	}
	m.Counter++
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatal("Marshal error", err)
	}
	err = broker.SendMessageRaw(amqp.Publishing{
		ContentType: "text/x-json",
		MessageId:   messageId,
		Type:        "Message",
		Body:        b,
	})
	if err != nil {
		log.Fatal("Send message error", err)
	}
}

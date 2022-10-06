package main

import (
	"context"
	"encoding/json"
	"github.com/flasherup/rabbitmq_messenger/broker"
	"log"
	"time"
)

type Request struct {
	Name    string
	Counter uint
}

func main() {
	url := "amqp://localhost:5672"

	ctx, cancel := context.WithCancel(context.Background())

	rt1, err := broker.NewReplyTo(ctx, url)
	if err != nil {
		log.Fatal("ReplyTo 1 create error", err)
	}

	rt2, err := broker.NewReplyTo(ctx, url)
	if err != nil {
		log.Fatal("ReplyTo 2 create error", err)
	}

	event, err := rt1.Listen("pin-pong")
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("End pin pong")
			case msg := <-event:
				if msg.Err != nil {
					log.Println(err)
					continue
				}
				m := Request{}
				err := json.Unmarshal(msg.Data, &m)
				if err != nil {
					log.Println("Broken request message", err)
					continue
				}

				m.Counter++

				b, err := json.Marshal(m)
				if err != nil {
					log.Fatal("Marshal error", err)
				}
				time.Sleep(time.Second * 3)
				msg.RespChan <- b
			}
		}
	}()

	r := Request{rt1.GetUniqueID() + "-" + rt2.GetUniqueID(), 0}
	go func() {
		counter := 0
		for {
			log.Println("R2 Request:", counter)
			r, err = doRequest(rt2, r)
			if err != nil {
				log.Println("R2 Request error", err)
			}
			log.Println("R2 Response:", counter, r)
			time.Sleep(time.Second)
			counter++
		}
	}()

	rt3, err := broker.NewReplyTo(ctx, url)
	if err != nil {
		log.Fatal("ReplyTo 2 create error", err)
	}

	r2 := Request{rt3.GetUniqueID() + "-" + rt3.GetUniqueID(), 0}
	go func() {
		counter := 0
		for {
			log.Println("R3 Request:", counter)
			r2, err = doRequest(rt3, r2)
			if err != nil {
				log.Println("R3 Request error", err)
			}
			log.Println("R3 Response:", counter, r2)
			time.Sleep(time.Second)
			counter++
		}
	}()

	time.Sleep(time.Second * 60)

	cancel()
	log.Println("Done")
}

func doRequest(rt *broker.ReplyTo, r Request) (Request, error) {
	data, err := rt.Request("pin-pong", r)
	if err != nil {
		return Request{}, err
	}

	m := Request{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return m, err
	}

	return m, nil
}

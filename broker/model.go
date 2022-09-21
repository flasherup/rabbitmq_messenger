package broker

import "github.com/streadway/amqp"

type BaseMessage struct {
	amqp.Delivery
}

type ErrorRsp struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

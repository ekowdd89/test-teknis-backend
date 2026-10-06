package mqtt

import (
	"context"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type IMqtt interface {
	Connect(ctx context.Context) error
	IsConnected() bool
	Close() (err error)
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) (err error)
	Subscribe(ctx context.Context, topic string, qos byte, h Message) (err error)
	UnSubscribe(ctx context.Context, topics ...string) (err error)
	onConnect(c paho.Client)
	withTimeout(ctx context.Context) (context.Context, context.CancelFunc)
}
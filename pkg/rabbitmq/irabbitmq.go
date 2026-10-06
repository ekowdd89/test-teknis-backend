package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)


type IRabbitmq interface {
	Connect(ctx context.Context) (err error)
	connect(ctx context.Context) (err error)
	watch()
	dial() (err error)
	declare(conn *amqp.Connection) (err error)
	markDisconnected()
	waitConn(ctx context.Context) (aq *amqp.Connection, err error)
	withTimeout(ctx context.Context) (context.Context, context.CancelFunc)
	handle(ctx context.Context, d amqp.Delivery, h Handler)
	consumeOnce(ctx context.Context, queue string, h Handler) (err error)
	Consume(ctx context.Context, queue string,  h Handler) (err error)
	publish(ctx context.Context, excahnge, key string, p Publishing) (err error)
	openPubChannel(conn *amqp.Connection) (ch *amqp.Channel, err error)
	Publish(ctx context.Context, exchange, key, contentType string, body []byte) (err error)
	PublishMessage(ctx context.Context, exchange, key string, p Publishing) (err error)
	PublishJSON(ctx context.Context, exchange, key, contentType string, v any) (err error)
	Close() (err error)
	IsConnected() bool
}
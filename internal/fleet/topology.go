package fleet

import (
	"github.com/ekowdd89/test-teknis-backend/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitTopology mendeklarasikan exchange, queue, binding, dan dead-letter
// queue. Dipakai server DAN worker dengan argumen identik, sehingga pesan
// tidak hilang walau server publish sebelum worker pernah berjalan
// (pesan ke exchange tanpa queue terikat akan dibuang broker).
//
// Pesan yang ditolak permanen oleh worker masuk ke "<queue>.dlq".
func RabbitTopology(exchange, queue, routingKey string) []rabbitmq.OptFunc {
	dlx := exchange + ".dlx"
	dlq := queue + ".dlq"
	return []rabbitmq.OptFunc{
		rabbitmq.WithExchange(rabbitmq.Exchange{Name: exchange, Kind: amqp.ExchangeTopic, Durable: true}),
		rabbitmq.WithExchange(rabbitmq.Exchange{Name: dlx, Kind: amqp.ExchangeFanout, Durable: true}),
		rabbitmq.WithQueue(rabbitmq.Queue{Name: dlq, Durable: true}),
		rabbitmq.WithBinding(rabbitmq.Binding{Queue: dlq, Exchange: dlx}),
		rabbitmq.WithQueue(rabbitmq.Queue{
			Name:    queue,
			Durable: true,
			Args:    amqp.Table{"x-dead-letter-exchange": dlx},
		}),
		rabbitmq.WithBinding(rabbitmq.Binding{Queue: queue, Exchange: exchange, RoutingKey: routingKey}),
	}
}

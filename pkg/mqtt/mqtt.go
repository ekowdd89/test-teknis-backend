package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)


var ErrNotConnected = errors.New("mqtt: client not connected")


type Message func(topic string, paylod []byte)

type subscription struct {
	qos byte
	handler Message
}

type Mqtt struct {
	clent paho.Client
	brokerUri string
	clientId string
	username string
	password string
	clearSession bool
	keepAlive time.Duration
	opTimeout time.Duration
	logger *slog.Logger

	mu sync.RWMutex
	subs map[string]subscription

}

type OptFunc func(*Mqtt) error

var _ IMqtt = &Mqtt{}


func WithBrokerId(brokerUri string) OptFunc {
	return func(m *Mqtt) error {
		m.brokerUri = brokerUri
		return nil
	}
}

func WithClientId(clientId string) OptFunc {
	return func(m *Mqtt) error {
		m.clientId = clientId
		return nil
	}
}

func WithCredential(username, password string) OptFunc {
	return func(m *Mqtt) error {
		m.username = username
		m.password = password
		return nil
	}
}

func WithCleanSession(cleanSession bool) OptFunc {
	return func(m *Mqtt) error {
		m.clearSession = cleanSession
		return nil
	}
}

func WithKeepAlive(keepAlive time.Duration) OptFunc {
	return func(m *Mqtt) (err error) {
		if keepAlive <= 0 {
			return  errors.New("mqtt: keepAlive must be greater than 0")
		}
		m.keepAlive = keepAlive
		return nil
	}
}

func WithTimeout(timeout time.Duration) OptFunc {
	return func(m *Mqtt) error {
		if timeout <= 0 {
			return errors.New("mqtt: Error timeout greater than 0")
		}
		m.opTimeout = timeout
		return nil
	}
}

func WithLogger(l *slog.Logger) OptFunc {
	return func(m *Mqtt) error {
		if l == nil {
			return errors.New("Logger nil not initialize")
		}
		m.logger = l
		return nil
	}
}

func WithClient(c paho.Client) OptFunc {
	return func(m *Mqtt) error {
		if c == nil {
			return errors.New("Client nil not initialize")
		}
		m.clent = c
		return nil
	}
}

func New(opts ...OptFunc) (mt *Mqtt, err error) {
	mt = &Mqtt{
		brokerUri: os.Getenv("MQTT_BROKER_URI"),
		clientId: os.Getenv("MQTT_CLIENT_ID"),
		username: os.Getenv("MQTT_USERNAME"),
		password: os.Getenv("MQTT_PASSWORD"),
		clearSession: true,
		keepAlive: 30 * time.Second,
		opTimeout: 10 * time.Second,
		logger: slog.Default(),
		subs: make(map[string]subscription),
	}
	for _, opt:= range opts {
		err = opt(mt);
		if err != nil {
			return
		}
	}
	if mt.clent != nil {
		return
	}
	if mt.brokerUri == "" {
		return nil, errors.New("mqtt: broker uri is required (WithBrokerURI or MQTT_BROKER)")
	}
	if mt.clientId == "" {
		return nil, errors.New("mqtt: client id is required (WithClientId or MQTT_CLIENT_ID)")
	}
	co := paho.NewClientOptions().
		AddBroker(mt.brokerUri).
		SetClientID(mt.clientId).
		SetCleanSession(mt.clearSession).
		SetKeepAlive(mt.keepAlive).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetOrderMatters(false).
		SetOnConnectHandler(mt.onConnect).
		SetConnectionLostHandler(func(c paho.Client, err error) {
			mt.logger.Warn("mqtt connection lost", "error", err)
		})
	if mt.username !="" {
		co.SetUsername(mt.username)
		co.SetPassword(mt.password)
	}
	mt.clent = paho.NewClient(co)
	return
}

// Connect menunggu sampai terhubung atau ctx selesai. Client tetap mencoba
// reconnect otomatis di belakang layar sampai Close dipanggil.
func (m *Mqtt) Connect(ctx context.Context) error {
	return wait(ctx, m.clent.Connect())
}

func (m *Mqtt) IsConnected() bool {
	return m.clent.IsConnectionOpen()
}

// Close memutus koneksi; pesan yang sedang diproses diberi waktu 250ms.
func (m *Mqtt) Close() (err error) {
	m.clent.Disconnect(250)
	return nil
}

func (m *Mqtt) Subscribe(ctx context.Context, topic string, qos byte, h Message) (err error) {
	if topic == "" {
		return errors.New("topic is required")
	}
	if h == nil {
		return errors.New("handler is required")
	}
	m.mu.Lock()
	m.subs[topic] = subscription{qos: qos, handler: h}
	m.mu.Unlock()

	// Daftarkan route sebelum terhubung: dengan clean session false, broker
	// langsung mengirim pesan yang tertahan sesaat setelah connect, sebelum
	// onConnect sempat subscribe ulang.
	m.clent.AddRoute(topic, wrap(h))

	if !m.clent.IsConnectionOpen() {
		return
	}
	ctx, cancel:= m.withTimeout(ctx)
	defer cancel()
	return wait(ctx, m.clent.Subscribe(topic, qos, wrap(h)))
}

func (m *Mqtt) UnSubscribe(ctx context.Context, topics ...string) (err error) {
	m.mu.Lock()
	for _, t:= range topics {
		delete(m.subs, t)
	}
	m.mu.Unlock()
	if !m.clent.IsConnectionOpen() {
		return
	}
	ctx, cancel:= m.withTimeout(ctx)
	defer cancel()
	return wait(ctx, m.clent.Unsubscribe(topics...))
}

func (m *Mqtt) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) (err error) {
	if !m.clent.IsConnectionOpen() {
		return ErrNotConnected
	}
	ctx, cancel:= m.withTimeout(ctx)
	defer cancel()
	return wait(ctx, m.clent.Publish(topic, qos, retain, payload))
}
func (m *Mqtt) PublishJSON(ctx context.Context, topic string, qos byte, retain bool, v any) (err error) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	err = m.Publish(ctx, topic, qos, retain, b)
	return
}
func (m *Mqtt) onConnect(c paho.Client) {
	m.logger.Info("mqtt connected", "broker", m.brokerUri, "client_id", m.clientId)
	m.mu.RLock()
	subs:= make(map[string]subscription, len(m.subs))

	for t, s:= range m.subs {
		subs[t] = s
	}
	m.mu.RUnlock()

	for topic , s:= range subs {
		t:= c.Subscribe(topic, s.qos, wrap(s.handler))
		if !t.WaitTimeout(m.opTimeout) {
			m.logger.Error("subscription timeout", "topic", topic)
			continue
		}
		if err := t.Error(); err !=nil {
			m.logger.Error("subscribe failed","topic", topic, "error", err)
			continue
		}
		m.logger.Info("subscription", "topic", topic, "qos", s.qos)
	}
}

func (m *Mqtt) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok:= ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, m.opTimeout)
}
func wrap(h Message) paho.MessageHandler {
	return func(_ paho.Client, m paho.Message) {
		h(m.Topic(), m.Payload())
	}
}

func wait(ctx context.Context, t paho.Token) (err error) {
	select {
	case <- t.Done():
		return t.Error()
	case <- ctx.Done():
		return ctx.Err()
	}
}
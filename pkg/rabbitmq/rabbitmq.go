package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrClosed       = errors.New("Rabbitmq: client closed")
	ErrNotConnected = errors.New("Rabbitmq: not connected")
	ErrNacked       = errors.New("Rabbitmq: message nacked by broker")
)

type Exchange struct {
	Name    string
	Kind    string
	Durable bool
}
type Queue struct {
	Name    string
	Durable bool
	Args    amqp.Table
}
type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
}
type topology struct {
	exchanges []Exchange
	queues    []Queue
	bindings  []Binding
}

type Message struct {
	Body        []byte
	Exchange    string
	RoutingKey  string
	ContentType string
	MessageId   string
	Timestamp   time.Time
	Headers     map[string]any
	Redelivered bool
}

// Publishing adalah pesan yang dikirim lewat PublishMessage.
type Publishing struct {
	ContentType string
	// MessageId dipakai consumer untuk idempotensi (inbox).
	MessageId string
	Type      string
	Headers   map[string]any
	Body      []byte
}

type Handler func(ctx context.Context, msg Message) (err error)
type permanentError struct{ err error }

func (e *permanentError) Error() string {
	return e.err.Error()
}
func (e *permanentError) Unwrap() error {
	return e.err
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func IsPrmanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type OptFunc func(*Rabbitmq) error

var _ IRabbitmq = &Rabbitmq{}

type Rabbitmq struct {
	uri            string
	logger         *slog.Logger
	reconnectDelay time.Duration
	publishTimeout time.Duration
	prefetch       int
	confirms       bool
	topo           topology
	mu              sync.RWMutex
	conn           *amqp.Connection
	ready chan struct{} // ditutup saat terhubung, dibuat ulang saat terputus
	pubMu          sync.Mutex
	pubCh          *amqp.Channel
	started        atomic.Bool
	ctx            context.Context
	cancel         context.CancelFunc
}

func WithURI(uri string) OptFunc {
	return func(r *Rabbitmq) error {
		if uri == "" {
			return errors.New("Rabbitmq: uri is empty")
		}
		r.uri = uri
		return nil
	}
}

func WithLogger(l *slog.Logger) OptFunc {
	return func(r *Rabbitmq) error {
		if l == nil {
			return errors.New("Rabbitmq: logger is nil")
		}
		r.logger = l
		return nil
	}
}

func WithReconnectDelay(d time.Duration) OptFunc {
	return func(r *Rabbitmq) error {
		if d <= 0 {
			return errors.New("Rabbitmq: reconnect delay must be > 0")
		}
		r.reconnectDelay = d
		return nil
	}
}

// WithPublishTimeout dipakai bila ctx yang diberikan tidak punya deadline.
func WithPublishTimeout(d time.Duration) OptFunc {
	return func(r *Rabbitmq) error {
		if d <= 0 {
			return errors.New("Rabbitmq: publish timeout must be > 0")
		}
		r.publishTimeout = d
		return nil
	}
}

func WithPrefetch(n int) OptFunc {
	return func(r *Rabbitmq) error {
		if n <= 0 {
			return errors.New("Rabbitmq: prefetch must be > 0")
		}
		r.prefetch = n
		return nil
	}
}

// WithPublisherConfirms(true) membuat Publish menunggu ack dari broker.
func WithPublisherConfirms(enabled bool) OptFunc {
	return func(r *Rabbitmq) error {
		r.confirms = enabled
		return nil
	}
}

func WithExchange(e Exchange) OptFunc {
	return func(r *Rabbitmq) error {
		if e.Name == "" {
			return errors.New("Rabbitmq: exchange name is empty")
		}
		if e.Kind == "" {
			e.Kind = amqp.ExchangeTopic
		}
		r.topo.exchanges = append(r.topo.exchanges, e)
		return nil
	}
}

func WithQueue(q Queue) OptFunc {
	return func(r *Rabbitmq) error {
		if q.Name == "" {
			return errors.New("Rabbitmq: queue name is empty")
		}
		r.topo.queues = append(r.topo.queues, q)
		return nil
	}
}

func WithBinding(b Binding) OptFunc {
	return func(r *Rabbitmq) error {
		if b.Queue == "" || b.Exchange == "" {
			return errors.New("Rabbitmq: binding requires queue and exchange")
		}
		r.topo.bindings = append(r.topo.bindings, b)
		return nil
	}
}

func New(opts ...OptFunc) (rb *Rabbitmq, err error) {
	rb = &Rabbitmq{
		uri:            os.Getenv("RABBITMQ_URI"),
		logger:         slog.Default(),
		reconnectDelay: 2 * time.Second,
		publishTimeout: 2 * time.Second,
		prefetch:       10,
		confirms:       true,
		ready:          make(chan struct{}),
	}
	for _, opt := range opts {
		if err = opt(rb); err != nil {
			return
		}
	}
	if rb.uri == "" {
		return nil, errors.New("Rabbitmq: uri is empty")
	}
	rb.ctx, rb.cancel = context.WithCancel(context.Background())

	return
}

// Consume implements IRabbitmq.
func (r *Rabbitmq) Consume(ctx context.Context, queue string, h Handler) (err error) {
	if queue == "" {
		return errors.New("Rabbitmq: queue name is empty")
	}
	if h == nil {
		return errors.New("Rabbitmq: handler is nil")
	}
	for {
		err = r.consumeOnce(ctx, queue, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.ctx.Err() != nil {
			return ErrClosed
		}
		r.logger.Warn("rabbitmq consumer stopped, restarting", "queue", queue, "error", err)
		select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.ctx.Done():
				return r.ctx.Err()
			case <-time.After(r.reconnectDelay):
		}
	}
}

func (r *Rabbitmq) Close() (err error) {
	r.cancel()
	r.pubMu.Lock()
	if r.pubCh != nil && !r.pubCh.IsClosed() {
		_ = r.pubCh.Close()
	}
	r.pubMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn !=nil && !r.conn.IsClosed() {
		return r.conn.Close()
	}
	return
}
func(r *Rabbitmq) IsConnected() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.conn != nil && !r.conn.IsClosed()
}

// Publish implements IRabbitmq.
func (r *Rabbitmq) Publish(ctx context.Context, exchange string, key string, contentType string, body []byte) (err error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return r.publish(ctx, exchange, key, Publishing{ContentType: contentType, Body: body})
}

// PublishMessage mengirim pesan lengkap (MessageId, Type, Headers).
func (r *Rabbitmq) PublishMessage(ctx context.Context, exchange string, key string, p Publishing) (err error) {
	return r.publish(ctx, exchange, key, p)
}

// PublishJSON implements IRabbitmq.
func (r *Rabbitmq) PublishJSON(ctx context.Context, exchange string, key string, contentType string, v any) (err error) {
	b, err:= json.Marshal(v)
	if err != nil {
		return err
	}
	if contentType == "" {
		contentType = "application/json"
	}
	return r.publish(ctx, exchange, key, Publishing{ContentType: contentType, Body: b})
}

// consumeOnce implements IRabbitmq.
func (r *Rabbitmq) consumeOnce(ctx context.Context, queue string, h Handler) (err error) {
	conn, err := r.waitConn(ctx)
	if err != nil {
		return
	}

	ch, err:= conn.Channel()
	if err != nil {
		return fmt.Errorf("open chanel: %w", err)
	}
	// Handler yang masih berjalan harus selesai (ack/nack) sebelum channel ditutup.
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		_ = ch.Close()
	}()

	err = ch.Qos(r.prefetch, 0, false)
	if err != nil {
		return fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	r.logger.Info("rabbitmq consuming", "queue", queue, "prefetch", r.prefetch)

	for {
		select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.ctx.Done():
				return ErrClosed
			case d, ok := <-deliveries:
				if !ok {
					return errors.New("rabbitmq: delivery channel closed")
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					r.handle(ctx, d, h)
				}()
		}
	}
}

// declare implements IRabbitmq.
func (r *Rabbitmq) declare(conn *amqp.Connection) (err error) {
	ch, err:= conn.Channel()
	if err !=nil {
		return fmt.Errorf("open chanel: %w", err)
	}

	defer ch.Close()
	for _, e:= range r.topo.exchanges {
		if err = ch.ExchangeDeclare(e.Name, e.Kind, e.Durable, false, false, false, nil); err != nil {
			return fmt.Errorf("exchange declare: %w", err)
		}
	}
	for _, q:= range r.topo.queues {
		if _, err = ch.QueueDeclare(q.Name, q.Durable, false, false,false, q.Args); err != nil {
			return fmt.Errorf("queue declare: %w", err)
		}
	}
	for _, b:= range r.topo.bindings {
		if err = ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, nil); err != nil {
			return fmt.Errorf("queue bind: %w", err)
		}
	}
	return
}

// dial implements IRabbitmq.
func (r *Rabbitmq) dial() (err error) {
	conn, err := amqp.Dial(r.uri)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	if err:= r.declare(conn); err != nil {
		_ = conn.Close()
		return fmt.Errorf("declare topology: %w", err)
	}
	pubCh, err := r.openPubChannel(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	r.pubMu.Lock()
	r.pubCh = pubCh
	r.pubMu.Unlock()

	r.mu.Lock()
	r.conn = conn
	close(r.ready) // membangunkan semua waitConn
	r.mu.Unlock()

	r.logger.Info("rabbitmq connected")
	return nil
}

func (r *Rabbitmq) openPubChannel(conn *amqp.Connection) (ch *amqp.Channel, err error) {
	ch, err = conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publish chanel: %w", err)
	}
	if r.confirms {
		if err = ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("set publish confirms: %w", err)
		}
	}
	return ch, nil
}

// handle implements IRabbitmq.
func (r *Rabbitmq) handle(ctx context.Context, d amqp.Delivery, h Handler) {
	err := safeCall(ctx, h, toMessage(d))
	switch {
	case err == nil:
		if errAck := d.Ack(false); errAck != nil {
			r.logger.Warn("rabbitmq ack failed", "message_id", d.MessageId, "error", errAck)
		}
	case IsPrmanent(err):
		// Pesan rusak tidak akan pernah berhasil: tolak tanpa requeue
		// (masuk dead-letter queue bila dikonfigurasi).
		r.logger.Error("rabbitmq message rejected (permanent)", "message_id", d.MessageId, "routing_key", d.RoutingKey, "error", err)
		_ = d.Nack(false, false)
	default:
		// Error sementara (mis. database down): kembalikan ke queue setelah jeda
		// agar tidak terjadi loop redelivery yang terlalu cepat.
		r.logger.Warn("rabbitmq message failed, requeue", "message_id", d.MessageId, "routing_key", d.RoutingKey, "error", err)
		select {
		case <-ctx.Done():
		case <-r.ctx.Done():
		case <-time.After(r.reconnectDelay):
		}
		_ = d.Nack(false, true)
	}
}

// markDisconnected implements IRabbitmq.
func (r *Rabbitmq) markDisconnected() {
	r.mu.Lock()
	r.conn = nil
	r.ready = make(chan struct{})
	r.mu.Unlock()
}

// publish implements IRabbitmq.
func (r *Rabbitmq) publish(ctx context.Context, excahnge string, key string, p Publishing) (err error) {
	ctx, cancel:= r.withTimeout(ctx)
	defer cancel()
	conn, err := r.waitConn(ctx)
	if err !=nil {
		return
	}
	msg:= amqp.Publishing{
		ContentType: p.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId: p.MessageId,
		Type: p.Type,
		Headers: amqp.Table(p.Headers),
		Timestamp: time.Now(),
		Body: p.Body,
	}
	r.pubMu.Lock()
	ch:= r.pubCh
	if ch == nil || ch.IsClosed() {
		// Channel bisa tertutup karena error channel-level walau koneksi masih hidup.
		if ch, err = r.openPubChannel(conn); err != nil {
			r.pubMu.Unlock()
			return fmt.Errorf("%w: %v", ErrNotConnected, err)
		}
		r.pubCh = ch
	}
	dc, err:= ch.PublishWithDeferredConfirmWithContext(ctx, excahnge, key, false, false, msg)
	r.pubMu.Unlock()
	if err != nil {
		return fmt.Errorf("rabbitmq: publish: %w", err)
	}
	if dc == nil {
		return nil
	}
	ok, err:= dc.WaitContext(ctx)
	if err !=nil {
		return fmt.Errorf("rabbitmq: wait confirm: %w", err)
	}
	if !ok {
		return ErrNacked
	}
	return
}

// waitConn implements IRabbitmq.
func (r *Rabbitmq) waitConn(ctx context.Context) (aq *amqp.Connection, err error) {
	for {
		r.mu.Lock()
		conn, ready:= r.conn, r.ready
		r.mu.Unlock()
		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}

		// Koneksi tertutup tapi watcher belum mereset state: cek ulang sebentar lagi.
		var wake <-chan struct{} = ready
		var retry <-chan time.Time
		if conn != nil {
			wake = nil
			retry = time.After(100 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.ctx.Done():
			return nil, ErrClosed
			case <-wake:
			case <-retry:
		}
	}
}

// withTimeout implements IRabbitmq.
func (r *Rabbitmq) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok:= ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.publishTimeout)
}

func (r *Rabbitmq) Connect(ctx context.Context) (err error) {
	if !r.started.CompareAndSwap(false, true) {
		return errors.New("rabbitmq: alredy connected")
	}
	if err = r.connect(ctx); err != nil {
		r.started.Store(false)
		return
	}
	go r.watch()
	return
}

func (r *Rabbitmq) connect(ctx context.Context) (err error) {
	for attempt:=1; ; attempt++ {
		err = r.dial()
		if err == nil {
			return nil
		}
		r.logger.Warn("rabbitmq: connect failed, retrying", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(r.reconnectDelay):
		}
	}
}

func (r *Rabbitmq) watch() {
	for {
		r.mu.Lock()
		conn := r.conn
		r.mu.Unlock()

		notify := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-r.ctx.Done():
			return
		case amqpErr := <-notify:
			r.markDisconnected()
			if r.ctx.Err() != nil {
				return
			}
			r.logger.Error("rabbitmq connection lost, reconnecting", "error", amqpErr)
			if err := r.connect(r.ctx); err != nil {
				r.logger.Error("rabbitmq connection error", "error", err)
				return
			}
		}
	}
}

func safeCall(ctx context.Context, h Handler, msg Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, msg)
}
func toMessage(d amqp.Delivery) (msg Message) {
	msg = Message{
		Body: d.Body,
		Headers: d.Headers,
		Exchange: d.Exchange,
		RoutingKey: d.RoutingKey,
		ContentType: d.ContentType,
		MessageId: d.MessageId,
		Timestamp: d.Timestamp,
		Redelivered: d.Redelivered,
	}
	return
}
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)


type OptFunc func(*Postgres) error


type Postgres struct {
	db *sql.DB
	dbUri string
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration

	retryDelay      time.Duration
	logger *slog.Logger
}

func WithDbUri(uri string) OptFunc {
	return func(p *Postgres) error {
		p.dbUri = uri
		return nil
	}
}

func WithLogger(l *slog.Logger) OptFunc {
	return func(p *Postgres) error {
		p.logger = l
		return nil
	}
}

func New(opts ...OptFunc) (pg *Postgres, err error) {
	pg = &Postgres{
		dbUri: os.Getenv("DB_CONNECTION"),
		retryDelay: 2 * time.Second,
		maxOpenConns: 5,
		maxIdleConns: 5,
		connMaxLifetime: 5 * time.Minute,
		connMaxIdleTime: 5 * time.Minute,
		logger: slog.Default(),
	}
	for _, opt:= range opts {
		err = opt(pg);
		if err != nil {
			return
		}
	}
	pg.db, err = sql.Open("pgx", pg.dbUri)
	if err != nil {
		return
	}
	pg.db.SetMaxOpenConns(pg.maxOpenConns)
	pg.db.SetMaxIdleConns(pg.maxIdleConns)
	pg.db.SetConnMaxLifetime(pg.connMaxLifetime)
	pg.db.SetConnMaxIdleTime(pg.connMaxIdleTime)

	return
}

func (p *Postgres) Connect(ctx context.Context) (err error) {
		for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := p.db.PingContext(pingCtx)
		cancel()
		if err == nil {
			p.logger.Info("postgres connected")
			return nil
		}
		p.logger.Warn("postgres not ready, retrying", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres: connect: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(p.retryDelay):
		}
	}

}

func (p *Postgres) WithTx(ctx context.Context, fn func(tx *sql.Tx) (err error)) (err error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}

		if err != nil {
			if errRb := tx.Rollback(); errRb != nil {
				err = errors.Join(err, fmt.Errorf("postgres: rollback %w", errRb))
			}
			return
		}
		if errCm:= tx.Commit(); errCm != nil {
			err = fmt.Errorf("postgres: commit %w", errCm)
		}
	}()
	return fn(tx)
}

func (p *Postgres) Db() *sql.DB {
	return p.db
}

func (p *Postgres) Ping(ctx context.Context) (err error) {
	return p.db.PingContext(ctx)
}

func (p *Postgres) Close() (err error) {
	if p.db !=nil {
		return p.db.Close()
	}
	return
}
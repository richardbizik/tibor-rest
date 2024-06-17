package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/richardbizik/tibor-rest/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func New(opts []kgo.Opt) (*kgo.Client, error) {
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create kafka client"))
	}

	return client, nil
}

func GetDefaultConfig(conf config.KafkaConfig) []kgo.Opt {
	options := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.WithLogger(newKLogger(slog.Default())),
		kgo.DefaultProduceTopic(config.Conf.Kafka.Topic),
	}

	var tlsConfig *tls.Config

	if conf.Auth.TLS.Enabled {
		if conf.Auth.TLS.CAPath != "" {
			caCert, err := os.ReadFile(conf.Auth.TLS.CAPath)
			if err != nil {
				panic(fmt.Errorf("unable to read CA: %w", err))
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				panic(fmt.Errorf("unable to append CA: %s", conf.Auth.TLS.CAPath))
			}
			tlsConfig = &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			}
		}
		if conf.Auth.TLS.CertPath != "" || conf.Auth.TLS.KeyPath != "" {
			if conf.Auth.TLS.CertPath == "" || conf.Auth.TLS.KeyPath == "" {
				panic("both kafka cert and key path must be specified")
			}
			cert, err := tls.LoadX509KeyPair(conf.Auth.TLS.CertPath, conf.Auth.TLS.KeyPath)
			if err != nil {
				panic(fmt.Errorf("unable to load kafka cert and key pair: %s, %s. %w", conf.Auth.TLS.CertPath, conf.Auth.TLS.KeyPath, err))
			}
			if tlsConfig == nil {
				tlsConfig = &tls.Config{
					MinVersion: tls.VersionTLS12,
				}
			}
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		}
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config:    tlsConfig,
		}
		options = append(options, kgo.Dialer(tlsDialer.DialContext))
	} else {
		plainDialer := &net.Dialer{Timeout: 10 * time.Second}
		options = append(options, kgo.Dialer(plainDialer.DialContext))
	}

	if conf.Auth.Scram != 0 {
		scram := scram.Auth{
			User: conf.Auth.Username,
			Pass: conf.Auth.Password,
		}
		if conf.Auth.Scram == 256 {
			options = append(options, kgo.SASL(scram.AsSha256Mechanism()))
		} else if conf.Auth.Scram == 512 {
			options = append(options, kgo.SASL(scram.AsSha512Mechanism()))
		} else {
			panic(fmt.Sprintf("invalid scram alg %q: must be 256 or 512", conf.Auth.Scram))
		}
	} else {
		options = append(options, kgo.SASL(plain.Auth{
			User: conf.Auth.Username,
			Pass: conf.Auth.Password,
		}.AsMechanism()))
	}

	return options
}

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	sl *slog.Logger
}

// New returns a new kgo.Logger that wraps an slog.Logger.
func newKLogger(sl *slog.Logger) *Logger {
	return &Logger{sl}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case l.sl.Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case l.sl.Enabled(ctx, slog.LevelInfo):
		return kgo.LogLevelInfo
	case l.sl.Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	case l.sl.Enabled(ctx, slog.LevelError):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.sl.Log(context.Background(), kgoToSlogLevel(level), msg, keyvals...)
}

func kgoToSlogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelError:
		return slog.LevelError
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelInfo:
		return slog.LevelInfo
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	default:
		// Using the default level for slog
		return slog.LevelInfo
	}
}

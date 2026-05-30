package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all broker configuration.
type Config struct {
	Broker  BrokerConfig
	Storage StorageConfig
	GRPC    GRPCConfig
	Metrics MetricsConfig
}

type BrokerConfig struct {
	ID                   string
	HeartbeatTimeout     time.Duration
	ReplicaLagTimeout    time.Duration
	ReplicaFetchInterval time.Duration
}

type StorageConfig struct {
	DataDir     string
	MaxSegBytes int64
}

type GRPCConfig    struct{ Addr string }
type MetricsConfig struct{ Addr string }

// Load reads config from path, overrideable by BURROW_* env vars.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("BURROW")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetDefault("broker.heartbeat_timeout", "3s")
	v.SetDefault("broker.replica_lag_timeout", "10s")
	v.SetDefault("broker.replica_fetch_interval", "100ms")
	v.SetDefault("storage.data_dir", "/data")
	v.SetDefault("storage.max_seg_bytes", 512<<20)
	v.SetDefault("grpc.addr", "0.0.0.0:9092")
	v.SetDefault("metrics.addr", "0.0.0.0:9093")
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	return &Config{
		Broker: BrokerConfig{
			ID:                   v.GetString("broker.id"),
			HeartbeatTimeout:     v.GetDuration("broker.heartbeat_timeout"),
			ReplicaLagTimeout:    v.GetDuration("broker.replica_lag_timeout"),
			ReplicaFetchInterval: v.GetDuration("broker.replica_fetch_interval"),
		},
		Storage: StorageConfig{
			DataDir:     v.GetString("storage.data_dir"),
			MaxSegBytes: v.GetInt64("storage.max_seg_bytes"),
		},
		GRPC:    GRPCConfig{Addr: v.GetString("grpc.addr")},
		Metrics: MetricsConfig{Addr: v.GetString("metrics.addr")},
	}, nil
}

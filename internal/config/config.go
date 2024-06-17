package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/richardbizik/tibor-rest/internal/profile"
)

type Config struct {
	Kafka KafkaConfig `yaml:"kafka" json:"kafka"`
}

type KafkaConfig struct {
	// Brokers specifies kafka brokers list that are part of a cluster
	Brokers []string `yaml:"brokers" json:"brokers" env:"KAFKA_BROKERS"`
	// Auth specifies the auth mechanism and credential to be used while communicating with the cluster
	Auth KafkaAuth `yaml:"auth" json:"auth"`

	Topic string `yaml:"topic" json:"topic"`
}

type KafkaAuth struct {
	Username string    `yaml:"user" env:"KAFKA_USERNAME"`
	Password string    `yaml:"pass" env:"KAFKA_PASSWORD"`
	Scram    int       `yaml:"scram" env:"KAFKA_SCRAM"`
	TLS      TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" env:"KAFKA_TLS_ENABLED"`
	CAPath   string `yaml:"caPath" env:"KAFKA_TLS_CA_PATH"`
	CertPath string `yaml:"certPath" env:"KAFKA_TLS_CERT_PATH"`
	KeyPath  string `yaml:"keyPath" env:"KAFKA_TLS_KEY_PATH"`
}

var (
	Conf Config
)

func InitConfig() Config {
	c := Config{}
	var fileName string
	confFile := os.Getenv("CONFIG_FILE")
	if confFile == "" {
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		fileName = fmt.Sprintf("%s/conf-%s.yaml", wd, strings.ToLower(string(profile.Current)))
	} else {
		fileName = confFile
	}
	err := cleanenv.ReadConfig(fileName, &c)
	if err != nil {
		fmt.Printf("Error occurred while reading the config file: %s Error: %v", fileName, err)
		panic(err)
	}
	Conf = c
	return c
}

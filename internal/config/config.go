package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// QueueConfig represents the configuration for a single queue.
type QueueConfig struct {
	Name                     string `yaml:"name"`
	MaxSize                  int    `yaml:"maxSize"`
	BackpressureMode         string `yaml:"backpressureMode"`
	VisibilityTimeoutSeconds int    `yaml:"visibilityTimeoutSeconds,omitempty"`
}

// Config represents the overall application configuration.
type Config struct {
	Queues []QueueConfig `yaml:"queues"`
}

// LoadConfig reads and parses the configuration file at the given path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

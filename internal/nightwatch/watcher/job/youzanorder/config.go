package youzanorder

import (
	"os"
	"strconv"
	"time"
)

// Config holds configuration for YouZan order processing.
type Config struct {
	API APIConfig `json:"api"`
}

// APIConfig holds YouZan API configuration.
type APIConfig struct {
	BaseURL   string        `json:"base_url"`
	AppID     string        `json:"app_id"`
	AppSecret string        `json:"app_secret"`
	Timeout   time.Duration `json:"timeout"`
}

// GetDefaultConfig returns default configuration for YouZan order processing.
func GetDefaultConfig() *Config {
	return &Config{
		API: APIConfig{
			BaseURL:   getEnvOrDefault("YOUZAN_API_BASE_URL", "https://open.youzan.com"),
			AppID:     getEnvOrDefault("YOUZAN_APP_ID", ""),
			AppSecret: getEnvOrDefault("YOUZAN_APP_SECRET", ""),
			Timeout:   getDurationEnvOrDefault("YOUZAN_API_TIMEOUT", 30*time.Second),
		},
	}
}

// getEnvOrDefault returns environment variable value or default if not set.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getDurationEnvOrDefault returns environment variable value as duration or default if not set.
func getDurationEnvOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultValue
}

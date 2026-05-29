package utils

import "strings"

// ParseBrokers accepts the common comma-separated KAFKA_BROKERS format and
// trims whitespace. Empty input returns an empty slice so callers can
// deliberately enter no-op/local-dev mode.
func ParseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

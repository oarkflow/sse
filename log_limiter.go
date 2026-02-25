package sse

import (
	"sync"
	"time"
)

type logRateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	last     time.Time
}

func newLogRateLimiter(interval time.Duration) *logRateLimiter {
	return &logRateLimiter{interval: interval}
}

func (l *logRateLimiter) Allow() bool {
	if l == nil {
		return true
	}
	if l.interval <= 0 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last.IsZero() || now.Sub(l.last) >= l.interval {
		l.last = now
		return true
	}
	return false
}

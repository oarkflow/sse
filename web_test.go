package sse

import (
	"errors"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func TestRunAuth(t *testing.T) {
	t.Run("required auth without hook", func(t *testing.T) {
		_, err := runAuth(nil, HandlerOptions{RequireAuth: true})
		if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("required auth with nil result", func(t *testing.T) {
		_, err := runAuth(nil, HandlerOptions{
			RequireAuth: true,
			Authenticate: func(_ fiber.Ctx) (*AuthResult, error) {
				return nil, nil
			},
		})
		if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("auth returns error", func(t *testing.T) {
		wantErr := errors.New("denied")
		_, err := runAuth(nil, HandlerOptions{
			Authenticate: func(_ fiber.Ctx) (*AuthResult, error) {
				return nil, wantErr
			},
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected %v, got %v", wantErr, err)
		}
	})
}

func TestCompileAllowedOrigins(t *testing.T) {
	allowed, wildcard := compileAllowedOrigins([]string{" https://A.EXAMPLE.com ", "*"})
	if !wildcard {
		t.Fatal("expected wildcard to be enabled")
	}
	if _, ok := allowed["https://a.example.com"]; !ok {
		t.Fatal("expected normalized origin to be present")
	}
}

func TestIsOriginAllowed(t *testing.T) {
	var ctx fiber.Ctx

	t.Run("no policy allows all", func(t *testing.T) {
		if !isOriginAllowed("", ctx, HandlerOptions{}, nil, false) {
			t.Fatal("expected request to be allowed without origin policy")
		}
	})

	t.Run("allowlist blocks missing origin", func(t *testing.T) {
		allowed, wildcard := compileAllowedOrigins([]string{"https://app.example.com"})
		if isOriginAllowed("", ctx, HandlerOptions{}, allowed, wildcard) {
			t.Fatal("expected missing origin to be rejected when policy is enabled")
		}
	})

	t.Run("allowlist allows exact match", func(t *testing.T) {
		allowed, wildcard := compileAllowedOrigins([]string{"https://app.example.com"})
		if !isOriginAllowed("https://app.example.com", ctx, HandlerOptions{}, allowed, wildcard) {
			t.Fatal("expected matching origin to be allowed")
		}
	})

	t.Run("allowlist rejects mismatch", func(t *testing.T) {
		allowed, wildcard := compileAllowedOrigins([]string{"https://app.example.com"})
		if isOriginAllowed("https://evil.example.com", ctx, HandlerOptions{}, allowed, wildcard) {
			t.Fatal("expected non-matching origin to be rejected")
		}
	})

	t.Run("validator can reject", func(t *testing.T) {
		if isOriginAllowed("https://app.example.com", ctx, HandlerOptions{
			OriginValidator: func(_ string, _ fiber.Ctx) bool { return false },
		}, nil, false) {
			t.Fatal("expected custom validator to reject")
		}
	})
}

func TestIPConnLimiter(t *testing.T) {
	limiter := newIPConnLimiter(2)
	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("first acquire should succeed")
	}
	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("second acquire should succeed")
	}
	if limiter.Acquire("1.2.3.4") {
		t.Fatal("third acquire should be rejected")
	}

	limiter.Release("1.2.3.4")
	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("acquire after release should succeed")
	}
}

func TestHandlerMetricsSnapshot(t *testing.T) {
	m := &HandlerMetrics{}
	m.TotalConnections.Add(3)
	m.ActiveConnections.Add(1)
	m.RejectedConnections.Add(2)
	m.AuthFailures.Add(1)
	m.OriginRejected.Add(1)
	m.IPLimitRejected.Add(1)
	m.ConnectRateRejected.Add(1)
	m.TopicLimitRejected.Add(1)
	m.DrainingRejected.Add(1)
	m.StreamErrors.Add(4)

	s := m.Snapshot()
	if s.TotalConnections != 3 || s.ActiveConnections != 1 || s.RejectedConnections != 2 || s.StreamErrors != 4 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
}

func TestConnectRateLimiter(t *testing.T) {
	limiter := newConnectRateLimiter(2, time.Minute)
	ip := "10.0.0.1"
	if !limiter.Allow(ip) {
		t.Fatal("first connect should pass")
	}
	if !limiter.Allow(ip) {
		t.Fatal("second connect should pass")
	}
	if limiter.Allow(ip) {
		t.Fatal("third connect should be rate-limited")
	}
}

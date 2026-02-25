package sse

import (
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func TestValidateHubOptions(t *testing.T) {
	if err := ValidateHubOptions(HubOptions{
		BackpressurePolicy: "bad-policy",
	}); err == nil {
		t.Fatal("expected invalid backpressure policy error")
	}

	if err := ValidateHubOptions(HubOptions{
		BackpressurePolicy: BackpressureDropNewest,
	}); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if err := ValidateHubOptions(HubOptions{
		MaxEventBytes: -1,
	}); err == nil {
		t.Fatal("expected max event bytes validation error")
	}

	if err := ValidateHubOptions(HubOptions{
		MaxDropsPerClient: -1,
	}); err == nil {
		t.Fatal("expected max drops per client validation error")
	}
}

func TestValidateHandlerOptions(t *testing.T) {
	if err := ValidateHandlerOptions(HandlerOptions{
		RequireAuth: true,
	}); err == nil {
		t.Fatal("expected missing auth hook error")
	}

	if err := ValidateHandlerOptions(HandlerOptions{
		RequireAuth: true,
		Authenticate: func(ctx fiber.Ctx) (*AuthResult, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if err := ValidateHandlerOptions(HandlerOptions{
		ConnectRateWindow: -time.Second,
	}); err == nil {
		t.Fatal("expected connect rate window validation error")
	}

	if err := ValidateHandlerOptions(HandlerOptions{
		StreamErrorLogInterval: -time.Second,
	}); err == nil {
		t.Fatal("expected stream error log interval validation error")
	}
}

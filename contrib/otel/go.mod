module github.com/oarkflow/sse/contrib/otel

go 1.26.5

replace github.com/oarkflow/sse => ../..

require (
	github.com/oarkflow/sse v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel/metric v1.46.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oarkflow/fh v0.0.25 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

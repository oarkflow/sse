module github.com/oarkflow/sse/contrib/redis

go 1.26.5

replace github.com/oarkflow/sse => ../..

require (
	github.com/alicebob/miniredis/v2 v2.39.0
	github.com/oarkflow/sse v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oarkflow/fh v0.0.25 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

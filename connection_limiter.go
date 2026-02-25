package sse

// ConnectionLimiter controls active-connection admission and release.
// It enables swapping local in-memory limiting with distributed implementations.
type ConnectionLimiter interface {
	Acquire(key string) bool
	Release(key string)
}

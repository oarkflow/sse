# SSE Examples

This directory contains complete examples of using the SSE library with GoFiber.

## Examples Overview

### 1. Simple Example (`simple-server/` and `simple-client/`)

A basic, easy-to-understand implementation that demonstrates:

- Creating a simple SSE server with GoFiber
- Setting up client connections
- Broadcasting events to all clients
- Sending periodic events
- Simple client UI to interact with the server

**Files:**
- `simple-server/main.go` - Server implementation
- `simple-client/index.html` - Client UI

### 2. Advanced Example (`server/` and `client/`)

A full-featured production-ready implementation that includes:

- Authentication and authorization
- Rate limiting
- Management endpoints
- Comprehensive UI with multiple features
- Sticky events
- User and group targeting
- Health checks

**Files:**
- `server/main.go` - Advanced server implementation
- `client/main.go` - Static file server for the UI
- `client/views/index.html` - Interactive demo UI
- `client/views/resilient-sse-client.js` - Advanced client with reconnection logic

## Running the Examples

### Prerequisites

- Go 1.20 or later
- A web browser

### Running the Simple Example

1. Start the server:
   ```bash
   cd simple-server
   go run main.go
   ```

2. Open the client in your browser:
   ```bash
   open simple-client/index.html
   ```
   or directly open `simple-client/index.html` in your browser.

### Running the Advanced Example

1. Start the server:
   ```bash
   cd server
   go run main.go
   ```

2. Start the client server (in a new terminal):
   ```bash
   cd client
   go run main.go
   ```

3. Open your browser and navigate to `http://localhost:8090`

## Features Demonstrated

### Simple Example

- Basic event streaming
- Client connection management
- Broadcasting events
- Periodic events
- Simple UI interactions

### Advanced Example

- **Authentication:** Token-based authentication
- **Authorization:** Per-user and per-group access control
- **Rate Limiting:** Connection rate limiting per IP
- **Management:** REST endpoints for managing events and connections
- **Sticky Events:** Events that are replayed to new clients
- **User Targeting:** Send events to all devices of a specific user
- **Group Targeting:** Send events to groups/rooms of clients
- **Health Checks:** Liveness and readiness endpoints
- **Metrics:** Server metrics collection
- **Reconnection:** Client with exponential backoff reconnection
- **Interactive UI:** Comprehensive demo interface

## Getting Started

For more information about the SSE library and its features, see the main [README.md](../README.md) file.

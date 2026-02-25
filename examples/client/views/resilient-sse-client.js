export class ResilientSSEClient {
  constructor(url, options = {}) {
    this.url = url;
    this.withCredentials = !!options.withCredentials;
    this.maxDelayMs = options.maxDelayMs || 15000;
    this.baseDelayMs = options.baseDelayMs || 500;
    this.jitterMs = options.jitterMs || 300;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs || 60000;
    this.tokenProvider = options.tokenProvider || (() => "");
    this.onOpen = options.onOpen || (() => {});
    this.onMessage = options.onMessage || (() => {});
    this.onError = options.onError || (() => {});
    this.onState = options.onState || (() => {});

    this.es = null;
    this.closed = false;
    this.reconnectAttempt = 0;
    this.lastEventAt = Date.now();
    this.heartbeatTimer = null;
    this.lastEventId = "";
  }

  start() {
    this.closed = false;
    this.connect();
  }

  stop() {
    this.closed = true;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    this.onState("stopped");
  }

  connect() {
    if (this.closed) return;

    const token = this.tokenProvider();
    const url = new URL(this.url, window.location.origin);
    if (token) {
      url.searchParams.set("access_token", token);
    }
    if (this.lastEventId) {
      url.searchParams.set("lastEventId", this.lastEventId);
    }

    this.es = new EventSource(url.toString(), { withCredentials: this.withCredentials });
    this.onState("connecting");

    this.es.onopen = () => {
      this.reconnectAttempt = 0;
      this.lastEventAt = Date.now();
      this.onState("open");
      this.onOpen();
      this.startHeartbeatMonitor();
    };

    this.es.onmessage = (event) => {
      this.lastEventAt = Date.now();
      if (event.lastEventId) {
        this.lastEventId = event.lastEventId;
      }
      this.onMessage(event);
    };

    this.es.onerror = (error) => {
      this.onError(error);
      this.onState("reconnecting");
      this.safeReconnect();
    };
  }

  startHeartbeatMonitor() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      if (Date.now() - this.lastEventAt > this.heartbeatTimeoutMs) {
        this.safeReconnect();
      }
    }, Math.max(2000, Math.floor(this.heartbeatTimeoutMs / 3)));
  }

  safeReconnect() {
    if (this.closed) return;
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    const exp = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** this.reconnectAttempt);
    const jitter = Math.floor(Math.random() * this.jitterMs);
    const delay = exp + jitter;
    this.reconnectAttempt += 1;
    setTimeout(() => this.connect(), delay);
  }
}

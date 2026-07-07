package sse

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

func Format(ev Event) string {
	var b strings.Builder
	if ev.ID != "" {
		b.WriteString("id: ")
		b.WriteString(clean(ev.ID))
		b.WriteByte('\n')
	}
	eventType := ev.Type
	if eventType == "" {
		eventType = "message"
	}
	b.WriteString("event: ")
	b.WriteString(clean(eventType))
	b.WriteByte('\n')
	if ev.Retry > 0 {
		b.WriteString("retry: ")
		b.WriteString(strconv.Itoa(ev.Retry))
		b.WriteByte('\n')
	}
	for _, line := range strings.Split(ev.Data, "\n") {
		b.WriteString("data: ")
		b.WriteString(clean(line))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

func Send(c fh.Ctx, ev Event) error {
	c.Set("Cache-Control", "no-cache, no-transform")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")
	c.Set("Transfer-Encoding", "chunked")
	c.Type("text/event-stream")
	return c.Stream(func(w *fh.StreamWriter) error {
		_, err := w.Write([]byte(Format(ev)))
		if err != nil {
			return err
		}
		return w.Flush()
	})
}

func SendStream(c fh.Ctx, ch <-chan Event) error {
	c.Set("Cache-Control", "no-cache, no-transform")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")
	c.Set("Transfer-Encoding", "chunked")
	c.Type("text/event-stream")
	return c.Stream(func(w *fh.StreamWriter) error {
		for ev := range ch {
			var err error
			_, err = w.Write([]byte(Format(ev)))
			if err != nil {
				return err
			}
			if err = w.Flush(); err != nil {
				return err
			}
		}
		return nil
	})
}

func Heartbeat() string { return ": heartbeat\n\n" }

func NewLegacyEvent(eventType string, data any) Event {
	var dataStr string
	switch x := data.(type) {
	case nil:
		dataStr = ""
	case string:
		dataStr = x
	case []byte:
		dataStr = string(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			dataStr = fmt.Sprint(x)
		} else {
			dataStr = string(b)
		}
	}
	return Event{
		ID:   newID(),
		Type: eventType,
		Data: dataStr,
	}
}

func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x)
		}
		return string(b)
	}
}

func clean(s string) string {
	return strings.NewReplacer("\r", "", "\x00", "").Replace(s)
}

var now = time.Now

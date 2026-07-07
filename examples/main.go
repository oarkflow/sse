package main

import (
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/sse"
)

func main() {
	app := fh.New()

	app.Get("/events", func(c fh.Ctx) error {
		ch := make(chan sse.Event, 16)
		go func() {
			defer close(ch)
			for i := 0; ; i++ {
				ch <- *sse.NewEvent("tick", time.Now().Format(time.RFC3339))
				time.Sleep(time.Second)
				if c.Err() != nil {
					return
				}
			}
		}()
		return sse.SendStream(c, ch)
	})

	app.Get("/hello", func(c fh.Ctx) error {
		return sse.Send(c, *sse.NewEvent("greeting", "Hello, SSE!"))
	})

	app.Listen(":3000")
}

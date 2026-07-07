package main

import (
	fiber "github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/static"
)


func main() {
	app := fiber.New()

	app.Get("/*", static.New("./views"))

	app.Listen(":8090")
}

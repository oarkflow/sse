package main

import (
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/static"
)


func main() {
	app := fiber.New()

	app.Get("/*", static.New("./views"))

	app.Listen(":8090")
}

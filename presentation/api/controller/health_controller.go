package controller

import (
	"github.com/gofiber/fiber/v2"
)

type HealthController interface {
	Health(c *fiber.Ctx) error
}

type healthController struct{}

func NewHealthController(app *fiber.App) HealthController {
	ctrl := &healthController{}

	app.Get("/health", ctrl.Health)

	return ctrl
}

// Health godoc
// @Summary      Health check
// @Description  Returns the health status of the service
// @Tags         Health
// @Accept       json
// @Produce      json
// @Success      200  {object}  map[string]string  "Service is healthy"
// @Router       /health [get]
func (ctrl *healthController) Health(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "ok",
	})
}

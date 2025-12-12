package controller

import (
	"log/slog"

	"github.com/event-ingestion/application"
	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/gofiber/fiber/v2"
)

type MetricsController interface {
	GetMetrics(c *fiber.Ctx) error
}

type metricsController struct {
	service application.MetricsService
}

func NewMetricsController(app *fiber.App, service application.MetricsService) MetricsController {
	ctrl := &metricsController{service: service}

	api := app.Group("/api/v1")
	api.Get("/metrics", ctrl.GetMetrics)

	return ctrl
}

// GetMetrics godoc
// @Summary      Get event metrics
// @Description  Retrieve aggregated metrics for events within a time range
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        event_name  query     string  true   "Event name to filter by"
// @Param        from        query     int     true   "Start timestamp (Unix)"
// @Param        to          query     int     true   "End timestamp (Unix)"
// @Param        channel     query     string  false  "Channel filter (web, mobile_app, api)"
// @Param        group_by    query     string  false  "Group results by (channel, hour, day)"
// @Success      200         {object}  dto.MetricsResponse  "Metrics data"
// @Failure      400         {object}  dto.ErrorResponse    "Validation error"
// @Failure      500         {object}  dto.ErrorResponse    "Internal server error"
// @Router       /api/v1/metrics [get]
func (ctrl *metricsController) GetMetrics(c *fiber.Ctx) error {
	requestID := c.Locals("requestID")

	query := &event.GetMetricsQuery{
		EventName: c.Query("event_name"),
		From:      int64(c.QueryInt("from", 0)),
		To:        int64(c.QueryInt("to", 0)),
		Channel:   c.Query("channel"),
		GroupBy:   c.Query("group_by"),
	}
	if err := query.Validate(); err != nil {
		if validationErr, ok := err.(*event.ValidationError); ok {
			slog.Warn("Metrics query validation failed", "requestID", requestID, "errors", validationErr.Errors)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error":  "validation failed",
				"errors": validationErr.Errors,
			})
		}
		slog.Warn("Metrics query validation failed", "requestID", requestID, "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: err.Error(),
		})
	}

	resp, err := ctrl.service.GetMetrics(c.Context(), query)
	if err != nil {
		slog.Error("Failed to get metrics", "requestID", requestID, "eventName", query.EventName, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(dto.ErrorResponse{
			Error: "failed to get metrics",
		})
	}

	return c.Status(fiber.StatusOK).JSON(resp)
}

package controller

import (
	"log/slog"

	"github.com/event-ingestion/application"
	"github.com/event-ingestion/application/dto"
	"github.com/event-ingestion/domain/event"
	"github.com/gofiber/fiber/v2"
)

type EventController interface {
	IngestEvent(c *fiber.Ctx) error
	IngestBulk(c *fiber.Ctx) error
}

type eventController struct {
	service application.EventService
}

func NewEventController(app *fiber.App, service application.EventService) EventController {
	ctrl := &eventController{service: service}

	api := app.Group("/api/v1")
	api.Post("/events", ctrl.IngestEvent)
	api.Post("/events/bulk", ctrl.IngestBulk)

	return ctrl
}

// IngestEvent godoc
// @Summary      Ingest single event
// @Description  Ingest a single event into the system for processing
// @Tags         Events
// @Accept       json
// @Produce      json
// @Param        event  body      event.IngestEventCommand  true  "Event data"
// @Success      202    {object}  dto.EventResponse         "Event queued for processing"
// @Failure      400    {object}  dto.ErrorResponse         "Validation error"
// @Failure      500    {object}  dto.ErrorResponse         "Internal server error"
// @Router       /api/v1/events [post]
func (ctrl *eventController) IngestEvent(c *fiber.Ctx) error {
	requestID := c.Locals("requestID")

	var cmd event.IngestEventCommand
	if err := c.BodyParser(&cmd); err != nil {
		slog.Warn("Invalid request body", "requestID", requestID, "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "invalid request body",
		})
	}

	resp, err := ctrl.service.IngestEvent(c.Context(), &cmd)
	if err != nil {
		if validationErr, ok := err.(*event.ValidationError); ok {
			slog.Warn("Event validation failed", "requestID", requestID, "errors", validationErr.Errors)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error":  "validation failed",
				"errors": validationErr.Errors,
			})
		}
		slog.Error("Failed to process event", "requestID", requestID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(dto.ErrorResponse{
			Error: "failed to process event",
		})
	}

	slog.Debug("Event ingested", "requestID", requestID, "eventID", resp.EventID)
	return c.Status(fiber.StatusAccepted).JSON(resp)
}

// IngestBulk godoc
// @Summary      Ingest bulk events
// @Description  Ingest multiple events (up to 1000) in a single request
// @Tags         Events
// @Accept       json
// @Produce      json
// @Param        events  body      event.IngestBulkCommand   true  "Bulk events data"
// @Success      202     {object}  dto.BulkEventResponse     "All events queued"
// @Success      207     {object}  dto.BulkEventResponse     "Partial success"
// @Failure      400     {object}  dto.ErrorResponse         "Validation error"
// @Failure      500     {object}  dto.ErrorResponse         "Internal server error"
// @Router       /api/v1/events/bulk [post]
func (ctrl *eventController) IngestBulk(c *fiber.Ctx) error {
	requestID := c.Locals("requestID")

	var cmd event.IngestBulkCommand
	if err := c.BodyParser(&cmd); err != nil {
		slog.Warn("Invalid bulk request body", "requestID", requestID, "error", err)
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "invalid request body",
		})
	}

	if err := cmd.Validate(); err != nil {
		slog.Warn("Bulk command validation failed", "requestID", requestID, "error", err.Message)
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: err.Message,
		})
	}

	resp, err := ctrl.service.IngestBulk(c.Context(), &cmd)
	if err != nil {
		slog.Error("Failed to process bulk events", "requestID", requestID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(dto.ErrorResponse{
			Error: "failed to process events",
		})
	}

	slog.Info("Bulk events processed", "requestID", requestID, "total", len(cmd.Events), "success", resp.SuccessCount, "failed", resp.FailedCount)
	return c.Status(determineBulkStatusCode(resp)).JSON(resp)
}

func determineBulkStatusCode(resp *dto.BulkEventResponse) int {
	switch {
	case resp.FailedCount > 0 && resp.SuccessCount > 0:
		return fiber.StatusMultiStatus
	case resp.FailedCount > 0:
		return fiber.StatusBadRequest
	default:
		return fiber.StatusAccepted
	}
}

package controller

import (
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
	var cmd event.IngestEventCommand
	if err := c.BodyParser(&cmd); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "invalid request body",
		})
	}

	resp, err := ctrl.service.IngestEvent(c.Context(), &cmd)
	if err != nil {
		if validationErr, ok := err.(*event.ValidationError); ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error":  "validation failed",
				"errors": validationErr.Errors,
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(dto.ErrorResponse{
			Error: "failed to process event",
		})
	}

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
	var cmd event.IngestBulkCommand
	if err := c.BodyParser(&cmd); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "invalid request body",
		})
	}

	if len(cmd.Events) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "events array cannot be empty",
		})
	}

	if len(cmd.Events) > 1000 {
		return c.Status(fiber.StatusBadRequest).JSON(dto.ErrorResponse{
			Error: "events array cannot exceed 1000 items",
		})
	}

	resp, err := ctrl.service.IngestBulk(c.Context(), &cmd)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(dto.ErrorResponse{
			Error: "failed to process events",
		})
	}

	// Determine status code based on results
	statusCode := fiber.StatusAccepted
	if resp.FailedCount > 0 && resp.SuccessCount > 0 {
		statusCode = fiber.StatusMultiStatus // 207
	} else if resp.FailedCount > 0 && resp.SuccessCount == 0 {
		statusCode = fiber.StatusBadRequest
	}

	return c.Status(statusCode).JSON(resp)
}

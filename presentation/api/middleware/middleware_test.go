package middleware

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestID_GeneratesNewUUID(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	requestID := resp.Header.Get("X-Request-ID")
	assert.NotEmpty(t, requestID)
	assert.Len(t, requestID, 36) // UUID format
}

func TestRequestID_UsesExistingHeader(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	existingID := "existing-request-id-123"
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Request-ID", existingID)

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	requestID := resp.Header.Get("X-Request-ID")
	assert.Equal(t, existingID, requestID)
}

func TestRequestID_SetsLocals(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())

	var localRequestID interface{}
	app.Get("/test", func(c *fiber.Ctx) error {
		localRequestID = c.Locals("requestID")
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	_, err := app.Test(req)
	require.NoError(t, err)

	assert.NotNil(t, localRequestID)
	assert.NotEmpty(t, localRequestID.(string))
}

func TestRequestLogger_LogsRequest(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(RequestLogger())
	app.Get("/test-path", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/test-path", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
}

func TestRequestLogger_LogsPostRequest(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(RequestLogger())
	app.Post("/api/events", func(c *fiber.Ctx) error {
		return c.Status(202).SendString("accepted")
	})

	req := httptest.NewRequest("POST", "/api/events", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 202, resp.StatusCode)
}

func TestRecovery_CatchesPanic(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(Recovery())
	app.Get("/panic", func(c *fiber.Ctx) error {
		panic("test panic")
	})

	req := httptest.NewRequest("GET", "/panic", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 500, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "internal server error")
}

func TestRecovery_NoPanic(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(Recovery())
	app.Get("/normal", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/normal", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "ok", string(body))
}

func TestRecovery_ReturnsJSONError(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(Recovery())
	app.Get("/panic", func(c *fiber.Ctx) error {
		panic("something went wrong")
	})

	req := httptest.NewRequest("GET", "/panic", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 500, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestMiddlewareChain(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Use(RequestLogger())
	app.Use(Recovery())

	var capturedRequestID string
	app.Get("/chain", func(c *fiber.Ctx) error {
		capturedRequestID = c.Locals("requestID").(string)
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/chain", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	assert.NotEmpty(t, capturedRequestID)
	assert.Equal(t, capturedRequestID, resp.Header.Get("X-Request-ID"))
}

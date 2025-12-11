package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	// Flags
	targetRPS := flag.Int("rps", 20000, "Target requests per second (in events)")
	duration := flag.Duration("duration", 60*time.Second, "Test duration")
	workers := flag.Int("workers", 100, "Number of concurrent workers")
	batchSize := flag.Int("batch", 200, "Events per batch")
	url := flag.String("url", "http://localhost:8080/api/v1/events/bulk", "Target URL")
	flag.Parse()

	fmt.Printf("Load Test Configuration:\n")
	fmt.Printf("  Target RPS: %d events/sec\n", *targetRPS)
	fmt.Printf("  Duration: %v\n", *duration)
	fmt.Printf("  Workers: %d\n", *workers)
	fmt.Printf("  Batch Size: %d\n", *batchSize)
	fmt.Printf("  URL: %s\n\n", *url)

	var successCount, errorCount, totalEvents int64
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	start := time.Now()
	var wg sync.WaitGroup

	// Calculate interval per worker
	batchesPerSecond := float64(*targetRPS) / float64(*batchSize)
	batchesPerWorkerPerSecond := batchesPerSecond / float64(*workers)
	intervalPerBatch := time.Duration(float64(time.Second) / batchesPerWorkerPerSecond)

	fmt.Printf("  Batches/sec total: %.0f\n", batchesPerSecond)
	fmt.Printf("  Interval per batch: %v\n\n", intervalPerBatch)

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(intervalPerBatch)
			defer ticker.Stop()
			for time.Since(start) < *duration {
				<-ticker.C
				events := generateBulkEvents(*batchSize, workerID)
				if err := sendBulkEvents(client, *url, events); err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
					atomic.AddInt64(&totalEvents, int64(*batchSize))
				}
			}
		}(i)
	}

	// Progress reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for time.Since(start) < *duration {
			<-ticker.C
			elapsed := time.Since(start).Seconds()
			events := atomic.LoadInt64(&totalEvents)
			errors := atomic.LoadInt64(&errorCount)
			rps := float64(events) / elapsed
			fmt.Printf("[%5.0fs] Events: %8d | Errors: %5d | RPS: %8.0f\n",
				elapsed, events, errors, rps)
		}
	}()

	wg.Wait()

	elapsed := time.Since(start).Seconds()
	fmt.Printf("\n========== FINAL RESULTS ==========\n")
	fmt.Printf("Duration:       %.2f seconds\n", elapsed)
	fmt.Printf("Total Events:   %d\n", totalEvents)
	fmt.Printf("Total Batches:  %d (success) / %d (error)\n", successCount, errorCount)
	fmt.Printf("Average RPS:    %.0f events/sec\n", float64(totalEvents)/elapsed)
	fmt.Printf("Error Rate:     %.2f%%\n", float64(errorCount)/float64(successCount+errorCount)*100)
	fmt.Printf("====================================\n")
}

func generateBulkEvents(count int, workerID int) map[string]interface{} {
	events := make([]map[string]interface{}, count)
	now := time.Now().Unix()

	channels := []string{"web", "mobile_app", "api"}
	eventNames := []string{"product_view", "add_to_cart", "purchase", "page_view", "signup"}

	for i := 0; i < count; i++ {
		events[i] = map[string]interface{}{
			"event_name":  eventNames[i%len(eventNames)],
			"channel":     channels[i%len(channels)],
			"campaign_id": fmt.Sprintf("cmp_%d", (workerID*count+i)%100),
			"user_id":     fmt.Sprintf("user_%d", (workerID*count+i)%10000),
			"timestamp":   now - int64(i%3600),
			"tags":        []string{"test", "loadtest", fmt.Sprintf("worker_%d", workerID)},
			"metadata": map[string]interface{}{
				"product_id": fmt.Sprintf("prod_%d", i%1000),
				"price":      99.99 + float64(i%100),
				"currency":   "TRY",
				"worker_id":  workerID,
			},
		}
	}
	return map[string]interface{}{"events": events}
}

func sendBulkEvents(client *http.Client, url string, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("status: %d", resp.StatusCode)
	}
	return nil
}

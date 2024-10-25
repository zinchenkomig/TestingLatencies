package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	binanceWSUrl = "fstream.binance.com"
	symbol       = "btcusdt@bookTicker"
)

// LatencyData stores update ID and latency in nanoseconds
type LatencyData struct {
	ConnectionID int    `json:"connection_id"`
	UpdateID     uint64 `json:"update_id"`
	Latency      int64  `json:"latency_ns"`
}

func connectAndCollectLatencies(id int, wg *sync.WaitGroup, latenciesChan chan<- LatencyData, stopChan <-chan struct{}) {
	defer wg.Done()

	u := url.URL{Scheme: "wss", Host: binanceWSUrl, Path: fmt.Sprintf("/ws/%s", symbol)}
	log.Printf("Connecting to %s with connection %d\n", u.String(), id)

	// Dial the WebSocket connection
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("Connection %d failed: %v\n", id, err)
		return
	}
	defer c.Close()

	for {
		select {
		case <-stopChan:
			// Exit when the stop signal is received
			log.Printf("Stopping connection %d\n", id)
			return
		default:
			// Read message from WebSocket
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Connection %d read error: %v\n", id, err)
				return
			}

			// Capture current time and parse received data
			localTime := time.Now()

			// Parse the received data (extract update ID and event time)
			var data map[string]interface{}
			err = json.Unmarshal(message, &data)
			if err != nil {
				log.Printf("Error parsing message for connection %d: %v\n", id, err)
				continue
			}

			// Extract update ID and event time (in milliseconds)
			updateID, ok := data["u"].(float64)
			eventTime, ok2 := data["E"].(float64)
			if !ok || !ok2 {
				log.Printf("Missing fields in message for connection %d\n", id)
				continue
			}

			// Calculate latency in nanoseconds
			serverTime := time.Unix(0, int64(eventTime)*int64(time.Millisecond))
			latency := localTime.Sub(serverTime).Nanoseconds()

			// Send latency data to the channel
			latenciesChan <- LatencyData{ConnectionID: id, UpdateID: uint64(updateID), Latency: latency}
		}
	}
}

func main() {
	var wg sync.WaitGroup
	latenciesChan := make(chan LatencyData, 100)
	stopChan := make(chan struct{})
	latencyResults := []LatencyData{}
	outputFile := "latencies.json"

	// Timer to stop the data collection after 1 minute
	collectionDuration := 1 * time.Minute
	timer := time.AfterFunc(collectionDuration, func() {
		close(stopChan)
	})

	// Spawn 5 goroutines for parallel WebSocket connections
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go connectAndCollectLatencies(i, &wg, latenciesChan, stopChan)
	}

	// Collect latencies and store in slice
	go func() {
		for latency := range latenciesChan {
			latencyResults = append(latencyResults, latency)
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()

	// Once done, write the latencies to a JSON file
	timer.Stop() // Stop the timer if goroutines finish earlier
	close(latenciesChan)

	// Create or open the file for writing the latencies in JSON format
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create file: %v\n", err)
	}
	defer f.Close()

	// Write the latencies as a JSON array
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ") // Pretty print JSON
	err = encoder.Encode(latencyResults)
	if err != nil {
		log.Fatalf("Failed to write JSON data: %v\n", err)
	}

	log.Printf("Latencies saved to %s\n", outputFile)
}

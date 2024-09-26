// nyxsub_test.go

package nyxsub

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestProcessPool tests the ProcessPool and Process functionality.
func TestProcessPool(t *testing.T) {
	// Create a temporary directory for the test worker program.
	tmpDir, err := os.MkdirTemp("", "processpool_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir) // Clean up the temporary directory.

	// Write the test worker program to a file.
	workerProgram := `
	package main
	
	import (
		"bufio"
		"encoding/json"
		"os"
		"time"
	)
	
	func main() {
		// Send a ready message to indicate the process is ready.
		readyMsg := map[string]interface{}{"type": "ready"}
		json.NewEncoder(os.Stdout).Encode(readyMsg)
	
		// Read commands from stdin.
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
	
			var cmd map[string]interface{}
			err := json.Unmarshal([]byte(line), &cmd)
			if err != nil {
				// Send an error message if the JSON is invalid.
				errMsg := map[string]interface{}{"type": "error", "message": "invalid JSON"}
				if id, ok := cmd["id"]; ok {
					errMsg["id"] = id
				}
				json.NewEncoder(os.Stdout).Encode(errMsg)
				continue
			}
	
			// Simulate processing time.
			time.Sleep(100 * time.Millisecond)
	
			// Send a success response.
			response := map[string]interface{}{
				"type":    "success",
				"id":      cmd["id"],
				"message": "ok",
				"data":    cmd["data"],
			}
			json.NewEncoder(os.Stdout).Encode(response)
		}
	}
	`

	workerFilePath := filepath.Join(tmpDir, "test_worker.go")
	err = os.WriteFile(workerFilePath, []byte(workerProgram), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Compile the test worker program.
	workerBinaryPath := filepath.Join(tmpDir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinaryPath, workerFilePath)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to compile worker program: %v\nOutput: %s", err, string(out))
	}

	// Create a logger.
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create the ProcessPool.
	pool := NewProcessPool(
		"test",
		2, // Number of processes in the pool.
		&logger,
		tmpDir,
		workerBinaryPath,
		[]string{},
		2*time.Second, // Worker timeout.
		2*time.Second, // Communication timeout.
		2*time.Second, // Initialization timeout.
	)

	// Wait for the pool to be ready.
	err = pool.WaitForReady()
	if err != nil {
		t.Fatal(err)
	}

	// Send commands to the pool and check the responses.
	for i := 0; i < 5; i++ {
		cmd := map[string]interface{}{
			"data": fmt.Sprintf("test data %d", i),
		}
		response, err := pool.SendCommand(cmd)
		if err != nil {
			t.Fatal(err)
		}

		// Check the response type.
		if response["type"] != "success" {
			t.Errorf("Expected response type 'success', got '%v'", response["type"])
		}

		// Check the response message.
		if response["message"] != "ok" {
			t.Errorf("Expected message 'ok', got '%v'", response["message"])
		}

		// Check the response data.
		if response["data"] != cmd["data"] {
			t.Errorf("Expected data '%v', got '%v'", cmd["data"], response["data"])
		}
	}

	// Stop the process pool.
	pool.StopAll()
}

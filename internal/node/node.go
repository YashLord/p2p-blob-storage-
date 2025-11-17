// internal/node/node.go
package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"p2p-blob-storage/internal/storage"
)

type Node struct {
	ID            string
	Address       string
	Port          string
	BootstrapAddr string
	storage       *storage.Storage
	server        *http.Server
	httpClient    *http.Client
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// Maximum retries for operations
	maxRetries    int
	retryInterval time.Duration

	// Last known peers for redundancy
	knownPeers []string
	peerMutex  sync.RWMutex
}

func NewNode(id, port, bootstrapAddr, storagePath string, maxStorageBytes uint64) (*Node, error) {
	// Get local IP address
	localIP, err := getLocalIP()
	if err != nil {
		return nil, fmt.Errorf("failed to get local IP: %w", err)
	}

	n := &Node{
		ID:            id,
		Port:          port,
		Address:       fmt.Sprintf("%s:%s", localIP, port),
		BootstrapAddr: bootstrapAddr,
		storage:       storage.NewStorage(storagePath, maxStorageBytes),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		stopCh:        make(chan struct{}),
		maxRetries:    3,
		retryInterval: 5 * time.Second,
		knownPeers:    make([]string, 0),
	}

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/chunk/store", n.handleStoreChunk)
	mux.HandleFunc("/chunk/exists", n.handleChunkExists)
	mux.HandleFunc("/chunk/retrieve", n.handleRetrieveChunk)
	mux.HandleFunc("/chunk/delete", n.handleDeleteChunk)
	mux.HandleFunc("/health", n.handleHealth)

	n.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return n, nil
}

func (n *Node) Start() error {
	// Start HTTP server
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Node server error: %v\n", err)
		}
	}()

	// Wait for server to start and retry bootstrap registration
	time.Sleep(100 * time.Millisecond)

	var lastErr error
	for i := 0; i < n.maxRetries; i++ {
		if err := n.registerWithBootstrap(); err != nil {
			lastErr = err
			fmt.Printf("Failed to register with bootstrap (attempt %d/%d): %v\n",
				i+1, n.maxRetries, err)
			time.Sleep(n.retryInterval)
			continue
		}
		lastErr = nil
		break
	}

	if lastErr != nil {
		return fmt.Errorf("failed to register with bootstrap after %d attempts: %w",
			n.maxRetries, lastErr)
	}

	// Start heartbeat
	n.wg.Add(1)
	go n.sendHeartbeats()

	return nil
}

func (n *Node) Stop() {
	close(n.stopCh)
	n.server.Close()
	n.wg.Wait()
}

func (n *Node) registerWithBootstrap() error {
	url := fmt.Sprintf("http://%s/register", n.BootstrapAddr)

	data := map[string]string{
		"id":      n.ID,
		"address": n.Address,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := n.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bootstrap returned status: %d", resp.StatusCode)
	}

	return nil
}

func (n *Node) sendHeartbeats() {
	defer n.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			// Send heartbeat
			url := fmt.Sprintf("http://%s/heartbeat?id=%s", n.BootstrapAddr, n.ID)
			resp, err := n.httpClient.Post(url, "application/json", nil)
			if err != nil {
				fmt.Printf("Heartbeat failed: %v\n", err)
				// On heartbeat failure, try to re-register
				if err := n.registerWithBootstrap(); err != nil {
					fmt.Printf("Re-registration failed: %v\n", err)
				}
				continue
			}
			resp.Body.Close()

			// Update known peers list
			if err := n.updateKnownPeers(); err != nil {
				fmt.Printf("Failed to update peer list: %v\n", err)
			}
		}
	}
}

func (n *Node) handleStoreChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var reqData struct {
		ChunkID int    `json:"chunk_id"`
		Data    []byte `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := n.storage.StoreChunk(reqData.ChunkID, reqData.Data); err != nil {
		http.Error(w, "Failed to store chunk", http.StatusInternalServerError)
		return
	}

	// Return an explicit acknowledgement with chunk id
	resp := map[string]interface{}{
		"status":   "stored",
		"chunk_id": reqData.ChunkID,
	}
	json.NewEncoder(w).Encode(resp)
}

func (n *Node) handleChunkExists(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var chunkID int
	if _, err := fmt.Sscanf(r.URL.Query().Get("id"), "%d", &chunkID); err != nil {
		http.Error(w, "Invalid chunk ID", http.StatusBadRequest)
		return
	}

	exists := n.storage.HasChunk(chunkID)

	json.NewEncoder(w).Encode(map[string]bool{"exists": exists})
}

func (n *Node) handleRetrieveChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var chunkID int
	if _, err := fmt.Sscanf(r.URL.Query().Get("id"), "%d", &chunkID); err != nil {
		http.Error(w, "Invalid chunk ID", http.StatusBadRequest)
		return
	}

	chunk, err := n.storage.RetrieveChunk(chunkID)
	if err != nil {
		http.Error(w, "Chunk not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(chunk)
}

func (n *Node) handleDeleteChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var reqData struct {
		ChunkID int `json:"chunk_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := n.storage.DeleteChunk(reqData.ChunkID); err != nil {
		http.Error(w, "Failed to delete chunk", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"status": "deleted", "chunk_id": reqData.ChunkID})
}

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (n *Node) updateKnownPeers() error {
	url := fmt.Sprintf("http://%s/peers", n.BootstrapAddr)
	resp, err := n.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bootstrap returned status: %d", resp.StatusCode)
	}

	var peers []struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return err
	}

	n.peerMutex.Lock()
	n.knownPeers = make([]string, len(peers))
	for i, peer := range peers {
		n.knownPeers[i] = peer.Address
	}
	n.peerMutex.Unlock()

	return nil
}

func getLocalIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

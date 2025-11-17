// internal/bootstrap/server.go
package bootstrap

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Node struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	LastSeen time.Time `json:"last_seen"`
}

type FileMetadata struct {
	Hash       string           `json:"hash"`
	Name       string           `json:"name"`
	Size       int64            `json:"size"`
	ChunkCount int              `json:"chunk_count"`
	Chunks     map[int][]string `json:"chunks"` // chunk_id -> []node_addresses
	Owner      string           `json:"owner"`
	Timestamp  time.Time        `json:"timestamp"`
}

type Server struct {
	port   string
	nodes  map[string]*Node
	files  map[string]*FileMetadata
	mu     sync.RWMutex
	server *http.Server
}

func NewServer(port string) (*Server, error) {
	s := &Server{
		port:  port,
		nodes: make(map[string]*Node),
		files: make(map[string]*FileMetadata),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/peers", s.handleGetPeers)
	mux.HandleFunc("/file/register", s.handleFileRegister)
	mux.HandleFunc("/file/delete", s.handleFileDelete)
	mux.HandleFunc("/file/metadata", s.handleFileMetadata)
	mux.HandleFunc("/file/list", s.handleFileList)

	s.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Load persisted metadata (if any)
	_ = s.loadFiles()

	// Start cleanup routine
	go s.cleanupStaleNodes()

	// Periodically persist files map
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = s.saveFiles()
		}
	}()

	return s, nil
}

const bootstrapDataFile = "bootstrap_data.json"

func (s *Server) saveFiles() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := json.MarshalIndent(s.files, "", "  ")
	if err != nil {
		return err
	}

	// Write atomically: write to temp file then rename
	tmpFile := bootstrapDataFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpFile, bootstrapDataFile); err != nil {
		return err
	}
	return nil
}

func (s *Server) loadFiles() error {
	if _, err := os.Stat(bootstrapDataFile); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(bootstrapDataFile)
	if err != nil {
		return err
	}

	var files map[string]*FileMetadata
	if err := json.Unmarshal(data, &files); err != nil {
		return err
	}

	s.mu.Lock()
	for k, v := range files {
		s.files[k] = v
	}
	s.mu.Unlock()

	return nil
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Stop() {
	s.server.Close()
	// Persist files on graceful shutdown
	_ = s.saveFiles()
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var node Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.LastSeen = time.Now()

	s.mu.Lock()
	s.nodes[node.ID] = &node
	count := len(s.nodes)
	s.mu.Unlock()

	// Print real-time registration event
	fmt.Printf("[%s] Node registered: id=%s address=%s (peers=%d)\n", time.Now().Format(time.RFC3339), node.ID, node.Address, count)

	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		http.Error(w, "Node ID required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if node, exists := s.nodes[nodeID]; exists {
		node.LastSeen = time.Now()
		// Print heartbeat event for visibility
		fmt.Printf("[%s] Heartbeat received: id=%s address=%s\n", time.Now().Format(time.RFC3339), node.ID, node.Address)
	}
	s.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	peers := make([]*Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		peers = append(peers, node)
	}
	s.mu.RUnlock()

	json.NewEncoder(w).Encode(peers)
}

func (s *Server) handleFileRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var metadata FileMetadata
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	metadata.Timestamp = time.Now()

	// Verify that for each chunk listed, a quorum of the provided peers actually has the chunk.
	// Quorum defined as majority of the listed replicas for that chunk.
	client := &http.Client{Timeout: 3 * time.Second}

	for chunkID, addrs := range metadata.Chunks {
		if len(addrs) == 0 {
			http.Error(w, fmt.Sprintf("no replicas listed for chunk %d", chunkID), http.StatusBadRequest)
			return
		}
		required := (len(addrs) / 2) + 1
		found := 0
		for _, addr := range addrs {
			url := fmt.Sprintf("http://%s/chunk/exists?id=%d", addr, chunkID)
			resp, err := client.Get(url)
			if err != nil {
				continue
			}
			var body struct {
				Exists bool `json:"exists"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err == nil && body.Exists {
				found++
			}
			resp.Body.Close()
			if found >= required {
				break
			}
		}
		if found < required {
			http.Error(w, fmt.Sprintf("insufficient replicas for chunk %d: have %d need %d", chunkID, found, required), http.StatusConflict)
			return
		}
	}

	// All chunks have quorum - record metadata and persist
	s.mu.Lock()
	s.files[metadata.Hash] = &metadata
	s.mu.Unlock()

	_ = s.saveFiles()

	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

func (s *Server) handleFileMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hash := r.URL.Query().Get("hash")
	if hash == "" {
		http.Error(w, "Hash required", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	metadata, exists := s.files[hash]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(metadata)
}

func (s *Server) handleFileDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Hash string `json:"hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.Hash == "" {
		http.Error(w, "Hash required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if _, exists := s.files[req.Hash]; exists {
		delete(s.files, req.Hash)
		_ = s.saveFiles()
	}
	s.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (s *Server) handleFileList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	owner := r.URL.Query().Get("owner")

	s.mu.RLock()
	files := make([]*FileMetadata, 0)
	for _, file := range s.files {
		if owner == "" || file.Owner == owner {
			files = append(files, file)
		}
	}
	s.mu.RUnlock()

	json.NewEncoder(w).Encode(files)
}

func (s *Server) cleanupStaleNodes() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		for id, node := range s.nodes {
			if time.Since(node.LastSeen) > 2*time.Minute {
				delete(s.nodes, id)
				fmt.Printf("Removed stale node: %s\n", id)
			}
		}
		s.mu.Unlock()
	}
}

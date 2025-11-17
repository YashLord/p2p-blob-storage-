// internal/client/client.go
package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"p2p-blob-storage/internal/bootstrap"
	"p2p-blob-storage/internal/node"
	"p2p-blob-storage/internal/storage"
)

type Client struct {
	nodeID        string
	bootstrapAddr string
	node          *node.Node
	httpClient    *http.Client
	storagePath   string
}

type FileInfo struct {
	Hash       string
	Name       string
	Size       int64
	ChunkCount int
}

func NewClient(bootstrapAddr, listenPort, nodeID, storagePath string) (*Client, error) {
	if nodeID == "" {
		nodeID = generateNodeID()
	}

	// Determine storage path for client node. If the caller provided a path, use it;
	// otherwise fall back to a temp directory.
	var clientStoragePath string
	if storagePath != "" {
		clientStoragePath = storagePath
	} else {
		clientStoragePath = filepath.Join(os.TempDir(), fmt.Sprintf("gostore_client_%s", nodeID))
	}

	// clients don't allocate an on-node storage limit here (we only write pending
	// chunks to disk as files under the provided path), so no maxStorageBytes is needed.

	// Do not create/start a full storage node for clients. Clients should not register
	// with the bootstrap as storage peers. Instead, use a client-local storage path
	// (created on demand) to persist pending chunk files. This prevents the client
	// from being included in the peer list used for distribution.

	c := &Client{
		nodeID:        nodeID,
		bootstrapAddr: bootstrapAddr,
		node:          nil,
		storagePath:   clientStoragePath,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Ensure storage path exists and is writable when provided by caller.
	if clientStoragePath != "" {
		if err := os.MkdirAll(clientStoragePath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create client storage path: %w", err)
		}
		// Try writing a small temp file to ensure writability
		testFile := filepath.Join(clientStoragePath, ".gostore_write_test")
		if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
			return nil, fmt.Errorf("storage path not writable: %w", err)
		}
		_ = os.Remove(testFile)

		fmt.Printf("Client using storage path: %s\n", clientStoragePath)
	}

	// Load any pending journal entries and attempt retries
	if entries, err := LoadAllPending(nodeID); err == nil && len(entries) > 0 {
		go func() {
			for _, e := range entries {
				if e.Completed {
					continue
				}
				// ensure quorum for each chunk; try to repair missing replicas
				peers, _ := c.getPeers()
				for chunkID := 0; chunkID < e.Metadata.ChunkCount; chunkID++ {
					locs := e.ChunkLocations[chunkID]
					replicationFactor := chooseReplicationFactor(e.Metadata.Size, len(peers))
					requireAck := (replicationFactor / 2) + 1
					if len(locs) >= requireAck {
						continue
					}
					// try to read local chunk file
					localPath := filepath.Join(c.storagePath, fmt.Sprintf("pending_%s_chunk_%d", e.FileHash, chunkID))
					data, err := os.ReadFile(localPath)
					if err != nil {
						// cannot repair without chunk data
						continue
					}
					// try to store to peers not already present
					for _, p := range peers {
						if contains(locs, p.Address) {
							continue
						}
						if err := c.storeChunkOnPeer(chunkID, data, p.Address); err == nil {
							// update journal
							_ = UpdateChunkLocation(nodeID, e.FileHash, chunkID, p.Address)
							locs = append(locs, p.Address)
						}
						if len(locs) >= requireAck {
							break
						}
					}
				}
				// After attempting repair, try to register metadata
				if err := c.registerFileMetadata(e.Metadata); err == nil {
					_ = MarkCompleted(nodeID, e.FileHash)
					// cleanup local chunk files
					for chunkID := 0; chunkID < e.Metadata.ChunkCount; chunkID++ {
						_ = os.Remove(filepath.Join(c.storagePath, fmt.Sprintf("pending_%s_chunk_%d", e.FileHash, chunkID)))
					}
				}
			}
		}()
	}

	return c, nil
}

func (c *Client) StoreFile(filePath string) (string, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %w", err)
	}

	fileName := filepath.Base(filePath)
	fileSize := fileInfo.Size()

	fmt.Printf("Uploading: %s (%.2f MB)\n", fileName, float64(fileSize)/(1024*1024))

	// Determine dynamic chunk size based on file size
	chunkSize := chooseChunkSize(fileSize)

	// Calculate file hash and split into chunks with dynamic chunk size
	chunks, fileHash, err := storage.SplitFile(file, chunkSize)
	if err != nil {
		return "", fmt.Errorf("failed to split file: %w", err)
	}

	fmt.Printf("Split into %d chunks\n", len(chunks))

	// Get available peers
	peers, err := c.getPeers()
	if err != nil {
		return "", fmt.Errorf("failed to get peers: %w", err)
	}

	if len(peers) == 0 {
		return "", fmt.Errorf("no peers available")
	}

	fmt.Printf("Distributing to %d peers...\n", len(peers))

	// Prepare journal entry and save before sending chunks
	entry := &JournalEntry{
		FileHash: fileHash,
		Metadata: &bootstrap.FileMetadata{
			Hash:       fileHash,
			Name:       fileName,
			Size:       fileSize,
			ChunkCount: len(chunks),
			Chunks:     map[int][]string{},
			Owner:      c.nodeID,
			Timestamp:  time.Now(),
		},
		ChunkLocations: make(map[int][]string),
		Completed:      false,
	}

	if err := AppendEntry(c.nodeID, entry); err != nil {
		return "", fmt.Errorf("failed to append journal entry: %w", err)
	}

	// persist chunks locally so we can retry after restarts
	for i, chunk := range chunks {
		chunkPath := filepath.Join(c.storagePath, fmt.Sprintf("pending_%s_chunk_%d", fileHash, i))
		if err := os.MkdirAll(c.storagePath, 0755); err == nil {
			_ = os.WriteFile(chunkPath, chunk, 0644)
		}
	}

	// Distribute chunks using deterministic sharding and dynamic replication
	chunkLocations, err := c.distributeChunksDeterministic(chunks, peers, fileHash, fileSize)
	if err != nil {
		return "", fmt.Errorf("failed to distribute chunks: %w", err)
	}

	// Update journal entry in-memory and persist (journal updated incrementally during stores)
	entry.ChunkLocations = chunkLocations
	entry.Metadata.Chunks = chunkLocations
	// mark completed after successful registration below

	// Register file metadata with bootstrap
	metadata := &bootstrap.FileMetadata{
		Hash:       fileHash,
		Name:       fileName,
		Size:       fileSize,
		ChunkCount: len(chunks),
		Chunks:     chunkLocations,
		Owner:      c.nodeID,
		Timestamp:  time.Now(),
	}

	if err := c.registerFileMetadata(metadata); err != nil {
		return "", fmt.Errorf("failed to register metadata: %w", err)
	}

	// Mark journal entry completed and cleanup local chunk files
	_ = MarkCompleted(c.nodeID, fileHash)
	for i := 0; i < len(chunks); i++ {
		_ = os.Remove(filepath.Join(c.storagePath, fmt.Sprintf("pending_%s_chunk_%d", fileHash, i)))
	}

	return fileHash, nil
}

func (c *Client) GetFile(hash, outputPath string, removeAfter bool) (string, error) {
	// Get file metadata from bootstrap
	metadata, err := c.getFileMetadata(hash)
	if err != nil {
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}

	fmt.Printf("Retrieving: %s (%.2f MB)\n", metadata.Name, float64(metadata.Size)/(1024*1024))
	fmt.Printf("Fetching %d chunks...\n", metadata.ChunkCount)

	// Retrieve chunks
	chunks := make([][]byte, metadata.ChunkCount)

	progress := &ProgressBar{Total: metadata.ChunkCount, Width: 50}
	progress.Start()

	for chunkID, nodeAddrs := range metadata.Chunks {
		chunk, err := c.retrieveChunkWithRetry(chunkID, nodeAddrs)
		if err != nil {
			progress.Finish()
			return "", fmt.Errorf("failed to retrieve chunk %d: %w", chunkID, err)
		}
		chunks[chunkID] = chunk
		progress.Increment()
	}

	progress.Finish()

	// Reassemble file
	if outputPath == "" {
		outputPath = metadata.Name
	}

	if err := storage.ReassembleFile(chunks, outputPath); err != nil {
		return "", fmt.Errorf("failed to reassemble file: %w", err)
	}

	// Verify hash
	file, err := os.Open(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to verify file: %w", err)
	}
	defer file.Close()

	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", fmt.Errorf("failed to calculate hash: %w", err)
	}

	retrievedHash := hex.EncodeToString(h.Sum(nil))
	if retrievedHash != hash {
		os.Remove(outputPath)
		return "", fmt.Errorf("hash mismatch: expected %s, got %s", hash, retrievedHash)
	}

	// If requested, attempt to delete remote chunk copies from storage nodes
	if removeAfter {
		fmt.Printf("Attempting to delete remote chunk copies for %s...\n", hash)
		// Try deleting chunk copies on each peer listed for each chunk
		for chunkID, addrs := range metadata.Chunks {
			for _, addr := range addrs {
				if err := c.deleteChunkOnPeer(chunkID, addr); err != nil {
					fmt.Printf("  Warning: failed to delete chunk %d on %s: %v\n", chunkID, addr, err)
				} else {
					fmt.Printf("  Deleted chunk %d on %s\n", chunkID, addr)
				}
			}
		}

		// Inform bootstrap to remove file metadata
		if err := c.deleteFileMetadata(hash); err != nil {
			fmt.Printf("Warning: failed to delete file metadata from bootstrap: %v\n", err)
		} else {
			fmt.Printf("File metadata removed from bootstrap\n")
		}
	}

	return outputPath, nil
}

// deleteChunkOnPeer sends a request to a storage peer to delete a chunk by ID.
func (c *Client) deleteChunkOnPeer(chunkID int, peerAddr string) error {
	url := fmt.Sprintf("http://%s/chunk/delete", peerAddr)
	reqData := map[string]int{"chunk_id": chunkID}
	jsonData, _ := json.Marshal(reqData)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}
	return nil
}

// deleteFileMetadata notifies the bootstrap to remove the file metadata entry.
func (c *Client) deleteFileMetadata(hash string) error {
	url := fmt.Sprintf("http://%s/file/delete", c.bootstrapAddr)
	reqData := map[string]string{"hash": hash}
	jsonData, _ := json.Marshal(reqData)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bootstrap returned status: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) ListFiles() ([]FileInfo, error) {
	url := fmt.Sprintf("http://%s/file/list?owner=%s", c.bootstrapAddr, c.nodeID)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status: %d", resp.StatusCode)
	}

	var metadata []*bootstrap.FileMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	files := make([]FileInfo, len(metadata))
	for i, m := range metadata {
		files[i] = FileInfo{
			Hash:       m.Hash,
			Name:       m.Name,
			Size:       m.Size,
			ChunkCount: m.ChunkCount,
		}
	}

	return files, nil
}

// chooseChunkSize returns a chunk size (bytes) based on file size heuristics.
// Small files use a smaller chunk; larger files use larger chunks to reduce metadata.
func chooseChunkSize(fileSize int64) int {
	// Heuristics (can be tuned):
	// <1MB -> use file size (single chunk)
	// 1MB-10MB -> 512KB
	// 10MB-100MB -> 1MB
	// 100MB-1GB -> 4MB
	// >1GB -> 8MB
	const (
		KB = 1024
		MB = KB * 1024
	)

	switch {
	case fileSize <= 1*MB:
		if fileSize <= 0 {
			return storage.DefaultChunkSize
		}
		return int(fileSize)
	case fileSize <= 10*MB:
		return 1 * MB
	case fileSize <= 100*MB:
		return 2 * MB
	case fileSize <= 1024*MB:
		return 4 * MB
	default:
		return 8 * MB
	}
}

// chooseReplicationFactor picks a replication factor based on file size and available peers.
func chooseReplicationFactor(fileSize int64, peerCount int) int {
	if peerCount <= 1 {
		return 1
	}
	// Heuristic: larger files get higher replication for durability.
	switch {
	case fileSize <= 10*1024*1024: // <=10MB
		return min(1, peerCount)
	case fileSize <= 100*1024*1024: // <=100MB
		return min(2, peerCount)
	default:
		return min(3, peerCount)
	}
}

// distributeChunksDeterministic deterministically assigns shards to peers using file hash
// and chunk index to choose starting peer. Replication factor is chosen dynamically.
func (c *Client) distributeChunksDeterministic(chunks [][]byte, peers []*bootstrap.Node, fileHash string, fileSize int64) (map[int][]string, error) {
	chunkLocations := make(map[int][]string)
	peerCount := len(peers)
	if peerCount == 0 {
		return nil, fmt.Errorf("no peers available")
	}

	// Convert fileHash hex to bytes, use first 8 bytes for a seed
	hashBytes, err := hex.DecodeString(fileHash)
	if err != nil || len(hashBytes) == 0 {
		// fallback: simple sum
		hashBytes = []byte{0}
	}
	var seed uint64
	for i := 0; i < len(hashBytes) && i < 8; i++ {
		seed = (seed << 8) | uint64(hashBytes[i])
	}

	replicationFactor := chooseReplicationFactor(fileSize, peerCount)

	for i, chunk := range chunks {
		// deterministic start index
		start := int((seed + uint64(i)) % uint64(peerCount))
		locations := make([]string, 0, replicationFactor)
		used := make(map[int]struct{})
		// We'll attempt to store until we reach replicationFactor or exhaust peers
		attempts := 0
		maxAttempts := peerCount * 2
		for len(locations) < replicationFactor && attempts < maxAttempts {
			idx := (start + attempts) % peerCount
			attempts++
			if _, ok := used[idx]; ok {
				continue
			}
			peer := peers[idx]

			// try storing; allow one retry per peer
			if err := c.storeChunkOnPeer(i, chunk, peer.Address); err != nil {
				// mark tried and continue
				used[idx] = struct{}{}
				continue
			}
			locations = append(locations, peer.Address)
			used[idx] = struct{}{}
			// persist ack in journal
			_ = UpdateChunkLocation(c.nodeID, fileHash, i, peer.Address)
		}

		// quorum check: require majority of replicationFactor
		requireAck := (replicationFactor / 2) + 1
		if len(locations) < requireAck {
			return nil, fmt.Errorf("failed to achieve quorum for chunk %d: got %d/%d", i, len(locations), replicationFactor)
		}
		chunkLocations[i] = locations
	}

	return chunkLocations, nil
}

func (c *Client) Close() {
	if c.node != nil {
		c.node.Stop()
	}
}

func (c *Client) getPeers() ([]*bootstrap.Node, error) {
	url := fmt.Sprintf("http://%s/peers", c.bootstrapAddr)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var peers []*bootstrap.Node
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func (c *Client) distributeChunks(chunks [][]byte, peers []*bootstrap.Node) (map[int][]string, error) {
	chunkLocations := make(map[int][]string)

	// Create load balancer with Fisher-Yates shuffle
	lb := NewLoadBalancer(peers)

	progress := &ProgressBar{Total: len(chunks), Width: 50}
	progress.Start()

	for i, chunk := range chunks {
		// Replicate each chunk to multiple nodes (replication factor = 3)
		replicationFactor := min(3, len(peers))
		locations := make([]string, 0, replicationFactor)

		for j := 0; j < replicationFactor; j++ {
			peer := lb.GetNext()

			if err := c.storeChunkOnPeer(i, chunk, peer.Address); err != nil {
				// Retry with different peer
				peer = lb.GetNext()
				if err := c.storeChunkOnPeer(i, chunk, peer.Address); err != nil {
					progress.Finish()
					return nil, fmt.Errorf("failed to store chunk %d: %w", i, err)
				}
			}

			locations = append(locations, peer.Address)
		}

		chunkLocations[i] = locations
		progress.Increment()
	}

	progress.Finish()

	return chunkLocations, nil
}

func (c *Client) storeChunkOnPeer(chunkID int, chunk []byte, peerAddr string) error {
	url := fmt.Sprintf("http://%s/chunk/store", peerAddr)

	reqData := map[string]interface{}{
		"chunk_id": chunkID,
		"data":     chunk,
	}

	jsonData, err := json.Marshal(reqData)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}

	// Parse ack body
	var ack struct {
		Status  string `json:"status"`
		ChunkID int    `json:"chunk_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		return fmt.Errorf("invalid ack from peer: %w", err)
	}
	if ack.Status != "stored" || ack.ChunkID != chunkID {
		return fmt.Errorf("peer ack mismatch")
	}

	return nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (c *Client) retrieveChunkWithRetry(chunkID int, nodeAddrs []string) ([]byte, error) {
	var lastErr error

	fmt.Printf("  Chunk %d: trying %d replicas...\n", chunkID, len(nodeAddrs))
	for _, addr := range nodeAddrs {
		fmt.Printf("    Attempting retrieval from %s...", addr)
		chunk, err := c.retrieveChunkFromPeer(chunkID, addr)
		if err == nil {
			fmt.Printf(" ✓\n")
			return chunk, nil
		}
		fmt.Printf(" ✗ (%v)\n", err)
		lastErr = err
	}

	fmt.Printf("  ! All replicas failed for chunk %d\n", chunkID)
	return nil, fmt.Errorf("all replicas failed: %w", lastErr)
}

func (c *Client) retrieveChunkFromPeer(chunkID int, peerAddr string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/chunk/retrieve?id=%d", peerAddr, chunkID)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) registerFileMetadata(metadata *bootstrap.FileMetadata) error {
	url := fmt.Sprintf("http://%s/file/register", c.bootstrapAddr)

	jsonData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	// Retry registration with exponential backoff
	backoff := time.Second
	for attempts := 0; attempts < 6; attempts++ {
		resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("failed to register metadata after retries")
}

func (c *Client) getFileMetadata(hash string) (*bootstrap.FileMetadata, error) {
	url := fmt.Sprintf("http://%s/file/metadata?hash=%s", c.bootstrapAddr, hash)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bootstrap returned status: %d", resp.StatusCode)
	}

	var metadata bootstrap.FileMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func generateNodeID() string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

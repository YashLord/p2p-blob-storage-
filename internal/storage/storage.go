// internal/storage/storage.go
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ChunkSize is the size used to split files into chunks (1 MiB)
// DefaultChunkSize is the fallback chunk size (1 MiB)
const DefaultChunkSize = 1024 * 1024

type ChunkMetric struct {
	Size         int
	AccessCount  int
	LastAccessed int64
}

type Storage struct {
	chunks       map[int][]byte
	chunkMetrics map[int]*ChunkMetric
	mu           sync.RWMutex
	activeOps    sync.WaitGroup
	storagePath  string
	maxSize      uint64
	currentSize  uint64
}

func NewStorage(storagePath string, maxSize uint64) *Storage {
	// Create storage directory if it doesn't exist
	os.MkdirAll(storagePath, 0755)

	s := &Storage{
		chunks:       make(map[int][]byte),
		chunkMetrics: make(map[int]*ChunkMetric),
		storagePath:  storagePath,
		maxSize:      maxSize,
		currentSize:  0,
	}

	// Load existing chunks on startup to restore state
	_ = s.loadExistingChunks()

	return s
}

// loadExistingChunks scans the storagePath for chunk files and populates
// the in-memory metadata without loading full chunks into memory.
func (s *Storage) loadExistingChunks() error {
	entries, err := os.ReadDir(s.storagePath)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "chunk_") {
			continue
		}
		idStr := strings.TrimPrefix(name, "chunk_")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}

		s.chunks[id] = nil // marker that chunk exists on disk
		s.chunkMetrics[id] = &ChunkMetric{
			Size:         int(fi.Size()),
			AccessCount:  0,
			LastAccessed: time.Now().Unix(),
		}
		s.currentSize += uint64(fi.Size())
	}

	return nil
}

func (s *Storage) StoreChunk(id int, data []byte) error {
	s.activeOps.Add(1)
	defer s.activeOps.Done()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if adding this chunk would exceed the size limit
	newSize := s.currentSize + uint64(len(data))
	if newSize > s.maxSize {
		return fmt.Errorf("storing chunk would exceed storage limit of %d bytes", s.maxSize)
	}

	// Create chunk file path
	chunkPath := fmt.Sprintf("%s/chunk_%d", s.storagePath, id)

	// Write chunk to file
	if err := os.WriteFile(chunkPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk to file: %w", err)
	}

	buf := make([]byte, len(data))
	copy(buf, data)
	s.chunks[id] = buf
	s.chunkMetrics[id] = &ChunkMetric{
		Size:         len(data),
		AccessCount:  0,
		LastAccessed: time.Now().Unix(),
	}

	// Update current size
	s.currentSize = newSize

	return nil
}

func (s *Storage) RetrieveChunk(id int) ([]byte, error) {
	s.activeOps.Add(1)
	defer s.activeOps.Done()

	s.mu.RLock()
	_, exists := s.chunks[id]
	s.mu.RUnlock()

	if !exists {
		return nil, errors.New("chunk not found")
	}

	// Read chunk from file
	chunkPath := fmt.Sprintf("%s/chunk_%d", s.storagePath, id)
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file: %w", err)
	}

	s.mu.Lock()
	if m, ok := s.chunkMetrics[id]; ok {
		m.AccessCount++
		m.LastAccessed = time.Now().Unix()
	}
	s.mu.Unlock()

	buf := make([]byte, len(data))
	copy(buf, data)
	return buf, nil
}

// HasChunk returns true if the chunk exists on disk
func (s *Storage) HasChunk(id int) bool {
	s.mu.RLock()
	_, exists := s.chunks[id]
	s.mu.RUnlock()

	if !exists {
		// still might exist on disk if loadExistingChunks didn't detect; check file
		chunkPath := fmt.Sprintf("%s/chunk_%d", s.storagePath, id)
		if _, err := os.Stat(chunkPath); err == nil {
			return true
		}
		return false
	}
	return true
}

func (s *Storage) DeleteChunk(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get chunk size before deleting
	if metric, exists := s.chunkMetrics[id]; exists {
		s.currentSize -= uint64(metric.Size)
	}

	// Delete chunk file
	chunkPath := fmt.Sprintf("%s/chunk_%d", s.storagePath, id)
	if err := os.Remove(chunkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete chunk file: %w", err)
	}

	delete(s.chunks, id)
	delete(s.chunkMetrics, id)
	return nil
}

func (s *Storage) ListChunks() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]int, 0, len(s.chunks))
	for id := range s.chunks {
		ids = append(ids, id)
	}
	return ids
}

func (s *Storage) GetMetrics(id int) (*ChunkMetric, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.chunkMetrics[id]
	if !ok {
		return nil, errors.New("chunk not found")
	}
	return m, nil
}

func (s *Storage) Wait() {
	s.activeOps.Wait()
}

// SplitFile splits a file into chunks and returns the chunks and file hash
func SplitFile(file *os.File, chunkSize int) ([][]byte, string, error) {
	hash := sha256.New()
	teeReader := io.TeeReader(file, hash)

	chunks := make([][]byte, 0)
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	buffer := make([]byte, chunkSize)

	for {
		n, err := teeReader.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, "", fmt.Errorf("failed to read file: %w", err)
		}

		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buffer[:n])
			chunks = append(chunks, chunk)
		}

		if err == io.EOF {
			break
		}
	}

	fileHash := hex.EncodeToString(hash.Sum(nil))
	return chunks, fileHash, nil
}

// ReassembleFile reassembles chunks into a file
func ReassembleFile(chunks [][]byte, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	for _, chunk := range chunks {
		if _, err := file.Write(chunk); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	return nil
}

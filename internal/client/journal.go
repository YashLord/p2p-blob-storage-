package client

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"p2p-blob-storage/internal/bootstrap"
)

type JournalEntry struct {
	FileHash       string                  `json:"file_hash"`
	Metadata       *bootstrap.FileMetadata `json:"metadata"`
	ChunkLocations map[int][]string        `json:"chunk_locations"`
	Completed      bool                    `json:"completed"`
}

var journalMu sync.Mutex

func journalPath(nodeID string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("gostore_client_%s_journal.json", nodeID))
}

func readJournal(nodeID string) ([]*JournalEntry, error) {
	path := journalPath(nodeID)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return []*JournalEntry{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var arr []*JournalEntry
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}

func writeJournal(nodeID string, entries []*JournalEntry) error {
	path := journalPath(nodeID)
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// AppendEntry appends a pending entry to the journal
func AppendEntry(nodeID string, entry *JournalEntry) error {
	journalMu.Lock()
	defer journalMu.Unlock()
	entries, err := readJournal(nodeID)
	if err != nil {
		return err
	}
	entries = append(entries, entry)
	return writeJournal(nodeID, entries)
}

// UpdateChunkLocation records a successful store for a chunk
func UpdateChunkLocation(nodeID, fileHash string, chunkID int, peerAddr string) error {
	journalMu.Lock()
	defer journalMu.Unlock()
	entries, err := readJournal(nodeID)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.FileHash == fileHash {
			if e.ChunkLocations == nil {
				e.ChunkLocations = make(map[int][]string)
			}
			// avoid duplicates
			locs := e.ChunkLocations[chunkID]
			for _, l := range locs {
				if l == peerAddr {
					return writeJournal(nodeID, entries)
				}
			}
			e.ChunkLocations[chunkID] = append(e.ChunkLocations[chunkID], peerAddr)
			return writeJournal(nodeID, entries)
		}
	}
	return fmt.Errorf("journal entry not found for %s", fileHash)
}

// MarkCompleted marks an entry completed and writes journal
func MarkCompleted(nodeID, fileHash string) error {
	journalMu.Lock()
	defer journalMu.Unlock()
	entries, err := readJournal(nodeID)
	if err != nil {
		return err
	}
	newEntries := make([]*JournalEntry, 0, len(entries))
	for _, e := range entries {
		if e.FileHash == fileHash {
			// skip removing; mark completed and keep for history
			e.Completed = true
		}
		newEntries = append(newEntries, e)
	}
	return writeJournal(nodeID, newEntries)
}

// LoadAllPending returns all journal entries
func LoadAllPending(nodeID string) ([]*JournalEntry, error) {
	journalMu.Lock()
	defer journalMu.Unlock()
	return readJournal(nodeID)
}

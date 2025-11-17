// internal/client/loadbalancer.go
package client

import (
	"math/rand"
	"p2p-blob-storage/internal/bootstrap"
	"sync"
	"time"
)

// LoadBalancer implements Fisher-Yates shuffle-based load balancing
type LoadBalancer struct {
	peers   []*bootstrap.Node
	indices []int
	current int
	mu      sync.Mutex
	rng     *rand.Rand
}

func NewLoadBalancer(peers []*bootstrap.Node) *LoadBalancer {
	lb := &LoadBalancer{
		peers:   peers,
		indices: make([]int, len(peers)),
		current: 0,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Initialize indices
	for i := range lb.indices {
		lb.indices[i] = i
	}

	// Initial shuffle
	lb.shuffle()

	return lb
}

// GetNext returns the next peer using Fisher-Yates shuffle
func (lb *LoadBalancer) GetNext() *bootstrap.Node {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.peers) == 0 {
		return nil
	}

	// Get current peer
	peerIdx := lb.indices[lb.current]
	peer := lb.peers[peerIdx]

	// Move to next
	lb.current++

	// If we've gone through all peers, reshuffle
	if lb.current >= len(lb.indices) {
		lb.shuffle()
		lb.current = 0
	}

	return peer
}

// shuffle performs Fisher-Yates shuffle
func (lb *LoadBalancer) shuffle() {
	n := len(lb.indices)
	for i := n - 1; i > 0; i-- {
		j := lb.rng.Intn(i + 1)
		lb.indices[i], lb.indices[j] = lb.indices[j], lb.indices[i]
	}
}

// UpdatePeers updates the peer list and reshuffles
func (lb *LoadBalancer) UpdatePeers(peers []*bootstrap.Node) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.peers = peers
	lb.indices = make([]int, len(peers))

	for i := range lb.indices {
		lb.indices[i] = i
	}

	lb.shuffle()
	lb.current = 0
}

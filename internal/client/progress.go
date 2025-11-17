// internal/client/progress.go
package client

import (
	"fmt"
	"strings"
	"sync"
)

type ProgressBar struct {
	Total   int
	Current int
	Width   int
	mu      sync.Mutex
}

func (p *ProgressBar) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Current = 0
	p.render()
}

func (p *ProgressBar) Increment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Current++
	p.render()
}

func (p *ProgressBar) Finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Current = p.Total
	p.render()
	fmt.Println()
}

func (p *ProgressBar) render() {
	percent := float64(p.Current) / float64(p.Total)
	filled := int(percent * float64(p.Width))

	bar := strings.Repeat("█", filled) + strings.Repeat("░", p.Width-filled)

	fmt.Printf("\r[%s] %d/%d (%.1f%%)", bar, p.Current, p.Total, percent*100)
}

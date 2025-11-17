// cmd/root.go
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	bootstrapAddr     string
	listenPort        string
	nodeID            string
	clientStoragePath string
)

var rootCmd = &cobra.Command{
	Use:   "p2p-blob",
	Short: "P2P Distributed Blob Storage System",
	Long:  `A production-grade peer-to-peer distributed blob storage system with LAN support`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&bootstrapAddr, "bootstrap", "", "Bootstrap node address (e.g., 192.168.1.100:8000)")
	rootCmd.PersistentFlags().StringVar(&listenPort, "port", "8001", "Port to listen on")
	rootCmd.PersistentFlags().StringVar(&nodeID, "id", "", "Node ID (auto-generated if not provided)")
	// Optional storage path for client commands (where pending chunks are stored)
	rootCmd.PersistentFlags().StringVar(&clientStoragePath, "storage-path", "", "Path where client will store pending chunks (optional)")
}

func checkNode() error {
	if bootstrapAddr == "" {
		return fmt.Errorf("bootstrap address required. Use --bootstrap flag")
	}
	return nil
}

package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"p2p-blob-storage/internal/node"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	storagePath  string
	maxStorageGB float64
)

var storageCmd = &cobra.Command{
	Use:   "storage",
	Short: "Run a dedicated storage node",
	RunE: func(cmd *cobra.Command, args []string) error {
		if bootstrapAddr == "" {
			return fmt.Errorf("bootstrap address required. Use --bootstrap flag")
		}

		fmt.Printf("Starting storage node on port %s...\n", listenPort)

		// Create and start storage node
		// Convert GB to bytes for internal use
		maxStorageBytes := uint64(maxStorageGB * 1024 * 1024 * 1024)

		n, err := node.NewNode(nodeID, listenPort, bootstrapAddr, storagePath, maxStorageBytes)
		if err != nil {
			return fmt.Errorf("failed to create storage node: %w", err)
		}

		if err := n.Start(); err != nil {
			return fmt.Errorf("failed to start storage node: %w", err)
		}

		fmt.Printf("✓ Storage node running on port %s\n", listenPort)
		fmt.Printf("Connected to bootstrap: %s\n", bootstrapAddr)

		// Set up signal handling
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

		// Block until signal is received
		sig := <-sigCh
		fmt.Printf("\nReceived signal %v, shutting down storage node...\n", sig)

		// Perform graceful shutdown
		n.Stop()
		fmt.Println("Storage node stopped.")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(storageCmd)

	// Add storage-specific flags
	storageCmd.Flags().StringVarP(&storagePath, "storage-path", "s", "", "Path where chunks will be stored (required)")
	storageCmd.Flags().Float64VarP(&maxStorageGB, "max-size", "m", 1.0, "Maximum storage size in GB")
	storageCmd.MarkFlagRequired("storage-path")
}

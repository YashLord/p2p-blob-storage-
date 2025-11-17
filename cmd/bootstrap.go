// cmd/bootstrap.go
package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"p2p-blob-storage/internal/bootstrap"
	"syscall"

	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Start the bootstrap node",
	RunE: func(cmd *cobra.Command, args []string) error {
		port := listenPort
		if port == "8001" {
			port = "8000" // Default bootstrap port
		}

		fmt.Printf("Starting bootstrap node on port %s...\n", port)

		server, err := bootstrap.NewServer(port)
		if err != nil {
			return fmt.Errorf("failed to start bootstrap server: %w", err)
		}

		go func() {
			if err := server.Start(); err != nil {
				fmt.Printf("Bootstrap server error: %v\n", err)
			}
		}()

		fmt.Println("✓ Bootstrap node running")
		fmt.Printf("Peers can connect using: --bootstrap <your-ip>:%s\n", port)

		// Wait for interrupt signal
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh

		fmt.Println("\nShutting down bootstrap node...")
		server.Stop()

		return nil
	},
}

func init() {
	rootCmd.AddCommand(bootstrapCmd)
}

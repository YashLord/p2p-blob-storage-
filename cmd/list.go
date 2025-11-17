// cmd/list.go
package cmd

import (
	"fmt"
	"p2p-blob-storage/internal/client"

	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Show all files stored by current user",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkNode(); err != nil {
			return err
		}

		c, err := client.NewClient(bootstrapAddr, listenPort, nodeID, clientStoragePath)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer c.Close()

		files, err := c.ListFiles()
		if err != nil {
			return fmt.Errorf("failed to list files: %w", err)
		}

		if len(files) == 0 {
			fmt.Println("No files stored")
			return nil
		}

		fmt.Printf("\n%-64s %-20s %-15s %-10s\n", "HASH", "FILENAME", "SIZE", "CHUNKS")
		fmt.Println("--------------------------------------------------------------------------------------------------------")

		for _, f := range files {
			fmt.Printf("%-64s %-20s %-15s %-10d\n", f.Hash, f.Name, formatSize(f.Size), f.ChunkCount)
		}

		fmt.Printf("\nTotal: %d files\n", len(files))

		return nil
	},
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func init() {
	rootCmd.AddCommand(listCmd)
}

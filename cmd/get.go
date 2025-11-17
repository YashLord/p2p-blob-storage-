// cmd/get.go
package cmd

import (
	"fmt"
	"p2p-blob-storage/internal/client"

	"github.com/spf13/cobra"
)

var (
	outputPath   string
	deleteChunks bool
)

var getCmd = &cobra.Command{
	Use:   "get <hash>",
	Short: "Retrieve a file by content hash",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkNode(); err != nil {
			return err
		}

		hash := args[0]

		c, err := client.NewClient(bootstrapAddr, listenPort, nodeID, clientStoragePath)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer c.Close()

		retrievedPath, err := c.GetFile(hash, outputPath, deleteChunks)
		if err != nil {
			return fmt.Errorf("failed to retrieve file: %w", err)
		}

		fmt.Printf("\n✓ File retrieved successfully\n")
		fmt.Printf("Saved to: %s\n", retrievedPath)

		return nil
	},
}

func init() {
	getCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (default: original filename)")
	getCmd.Flags().BoolVarP(&deleteChunks, "delete-chunks", "d", false, "Delete remote chunk copies after successful retrieval")
	rootCmd.AddCommand(getCmd)
}

// cmd/store.go
package cmd

import (
	"fmt"
	"p2p-blob-storage/internal/client"

	"github.com/spf13/cobra"
)

var storeCmd = &cobra.Command{
	Use:   "store <file>",
	Short: "Upload and distribute a file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkNode(); err != nil {
			return err
		}

		filePath := args[0]

		c, err := client.NewClient(bootstrapAddr, listenPort, nodeID, clientStoragePath)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer c.Close()

		hash, err := c.StoreFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to store file: %w", err)
		}

		fmt.Printf("\n✓ File stored successfully\n")
		fmt.Printf("Content Hash: %s\n", hash)
		fmt.Printf("Use 'p2p-blob get %s' to retrieve\n", hash)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(storeCmd)
}

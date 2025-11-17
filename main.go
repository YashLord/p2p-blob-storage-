package main

import (
	"fmt"
	"os"
	"p2p-blob-storage/cmd"
)

// printBanner prints a colorful ASCII banner for GOStore.
// Uses ANSI escape sequences for color — modern Windows terminals support these.
func printBanner() {
	// Large cyan ASCII banner for GOStore
	fmt.Print("\n")
	cyan := "\x1b[1;36m" // bright cyan
	reset := "\x1b[0m"

	fmt.Print(cyan)
	fmt.Println("  ██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗")
	fmt.Println(" ██╔════╝ ██╔═══██╗██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔════╝")
	fmt.Println(" ██║  ███╗██║   ██║███████╗   ██║   ██║   ██║██████╔╝█████╗  ")
	fmt.Println(" ██║   ██║██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ")
	fmt.Println(" ╚██████╔╝╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗")
	fmt.Println("  ╚═════╝  ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝")
	fmt.Print(reset)

	// Short description in normal text
	fmt.Print(cyan)
	fmt.Println("\n  GOStore — lightweight P2P blob storage for LANs and small clusters")
	fmt.Print(reset)
	fmt.Println()
}

func main() {
	printBanner()
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/mcp"
)

func main() {
	if err := mcp.Run(context.Background(), config.Load()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

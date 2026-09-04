package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/meln1k/gotel/internal/cleanup"
	"github.com/meln1k/gotel/internal/cli"
	"github.com/meln1k/gotel/internal/config"
	"github.com/meln1k/gotel/internal/daemon"
	"github.com/meln1k/gotel/internal/mcp"
)

func Run(args []string) error {
	cfg := config.Load()
	if len(args) == 0 || oneOf(args[0], "tui", "ui", "help", "--help", "-h") {
		fmt.Print(cli.Usage())
		return nil
	}
	cfg = config.LoadManaged()
	ctx := context.Background()
	switch args[0] {
	case "start", "daemon":
		status, err := daemon.Ensure(ctx, cfg)
		if err != nil {
			return err
		}
		return daemon.PrintStatus(status)
	case "status":
		return daemon.PrintStatus(daemon.GetStatus(ctx, cfg))
	case "stop":
		status, err := daemon.Stop(ctx, cfg)
		if err != nil {
			return err
		}
		return daemon.PrintStatus(status)
	case "restart":
		status, err := daemon.Restart(ctx, cfg)
		if err != nil {
			return err
		}
		return daemon.PrintStatus(status)
	case "server":
		signalContext, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer cancel()
		return daemon.RunServer(signalContext, cfg)
	case "mcp":
		return mcp.Run(ctx, cfg)
	case "clear-debug":
		if len(args) > 1 && oneOf(args[1], "help", "--help", "-h") {
			fmt.Print("Usage: gotel clear-debug [path]\n\nRemoves blocks wrapped in '#region gotel debug' and '#endregion gotel debug' from JS/TS files under the given path. Defaults to the current directory.\n")
			return nil
		}
		root := "."
		if len(args) > 1 && args[1] != "" {
			root = args[1]
		}
		changed, err := cleanup.Run(root)
		if err != nil {
			return err
		}
		absoluteRoot, _ := filepath.Abs(root)
		if len(changed) == 0 {
			fmt.Printf("No '#region gotel debug' blocks found under %s\n", absoluteRoot)
			return nil
		}
		fmt.Printf("Removed Gotel debug blocks from %d file(s):\n", len(changed))
		for _, file := range changed {
			fmt.Printf("- %s\n", file)
		}
		return nil
	default:
		handled, err := cli.Run(ctx, cfg, args)
		if err != nil {
			return err
		}
		if !handled {
			fmt.Print(cli.Usage())
		}
		return nil
	}
}

func oneOf(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}

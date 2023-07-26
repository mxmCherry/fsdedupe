package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mxmCherry/fsdedupe"
)

func main() {
	if err := run(); err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()

	dirs := os.Args[1:]
	if len(dirs) != 1 {
		return fmt.Errorf("one and only one argument (dir path) is expected, got %+v", dirs)
	}

	return fsdedupe.DedupeDirSymlink(ctx, dirs[0])
}

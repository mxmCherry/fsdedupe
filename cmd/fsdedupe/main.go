package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/google/subcommands"
	"github.com/mxmCherry/fsdedupe"
)

var selfCmd = filepath.Base(os.Args[0])

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()

	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&dirSymlink{}, "")

	flag.Parse()
	os.Exit(int(subcommands.Execute(ctx)))
}

// ----------------------------------------------------------------------------

type dirSymlink struct{}

func (*dirSymlink) Name() string     { return "dir-symlink" }
func (*dirSymlink) Synopsis() string { return "Deduplicate dir files using symlinks" }
func (*dirSymlink) Usage() string {
	return selfCmd + ` dir-symlink <path-to-dir>
	Recursively deduplicate <path-to-dir>'s files by replacing duplicates (by SHA512 content hash) with symlinks.
	If no <path-to-dir> provided - defaults to current directory.
`
}

func (p *dirSymlink) SetFlags(f *flag.FlagSet) {}

func (p *dirSymlink) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	dir := "."
	if args := f.Args(); len(args) == 1 {
		dir = args[0]
	} else if len(args) > 1 {
		fmt.Fprintf(os.Stderr, "only one directory path is expected, got %+v\n", args)
		return subcommands.ExitUsageError
	}

	if err := fsdedupe.DedupeDirSymlink(ctx, dir); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

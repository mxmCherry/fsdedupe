package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

type dirSymlink struct {
	verbose bool
}

func (*dirSymlink) Name() string     { return "dir-symlink" }
func (*dirSymlink) Synopsis() string { return "Deduplicate dir files using symlinks" }
func (*dirSymlink) Usage() string {
	return selfCmd + ` dir-symlink [-v] <path-to-dir>
	Recursively deduplicate <path-to-dir>'s files by replacing duplicates (by SHA512 content hash) with symlinks.
	It skips hidden (dot-prefixed, like ".git" or ".bashrc") entries.
	The "-v" flag enables action logging.
	If no <path-to-dir> provided - defaults to current directory.
`
}

func (c *dirSymlink) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&c.verbose, "v", false, "verbose (log actions)")
}

func (c *dirSymlink) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	dir := "."
	if args := f.Args(); len(args) == 1 {
		dir = args[0]
	} else if len(args) > 1 {
		fmt.Fprintf(os.Stderr, "only one directory path is expected, got %+v\n", args)
		return subcommands.ExitUsageError
	}

	var logger *log.Logger
	if c.verbose {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if err := fsdedupe.DedupeDirSymlink(ctx, dir, logger); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

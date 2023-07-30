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
	subcommands.Register(&symlink{}, "")

	flag.Parse()
	os.Exit(int(subcommands.Execute(ctx)))
}

// ----------------------------------------------------------------------------

type symlink struct{}

func (*symlink) Name() string { return "symlink" }
func (*symlink) Synopsis() string {
	return "Deduplicate STDIN filenames by symlinking same-content ones"
}
func (*symlink) Usage() string {
	return `find <SOMEDIR> -type f -not -path '*/.*' | ` + selfCmd + ` symlink
	Deduplicate STDIN-provided filenames by symlinking same-content ones (SHA512) to the first-seen one.
	Provided "find ..." snippet excludes UNIX hidden files (dot-prefixed).
`
}

func (c *symlink) SetFlags(f *flag.FlagSet) {}

func (c *symlink) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	it := fsdedupe.Lines(os.Stdin)
	if err := fsdedupe.DedupeSymlink(ctx, it); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

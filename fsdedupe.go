package fsdedupe

import (
	"bufio"
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Iterator defines a string (filename) iterator.
// It is expected to return io.EOF on no more entries.
type Iterator interface {
	Next() (string, error)
}

type lines struct {
	scanner *bufio.Scanner
}

// Lines is an Iterator-adapter for an io.Reader (os.Stdin etc).
// It strips leading/trailing whitespaces and skips empty lines.
func Lines(r io.Reader) Iterator {
	return &lines{
		scanner: bufio.NewScanner(r),
	}
}

func (l *lines) Next() (string, error) {
	if !l.scanner.Scan() {
		if err := l.scanner.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}

	line := strings.TrimSpace(l.scanner.Text())
	if line == "" {
		return l.Next()
	}
	return line, nil
}

// ----------------------------------------------------------------------------

// DedupeSymlink deduplicates input filenames
// by symlinking files to the first-seen file
// by SHA512 content hash.
func DedupeSymlink(ctx context.Context, filenames Iterator) error {
	byHash := make(map[string]string)
	digest := sha512.New()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		filename, err := filenames.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return filepath.ErrBadPattern
		}

		println("filename", filename)

		stat, err := os.Stat(filename)
		if err != nil {
			return fmt.Errorf("stat %q: %w", filename, err)
		}
		if !stat.Mode().IsRegular() {
			return fmt.Errorf("not a regular file: %q", filename)
		}

		digest.Reset()
		hash, err := hashContents(digest, filename)
		if err != nil {
			return fmt.Errorf("hash contents of %q: %w", filename, err)
		}

		existing, ok := byHash[hash]
		if !ok {
			byHash[hash] = filename
			continue
		}

		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("remove %q: %w", filename, err)
		}
		if err := os.Symlink(existing, filename); err != nil {
			return fmt.Errorf("symlink %q -> %q: %w", filename, existing, err)
		}
	}

	return nil
}

func hashContents(d hash.Hash, filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(d, f); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}

	return fmt.Sprintf("%x", d.Sum(nil)), nil
}

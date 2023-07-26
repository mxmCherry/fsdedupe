package fsdedupe

import (
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DedupeDirSymlink deduplicates regular files by it's content hash (SHA512),
// and replaces duplicate file with a symlink to previously seen one.
func DedupeDirSymlink(ctx context.Context, path string, logger *log.Logger) error {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	byHash := make(map[string]string)

	cb := func(path string, entry fs.DirEntry) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if strings.HasPrefix(entry.Name(), ".") {
			logger.Printf("skipping hidden entry %q\n", path)
			return fs.SkipDir
		}

		if !entry.Type().IsRegular() {
			logger.Printf("skipping non-regular file %q\n", path)
			return nil
		}

		logger.Printf("checking regular file %q...\n", path)

		hash, err := fileHash(path)
		if err != nil {
			return fmt.Errorf("hash file: %w", err)
		}

		existing, ok := byHash[hash]
		if !ok {
			byHash[hash] = path
			logger.Printf("unique so far: %q (hash: %s)\n", path, hash)
			return nil
		}

		tmpLinkName := path + ".tmp-" + fmt.Sprintf("%d", time.Now().UnixNano())
		if err := os.Symlink(existing, tmpLinkName); err != nil {
			return fmt.Errorf("symlink %q -> %q: %w", existing, tmpLinkName, err)
		}

		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove %q: %w", path, err)
		}

		if err := os.Rename(tmpLinkName, path); err != nil {
			return fmt.Errorf("rename %q -> %q: %w", tmpLinkName, path, err)
		}

		logger.Printf("symlinked %q -> %q (hash: %s)\n", path, existing, hash)

		return nil
	}

	if err := walk(path, cb); err != nil {
		return fmt.Errorf("walk: %w", err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func walk(path string, cb func(string, fs.DirEntry) error) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer dir.Close()

	for {
		entries, err := dir.ReadDir(1)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("read dir: %w", err)
		}

		for _, entry := range entries {
			path := filepath.Join(path, entry.Name())

			if err := cb(path, entry); errors.Is(err, fs.SkipDir) {
				continue
			} else if err != nil {
				return fmt.Errorf("cb: %w", err)
			}

			if entry.IsDir() {
				if err := walk(path, cb); err != nil {
					return fmt.Errorf("recurse into %q: %w", path, err)
				}
			}
		}
	}
}

func fileHash(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	digest := sha512.New()

	if _, err := io.Copy(digest, f); err != nil {
		return "", fmt.Errorf("copy: %w", err)
	}

	return fmt.Sprintf("%X", digest.Sum(nil)), nil
}

package fsdedupe

import (
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// DedupeDirSymlink deduplicates regular files by it's content hash (SHA512),
// and replaces duplicate file with a symlink to previously seen one.
func DedupeDirSymlink(ctx context.Context, path string) error {
	byHash := make(map[string]string)

	cb := func(entry fs.DirEntry, fullpath string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !entry.Type().IsRegular() {
			return nil
		}

		hash, err := fileHash(fullpath)
		if err != nil {
			return fmt.Errorf("hash file: %w", err)
		}

		existing, ok := byHash[hash]
		if !ok {
			byHash[hash] = fullpath
			return nil
		}

		tmpLinkName := fullpath + ".tmp-" + fmt.Sprintf("%d", time.Now().UnixNano())
		if err := os.Symlink(existing, tmpLinkName); err != nil {
			return fmt.Errorf("symlink %q -> %q: %w", existing, tmpLinkName, err)
		}

		if err := os.Remove(fullpath); err != nil {
			return fmt.Errorf("remove %q: %w", fullpath, err)
		}

		if err := os.Rename(tmpLinkName, fullpath); err != nil {
			return fmt.Errorf("rename %q -> %q: %w", tmpLinkName, fullpath, err)
		}

		return nil
	}

	if err := walk(path, cb); err != nil {
		return fmt.Errorf("walk: %w", err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func walk(path string, cb func(fs.DirEntry, string) error) error {
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
			fullpath := filepath.Join(path, entry.Name())
			if err := cb(entry, fullpath); err != nil {
				return fmt.Errorf("cb: %w", err)
			}

			if entry.IsDir() {
				if err := walk(fullpath, cb); err != nil {
					return fmt.Errorf("recurse into %q: %w", fullpath, err)
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

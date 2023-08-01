package fsdedupe

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// DedupeFS is a deduplicated files manager.
// It keeps files in one dir by their content hash (SHA512),
// and symlinks (with human-ish names) to them in another dir.
type DedupeFS struct {
	tempDir string
	dataDir string
	linkDir string
	dirPerm os.FileMode
}

// NewDedupeFS constructs a new DedupeFS with given details.
func NewDedupeFS(
	tempDir string,
	dataDir string,
	linkDir string,
	dirPerm os.FileMode,
) (*DedupeFS, error) {
	var err error

	if filepath.IsLocal(tempDir) {
		if tempDir, err = filepath.Abs(tempDir); err != nil {
			return nil, fmt.Errorf("resolve abs path for tempDir: %w", err)
		}
	}
	if filepath.IsLocal(dataDir) {
		if dataDir, err = filepath.Abs(dataDir); err != nil {
			return nil, fmt.Errorf("resolve abs path for dataDir: %w", err)
		}
	}

	if filepath.IsLocal(linkDir) {
		if linkDir, err = filepath.Abs(linkDir); err != nil {
			return nil, fmt.Errorf("resolve abs path for linkDir: %w", err)
		}
	}

	if dirPerm == 0 {
		dirPerm = 0700
	}

	return &DedupeFS{
		tempDir: tempDir,
		dataDir: dataDir,
		linkDir: linkDir,
		dirPerm: dirPerm,
	}, nil
}

// Create creates or truncates/opens existing file to be written by caller.
func (s *DedupeFS) Create(linkName string) (io.WriteCloser, error) {
	absLinkName := filepath.Join(
		s.linkDir,
		filepath.Join(string(filepath.Separator), linkName),
	)
	return createFile(s.tempDir, s.dataDir, absLinkName, s.dirPerm)
}

// Open opens the file for reading.
func (s *DedupeFS) Open(linkName string) (io.ReadCloser, error) {
	absLinkName := filepath.Join(
		s.linkDir,
		filepath.Join(string(filepath.Separator), linkName),
	)
	return os.Open(absLinkName)
}

// Rename renames (moves) the file.
func (s *DedupeFS) Rename(oldLinkName, newLinkName string) error {
	cleanOldLinkName := filepath.Join(string(filepath.Separator), oldLinkName)
	absOldLinkName := filepath.Join(
		s.linkDir,
		cleanOldLinkName,
	)
	absNewLinkName := filepath.Join(
		s.linkDir,
		filepath.Join(string(filepath.Separator), newLinkName),
	)

	if err := os.MkdirAll(filepath.Dir(absNewLinkName), s.dirPerm); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	if err := os.Rename(absOldLinkName, absNewLinkName); err != nil {
		return fmt.Errorf("rename %q -> %q: %w", absOldLinkName, absNewLinkName, err)
	}

	if err := cleanTree(s.linkDir, filepath.Dir(cleanOldLinkName)); err != nil {
		return fmt.Errorf("clean tree of %q: %w", cleanOldLinkName, err)
	}
	return nil
}

// Remove removes the file.
func (s *DedupeFS) Remove(linkName string) error {
	cleanLinkName := filepath.Join(string(filepath.Separator), linkName)
	absLinkName := filepath.Join(
		s.linkDir,
		cleanLinkName,
	)
	if err := os.RemoveAll(absLinkName); err != nil {
		return fmt.Errorf("rm: %w", err)
	}

	if err := cleanTree(s.linkDir, filepath.Dir(cleanLinkName)); err != nil {
		return fmt.Errorf("clean tree: %w", err)
	}
	return nil
}

// GC removes unreferenced data files.
func (s *DedupeFS) GC() error {
	dataFiles := make(map[string]struct{})

	collectDataFiles := func(path string, entry os.DirEntry) error {
		if !entry.Type().IsRegular() {
			return fs.SkipDir
		}
		dataFiles[path] = struct{}{}
		return nil
	}
	if err := walk(s.dataDir, collectDataFiles); err != nil {
		return fmt.Errorf("walk %q: %w", s.dataDir, err)
	}

	onLink := func(path string, entry os.DirEntry) error {
		// skip non-links
		if entry.Type()&fs.ModeSymlink == 0 {
			return nil
		}

		target, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("readlink %q: %w", path, err)
		}

		delete(dataFiles, target)

		return nil
	}
	if err := walk(s.linkDir, onLink); err != nil {
		return fmt.Errorf("walk %q: %w", s.linkDir, err)
	}

	// any data-link remains there to be reaped?
	if len(dataFiles) == 0 {
		return nil
	}

	for dataFile := range dataFiles {
		if err := os.RemoveAll(dataFile); err != nil {
			return fmt.Errorf("remove %q: %w", dataFile, err)
		}
	}

	return nil
}

// ----------------------------------------------------------------------------

type fileWriter struct {
	io.Writer

	tempFileName string
	dataDir      string
	absLinkName  string
	dirPerm      os.FileMode

	tempFile *os.File
	digest   hash.Hash
}

func createFile(tempDir, dataDir, absLinkName string, dirPerm os.FileMode) (*fileWriter, error) {
	tempFileName := filepath.Join(tempDir, fmt.Sprintf("%d.bin", time.Now().UnixNano()))

	if err := os.MkdirAll(filepath.Dir(tempFileName), dirPerm); err != nil {
		return nil, fmt.Errorf("ensure dir for %q: %w", tempFileName, err)
	}

	tempFile, err := os.Create(tempFileName)
	if err != nil {
		return nil, fmt.Errorf("create temp file %q: %w", tempFileName, err)
	}

	digest := sha512.New()

	return &fileWriter{
		Writer: io.MultiWriter(tempFile, digest),

		tempFileName: tempFileName,
		dataDir:      dataDir,
		absLinkName:  absLinkName,
		dirPerm:      dirPerm,

		tempFile: tempFile,
		digest:   digest,
	}, nil
}

func (f *fileWriter) Close() error {
	if err := f.tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file %q: %w", f.tempFileName, err)
	}

	absDataName := filepath.Join(
		f.dataDir,
		fmt.Sprintf("%x", f.digest.Sum(nil))+".bin",
	)

	if err := os.MkdirAll(filepath.Dir(absDataName), f.dirPerm); err != nil {
		return fmt.Errorf("ensure dir for %q: %w", absDataName, err)
	}

	if err := os.Rename(f.tempFileName, absDataName); err != nil {
		return fmt.Errorf("rename temp file %q into data file %q: %w", f.tempFileName, absDataName, err)
	}

	if err := os.MkdirAll(filepath.Dir(f.absLinkName), f.dirPerm); err != nil {
		return fmt.Errorf("ensure dir for %q: %w", f.absLinkName, err)
	}

	if err := os.Symlink(absDataName, f.absLinkName); err != nil {
		return fmt.Errorf("symlink %q pointing to data file %q: %w", f.absLinkName, absDataName, err)
	}

	return nil
}

// ----------------------------------------------------------------------------

func cleanTree(root, dir string) error {
	for dir != string(filepath.Separator) {
		absDir := filepath.Join(root, dir)

		empty, err := isDirEmpty(absDir)
		if err != nil {
			return fmt.Errorf("check empty %q: %w", absDir, err)
		}
		if !empty {
			return nil
		}

		if err := os.RemoveAll(absDir); err != nil {
			return fmt.Errorf("rm %q: %w", absDir, err)
		}

		dir = filepath.Dir(dir)
	}

	return nil
}

func isDirEmpty(dir string) (bool, error) {
	d, err := os.Open(dir)
	if err != nil {
		return false, fmt.Errorf("open: %w", err)
	}
	defer d.Close()

	names, err := d.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

func walk(path string, cb func(string, os.DirEntry) error) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	for {
		entries, err := d.ReadDir(1)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("readdir: %w", err)
		}

		for _, entry := range entries {
			childPath := filepath.Join(path, entry.Name())
			if err := cb(childPath, entry); errors.Is(err, fs.SkipDir) {
				continue
			} else if err != nil {
				return fmt.Errorf("cb %q: %w", childPath, err)
			}

			if entry.IsDir() {
				if err := walk(childPath, cb); err != nil {
					return fmt.Errorf("walk %q: %w", childPath, err)
				}
			}
		}
	}

	return nil
}

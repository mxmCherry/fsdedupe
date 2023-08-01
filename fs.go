package fsdedupe

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FS is a deduplicated files manager.
// It keeps files in one dir by their content hash (SHA512),
// and symlinks (with human-ish names) to them in another dir.
type FS struct {
	tempDir string
	dataDir string
	linkDir string
	dirPerm os.FileMode
}

// NewFS constructs a new FS with given details.
func NewFS(
	tempDir string,
	dataDir string,
	linkDir string,
	dirPerm os.FileMode,
) (*FS, error) {
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

	return &FS{
		tempDir: tempDir,
		dataDir: dataDir,
		linkDir: linkDir,
		dirPerm: dirPerm,
	}, nil
}

// Create creates or truncates/opens existing file to be written by caller.
func (s *FS) Create(linkName string) (io.WriteCloser, error) {
	absLinkName := filepath.Join(
		s.linkDir,
		filepath.Join(string(filepath.Separator), linkName),
	)
	return createFile(s.tempDir, s.dataDir, absLinkName, s.dirPerm)
}

// Open opens the file for reading.
func (s *FS) Open(linkName string) (io.ReadCloser, error) {
	absLinkName := filepath.Join(
		s.linkDir,
		filepath.Join(string(filepath.Separator), linkName),
	)
	return os.Open(absLinkName)
}

// Rename renames (moves) the file.
func (s *FS) Rename(oldLinkName, newLinkName string) error {
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
func (s *FS) Remove(linkName string) error {
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
func (s *FS) GC() error {
	return errors.New("not implemented") // TODO: implement GC-ing
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

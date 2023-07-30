package fsdedupe_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mxmCherry/fsdedupe"
)

func TestLines(t *testing.T) {
	r := strings.NewReader(`
		Indent
		Does not
		Matter
	`)
	it := fsdedupe.Lines(r)

	if line, err := it.Next(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	} else if actual, expected := line, "Indent"; actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}

	if line, err := it.Next(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	} else if actual, expected := line, "Does not"; actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}

	if line, err := it.Next(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	} else if actual, expected := line, "Matter"; actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestDedupeSymlink(t *testing.T) {
	tmp := t.TempDir()

	file1 := filepath.Join(tmp, "file1.txt")
	writeFile(t, file1, "DUPE")

	file2 := filepath.Join(tmp, "file2.txt")
	writeFile(t, file2, "UNIQ")

	file3 := filepath.Join(tmp, "file3.txt")
	writeFile(t, file3, "DUPE")

	file4 := filepath.Join(tmp, "sub", "dir", "file4.txt")
	writeFile(t, file4, "DUPE")

	it := &simpleIterator{
		Entries: []string{
			file1,
			file2,
			file3,
			file4,
		},
	}
	if err := fsdedupe.DedupeSymlink(context.Background(), it); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	// file1 - kept as is (first-seen of duplicates)
	stat1, err := os.Stat(file1)
	if err != nil {
		t.Fatalf("stat %q: %s", file1, err)
	}
	if !stat1.Mode().IsRegular() {
		t.Errorf("expected %q to be a regular file, but it is not", file1)
	}

	// file1 - kept as is (unique)
	stat2, err := os.Stat(file2)
	if err != nil {
		t.Fatalf("stat %q: %s", file2, err)
	}
	if !stat2.Mode().IsRegular() {
		t.Errorf("expected %q to be a regular file, but it is not", file2)
	}

	// file3 -> file1 (symlink-aliased duplicate)
	if focus, actual, expected := file3, readlink(t, file3), file1; actual != expected {
		t.Errorf("expected %q to point to %q, but got: %q", focus, expected, actual)
	}

	// file4 -> file1 (symlink-aliased duplicate)
	if focus, actual, expected := file4, readlink(t, file4), file1; actual != expected {
		t.Errorf("expected %q to point to %q, but got: %q", focus, expected, actual)
	}
}

// ----------------------------------------------------------------------------

type simpleIterator struct {
	Entries []string
}

func (i *simpleIterator) Next() (string, error) {
	if len(i.Entries) == 0 {
		return "", io.EOF
	}

	head := i.Entries[0]
	i.Entries = i.Entries[1:]
	return head, nil
}

func writeFile(t *testing.T, name string, contents string) {
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir %q: %s", dir, err)
	}

	f, err := os.Create(name)
	if err != nil {
		t.Fatalf("create %q: %s", name, err)
	}
	defer f.Close()

	if _, err := io.WriteString(f, contents); err != nil {
		t.Fatalf("write %q: %s", name, err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close %q: %s", name, err)
	}
}

func readlink(t *testing.T, name string) string {
	target, err := os.Readlink(name)
	if err != nil {
		t.Fatalf("readlink %q: %s", name, err)
	}

	return target
}

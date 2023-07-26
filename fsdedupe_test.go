package fsdedupe_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mxmCherry/fsdedupe"
	"golang.org/x/exp/slices"
)

func TestDedupeDirSymlink(t *testing.T) {
	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("mkdirtemp: %s", err)
	}
	// defer os.RemoveAll(tmp)

	file1 := filepath.Join(tmp, "file1.txt")
	writeFile(t, file1, "DUPE")

	file2 := filepath.Join(tmp, "file2.txt")
	writeFile(t, file2, "UNIQ")

	file3 := filepath.Join(tmp, "file3.txt")
	writeFile(t, file3, "DUPE")

	file4 := filepath.Join(tmp, "sub", "dir", "file4.txt")
	writeFile(t, file4, "DUPE")

	if err := fsdedupe.DedupeDirSymlink(context.Background(), tmp); err != nil {
		t.Fatalf("dedupesymlink %q: %s", tmp, err)
	}

	var regular []string
	var symlinked []string
	err = filepath.Walk(tmp, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if info.Mode().IsRegular() {
			regular = append(regular, path)
		} else {
			symlinked = append(symlinked, path, readlink(t, path))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %s", tmp, err)
	}

	if actual, expected := symlinked, []string{file1, file3, file4}; !consistsOf(actual, expected...) {
		t.Fatalf("expected %+v to consist of %+v", actual, expected)
	}

	if actual, expected := regular, file2; !slices.Contains(actual, expected) {
		t.Fatalf("expected %+v to contain %+v", actual, expected)
	}
}

// ----------------------------------------------------------------------------

func writeFile(t *testing.T, name string, contents string) {
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdirall %q: %s", dir, err)
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

func consistsOf(x []string, elems ...string) bool {
	slices.Sort(x)
	slices.Sort(elems)

	x = slices.Compact(x)
	elems = slices.Compact(elems)

	return slices.Equal(x, elems)
}

package fsdedupe_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mxmCherry/fsdedupe"
)

func TestFS_Create(t *testing.T) {
	tmp := t.TempDir()
	subject := setupFS(t, tmp)

	const name = "sub/dir/file.txt"
	const contents = "DUMMY"
	const contentsHash = "0a8649de6b948fac1722c82ee07f4e3e8386a071750daf23c56fbba31acc922323b362fe10327e7e3322bc9354df59e02ded56f7f6f0ebfd6e99702154299d51" // echo -n DUMMY | sha512sum

	setupFS_Create(t, subject, name, contents)

	absLinkPath := filepath.Join(tmp, "link", name)
	b, err := os.ReadFile(absLinkPath)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
	if actual, expected := string(b), "DUMMY"; err != nil {
		t.Errorf("expected %q, got %q", actual, expected)
	}

	absDataPath := filepath.Join(tmp, "data", contentsHash+".bin")
	if actual, expected := readlink(t, absLinkPath), absDataPath; actual != expected {
		t.Errorf("expected link %q to point to point to content-hash-ed data file %q, got this instead: %q", contentsHash, expected, actual)
	}
}

func TestFS_Read(t *testing.T) {
	tmp := t.TempDir()
	subject := setupFS(t, tmp)

	const name = "sub/dir/file.txt"
	const contents = "DUMMY"

	setupFS_Create(t, subject, name, contents)

	r, err := subject.Open(name)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	if actual, expected := string(b), contents; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestFS_Rename(t *testing.T) {
	tmp := t.TempDir()
	subject := setupFS(t, tmp)

	const oldName = "sub/dir/file.txt"
	const newName = "another/sub/dir/file.txt"
	const contents = "DUMMY"

	setupFS_Create(t, subject, oldName, contents)

	absOldName := filepath.Join(tmp, "link", oldName)
	absNewName := filepath.Join(tmp, "link", newName)

	oldLink := readlink(t, absOldName)

	if err := subject.Rename(oldName, newName); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	_, err := os.Stat(filepath.Join(tmp, "link", oldName))
	if !os.IsNotExist(err) {
		t.Errorf("expected old parent dir to be gone, but it still exists")
	}

	newLink := readlink(t, absNewName)
	if actual, expected := newLink, oldLink; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestFS_Remove(t *testing.T) {
	tmp := t.TempDir()
	subject := setupFS(t, tmp)

	const name = "sub/dir/file.txt"
	const contents = "DUMMY"
	const contentsHash = "0a8649de6b948fac1722c82ee07f4e3e8386a071750daf23c56fbba31acc922323b362fe10327e7e3322bc9354df59e02ded56f7f6f0ebfd6e99702154299d51" // echo -n DUMMY | sha512sum

	setupFS_Create(t, subject, name, contents)

	if err := subject.Remove(name); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	// this, obviously, asserts that link is gone as well
	_, err := os.Stat(filepath.Join(tmp, "link", "sub"))
	if !os.IsNotExist(err) {
		t.Errorf("expected old parent dir to be gone, but it still exists")
	}

	// data file is kept
	absDataPath := filepath.Join(tmp, "data", contentsHash+".bin")
	if _, err := os.Stat(absDataPath); err != nil {
		t.Fatalf("expected data file to still exist, but got: %v", err)
	}
}

func TestFS_GC(t *testing.T) {
	tmp := t.TempDir()
	subject := setupFS(t, tmp)

	const name = "sub/dir/file.txt"
	const contents = "DUMMY"
	const contentsHash = "0a8649de6b948fac1722c82ee07f4e3e8386a071750daf23c56fbba31acc922323b362fe10327e7e3322bc9354df59e02ded56f7f6f0ebfd6e99702154299d51" // echo -n DUMMY | sha512sum

	setupFS_Create(t, subject, name, contents)

	// GC 1, have SOME links pointing to data file

	if err := subject.GC(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	// link gone, data-file kept
	if err := subject.Remove(name); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	absDataPath := filepath.Join(tmp, "data", contentsHash+".bin")
	if _, err := os.Stat(absDataPath); err != nil {
		t.Fatalf("expected data file to still exist, but got: %v", err)
	}

	// GC 2, have NO links pointing to data file

	if err := subject.GC(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	if _, err := os.Stat(absDataPath); !os.IsNotExist(err) {
		t.Fatalf("expected data file to be gone, but got: %v", err)
	}
}

// ----------------------------------------------------------------------------

func setupFS(t *testing.T, tmp string) *fsdedupe.FS {
	fs, err := fsdedupe.NewFS(
		filepath.Join(tmp, "temp"),
		filepath.Join(tmp, "data"),
		filepath.Join(tmp, "link"),
		0700,
	)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}

	return fs
}

func setupFS_Create(t *testing.T, fs *fsdedupe.FS, name, contents string) {
	f, err := fs.Create(name)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
	defer f.Close()

	if _, err := io.WriteString(f, contents); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
}

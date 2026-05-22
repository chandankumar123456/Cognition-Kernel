package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "deep")
	r := Execute("create_dir", map[string]interface{}{"path": dir})
	if r.Error != "" {
		t.Fatalf("create_dir failed: %s", r.Error)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("dir not created: %v", err)
	}
}

func TestWriteAndReadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.txt")
	r := Execute("write_file", map[string]interface{}{"path": path, "content": "hello world"})
	if r.Error != "" {
		t.Fatalf("write_file failed: %s", r.Error)
	}
	r = Execute("read_file", map[string]interface{}{"path": path})
	if r.Error != "" {
		t.Fatalf("read_file failed: %s", r.Error)
	}
	if r.Output != "hello world" {
		t.Fatalf("expected 'hello world', got: %s", r.Output)
	}
}

func TestDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "todelete.txt")
	os.WriteFile(path, []byte("x"), 0644)
	r := Execute("delete", map[string]interface{}{"path": path})
	if r.Error != "" {
		t.Fatalf("delete failed: %s", r.Error)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file still exists after delete")
	}
}

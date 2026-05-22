package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
)

type Result struct {
	Output string
	Error  string
}

func Execute(action string, params map[string]interface{}) Result {
	switch action {
	case "create_dir":
		return createDir(params)
	case "write_file":
		return writeFile(params)
	case "read_file":
		return readFile(params)
	case "delete":
		return deleteEntry(params)
	default:
		return Result{Error: fmt.Sprintf("unknown action: %s", action)}
	}
}

func getString(params map[string]interface{}, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func createDir(params map[string]interface{}) Result {
	path := getString(params, "path")
	if path == "" {
		return Result{Error: "path required"}
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return Result{Error: err.Error()}
	}
	return Result{Output: "created"}
}

func writeFile(params map[string]interface{}) Result {
	path := getString(params, "path")
	content := getString(params, "content")
	if path == "" {
		return Result{Error: "path required"}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return Result{Error: err.Error()}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0644); err != nil {
		return Result{Error: err.Error()}
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return Result{Error: err.Error()}
	}
	return Result{Output: "written"}
}

func readFile(params map[string]interface{}) Result {
	path := getString(params, "path")
	if path == "" {
		return Result{Error: "path required"}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{Error: err.Error()}
	}
	return Result{Output: string(data)}
}

func deleteEntry(params map[string]interface{}) Result {
	path := getString(params, "path")
	if path == "" {
		return Result{Error: "path required"}
	}
	if err := os.RemoveAll(path); err != nil {
		return Result{Error: err.Error()}
	}
	return Result{Output: "deleted"}
}

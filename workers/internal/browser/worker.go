package browser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
)

type Result struct {
	Success bool
	Output  string
}

func Execute(operation string, params map[string]interface{}, workDir string) Result {
	bridgePath := filepath.Join(workDir, "workers", "browser_bridge.py")

	pythonCmd := "python"
	if runtime.GOOS == "windows" {
		venvPython := filepath.Join(workDir, "cognition", ".venv", "Scripts", "python.exe")
		if _, err := exec.LookPath(venvPython); err == nil {
			pythonCmd = venvPython
		}
	}

	cmd := exec.Command(pythonCmd, bridgePath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Result{false, fmt.Sprintf("stdin pipe error: %v", err)}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{false, fmt.Sprintf("stdout pipe error: %v", err)}
	}

	if err := cmd.Start(); err != nil {
		return Result{false, fmt.Sprintf("failed to start browser bridge: %v", err)}
	}

	action := map[string]interface{}{"operation": operation}
	for k, v := range params {
		action[k] = v
	}

	data, _ := json.Marshal(action)
	fmt.Fprintf(stdin, "%s\n", data)
	stdin.Close()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	var resultLine string
	for scanner.Scan() {
		resultLine = scanner.Text()
	}
	cmd.Wait()

	if resultLine == "" {
		return Result{false, "no output from browser bridge"}
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultLine), &result); err != nil {
		return Result{false, fmt.Sprintf("parse result failed: %v", err)}
	}

	success, _ := result["success"].(bool)
	output, _ := result["output"].(string)
	return Result{success, output}
}

package shell

import (
	"context"
	"os/exec"
	"runtime"
	"time"
)

type Result struct {
	Output   string
	Error    string
	ExitCode int
	Duration time.Duration
}

func Execute(command, workDir string, timeoutMs uint64) Result {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/C", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}

	if workDir != "" {
		cmd.Dir = workDir
	}

	start := time.Now()
	out, err := cmd.CombinedOutput()
	duration := time.Since(start)

	r := Result{Output: string(out), Duration: duration}
	if err != nil {
		r.Error = err.Error()
		if exitErr, ok := err.(*exec.ExitError); ok {
			r.ExitCode = exitErr.ExitCode()
		} else {
			r.ExitCode = -1
		}
	}
	return r
}

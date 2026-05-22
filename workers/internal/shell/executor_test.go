package shell

import (
	"runtime"
	"strings"
	"testing"
)

func TestEchoCommand(t *testing.T) {
	var r Result
	if runtime.GOOS == "windows" {
		r = Execute("echo hello", "", 5000)
	} else {
		r = Execute("echo hello", "", 5000)
	}
	if r.ExitCode != 0 {
		t.Fatalf("expected exit 0, got %d: %s", r.ExitCode, r.Error)
	}
	if !strings.Contains(r.Output, "hello") {
		t.Fatalf("expected output to contain 'hello', got: %s", r.Output)
	}
}

func TestTimeout(t *testing.T) {
	var r Result
	if runtime.GOOS == "windows" {
		r = Execute("ping -n 10 127.0.0.1", "", 500)
	} else {
		r = Execute("sleep 10", "", 500)
	}
	if r.ExitCode == 0 && r.Error == "" {
		t.Fatal("expected timeout error")
	}
}

func TestFailure(t *testing.T) {
	var r Result
	if runtime.GOOS == "windows" {
		r = Execute("cmd /C exit 1", "", 5000)
	} else {
		r = Execute("exit 1", "", 5000)
	}
	if r.ExitCode == 0 {
		t.Fatal("expected non-zero exit code")
	}
}

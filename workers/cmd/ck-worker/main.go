package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/cognition-kernel/workers/internal/filesystem"
	"github.com/cognition-kernel/workers/internal/ipc"
	"github.com/cognition-kernel/workers/internal/shell"
	"github.com/cognition-kernel/workers/pkg/protocol"
)

func main() {
	pipe := flag.String("pipe", "", "IPC pipe address to connect to")
	flag.Parse()

	if *pipe == "" {
		log.Fatal("--pipe flag is required")
	}

	client, err := ipc.Connect(*pipe)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Worker connected, waiting for requests...")

	for {
		req, err := client.ReadRequest()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed")
				return
			}
			log.Fatalf("Read error: %v", err)
		}

		resp := dispatch(req)
		if err := client.WriteResponse(resp); err != nil {
			log.Fatalf("Write error: %v", err)
		}
	}
}

func dispatch(req *protocol.ExecutionRequest) *protocol.ExecutionResponse {
	start := time.Now()
	resp := &protocol.ExecutionResponse{
		TaskID:   req.TaskID,
		ActionID: req.ActionID,
	}

	switch req.Tool {
	case "shell":
		cmd := ""
		if v, ok := req.Params["command"]; ok {
			cmd, _ = v.(string)
		}
		workDir := ""
		if v, ok := req.Params["work_dir"]; ok {
			workDir, _ = v.(string)
		}
		timeout := req.TimeoutMs
		if timeout == 0 {
			timeout = 30000
		}
		r := shell.Execute(cmd, workDir, timeout)
		resp.Output = r.Output
		resp.Success = r.ExitCode == 0 && r.Error == ""
		if r.Error != "" {
			resp.Error = &r.Error
		}

	case "filesystem":
		action := ""
		if v, ok := req.Params["action"]; ok {
			action, _ = v.(string)
		}
		r := filesystem.Execute(action, req.Params)
		resp.Output = r.Output
		resp.Success = r.Error == ""
		if r.Error != "" {
			resp.Error = &r.Error
		}
		if action == "write_file" || action == "create_dir" || action == "delete" {
			path := ""
			if v, ok := req.Params["path"]; ok {
				path, _ = v.(string)
			}
			resp.SideEffects = []string{fmt.Sprintf("%s:%s", action, path)}
		}

	default:
		errMsg := fmt.Sprintf("unknown tool: %s", req.Tool)
		resp.Error = &errMsg
	}

	resp.DurationMs = uint64(time.Since(start).Milliseconds())
	return resp
}

//go:build !windows

package ipc

import "net"

func dial(pipePath string) (net.Conn, error) {
	return net.Dial("unix", pipePath)
}

//go:build windows

package ipc

import (
	"net"

	"github.com/Microsoft/go-winio"
)

func dial(pipePath string) (net.Conn, error) {
	return winio.DialPipe(pipePath, nil)
}

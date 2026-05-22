package ipc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/cognition-kernel/workers/pkg/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type Client struct {
	conn net.Conn
}

func Connect(pipePath string) (*Client, error) {
	conn, err := dial(pipePath)
	if err != nil {
		return nil, fmt.Errorf("connect to pipe %s: %w", pipePath, err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) ReadRequest() (*protocol.ExecutionRequest, error) {
	data, err := readFrame(c.conn)
	if err != nil {
		return nil, err
	}
	var req protocol.ExecutionRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("unmarshal request: %w", err)
	}
	return &req, nil
}

func (c *Client) WriteResponse(resp *protocol.ExecutionResponse) error {
	data, err := msgpack.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}
	return writeFrame(c.conn, data)
}

func readFrame(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeFrame(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

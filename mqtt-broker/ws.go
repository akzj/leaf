package mqtt_broker

import (
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"io"
	"net"
	"time"
)

type WSConn struct {
	conn   *websocket.Conn
	reader io.Reader
}

func newWSConn(conn *websocket.Conn) *WSConn {
	return &WSConn{
		conn:   conn,
		reader: nil,
	}
}

func (ws *WSConn) Read(b []byte) (n int, err error) {
	for {
		if ws.reader == nil {
			var messageType int
			messageType, ws.reader, err = ws.conn.NextReader()
			if err != nil {
				return 0, errors.WithStack(err)
			}
			if messageType != websocket.BinaryMessage {
				return 0, errors.New("websocket message type error")
			}
		}
		n, err := ws.reader.Read(b)
		if err == io.EOF {
			ws.reader = nil
			continue
		}
		return n, err
	}
}

func (ws *WSConn) Write(b []byte) (n int, err error) {
	if err := ws.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (ws *WSConn) Close() error {
	return ws.conn.Close()
}

func (ws *WSConn) LocalAddr() net.Addr {
	return ws.conn.LocalAddr()
}

func (ws *WSConn) RemoteAddr() net.Addr {
	return ws.conn.RemoteAddr()
}

func (ws *WSConn) SetDeadline(t time.Time) error {
	if err := ws.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return ws.conn.SetWriteDeadline(t)
}

func (ws *WSConn) SetReadDeadline(t time.Time) error {
	return ws.conn.SetReadDeadline(t)
}

func (ws *WSConn) SetWriteDeadline(t time.Time) error {
	return ws.conn.SetWriteDeadline(t)
}

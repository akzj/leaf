package utils

import (
	"crypto/tls"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
)

func NewListener(host string, port int, tlsConfig *tls.Config) (net.Listener, error) {
	if tlsConfig == nil {
		listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
		if err != nil {
			log.WithField("broker.BindPort", port).Error(err)
			return nil, err
		}
		return listener, nil
	}
	listener, err := tls.Listen("tcp",
		net.JoinHostPort(host, strconv.Itoa(port)), tlsConfig)
	if err != nil {
		log.WithField("broker.BindTLSPort", port).Error(err)
		return nil, err
	}
	return listener, nil
}

// Copyright 2020-2026 The streamIO Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package listenutils

import (
	"crypto/tls"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
)

func Listen(host string, port int, tlsConfig *tls.Config) (net.Listener, error) {
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

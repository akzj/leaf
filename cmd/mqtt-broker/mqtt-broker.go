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
package main

import (
	broker "github.com/akzj/streamIO/mqtt-broker"
	log "github.com/sirupsen/logrus"
)

func main() {

	if err := broker.New(broker.DefaultOptions()).Start(); err != nil {
		log.Fatal(err)
	}
}

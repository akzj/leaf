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

package sstore

import (
	"github.com/akzj/streamIO/pkg/block-queue"
	log "github.com/sirupsen/logrus"
)

type updateSectionTable struct {
	mStreams  []*stream
	notifies  []interface{}
	callbacks []interface{}
}

type sectionsTableUpdater struct {
	callbackQueue *block_queue.QueueWithContext
	notifyQueue   *block_queue.QueueWithContext
	queue         *block_queue.QueueWithContext
	sectionsTable *sectionsTable
}

func (updater *sectionsTableUpdater) updateLoop() {
	for {
		items, err := updater.queue.PopAll(nil)
		if err != nil {
			log.Warn(err)
			return
		}
		for _, item := range items {
			update := item.(updateSectionTable)
			for _, stream := range update.mStreams {
				updater.sectionsTable.UpdateSectionWithStream(stream)
			}
			if update.callbacks != nil {
				if err := updater.callbackQueue.PushMany(update.callbacks); err != nil {
					log.Fatal(err)
				}
			}
			if update.notifies != nil {
				if err := updater.notifyQueue.PushMany(update.notifies); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

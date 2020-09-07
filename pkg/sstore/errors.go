// Copyright 2020-2026 The sstore Authors
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
	"errors"
)

var (
	ErrOffset            = errors.New("offset error")
	ErrNoFindStream      = errors.New("no find stream")
	ErrNoFindIndexInfo   = errors.New("no find index info")
	ErrNoFindOffsetIndex = errors.New("no find offset index")
	ErrNoFindSegment     = errors.New("no find segment")
	ErrWhence            = errors.New("whence error")
	ErrWal               = errors.New("journal error")
	ErrClose             = errors.New("SStore close")
)

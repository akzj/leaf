package server

import "errors"

var (
	ErrNoFindStream       = errors.New("no find stream error")
	ErrInvalidStreamEnd   = errors.New("invalid stream end error")
	ErrInvalidStreamBegin = errors.New("invalid stream begin error")
)

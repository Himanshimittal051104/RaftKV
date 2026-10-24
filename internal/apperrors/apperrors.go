package apperrors

import "errors"

var (
	ErrEngineClosed  = errors.New("engine is closed")
	ErrInvalidRecord = errors.New("invalid record")
	ErrEngineFailed  = errors.New("storage engine failed")
)

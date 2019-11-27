package types

type ErrClientRequest struct {
	message string
}

func NewErrClientRequest(message string) *ErrClientRequest {
	return &ErrClientRequest{message: message}
}

func (e *ErrClientRequest) Error() string {
	return e.message
}

package hec

import (
	"fmt"
	"strconv"
	"strings"
)

// Response is response message from HEC. For example, `{"text":"Success","code":0}`.
type Response struct {
	Text string `json:"text"`
	Code int    `json:"code"`
}

// Response status codes
const (
	StatusSuccess              = 0
	StatusTokenDisabled        = 1
	StatusTokenRequired        = 2
	StatusInvalidAuthorization = 3
	StatusInvalidToken         = 4
	StatusNoData               = 5
	StatusInvalidDataFormat    = 6
	StatusIncorrectIndex       = 7
	StatusInternalServerError  = 8
	StatusServerBusy           = 9
	StatusChannelMissing       = 10
	StatusInvalidChannel       = 11
	StatusEventFieldRequired   = 12
	StatusEventFieldBlank      = 13
	StatusAckDisabled          = 14
)

func retriable(code int) bool {
	return code == StatusServerBusy || code == StatusInternalServerError
}

type ErrEventTooLong struct {
	indexes []int
}

func (e *ErrEventTooLong) Error() string {
	if e.indexes == nil {
		return "Event length is too long"
	}
	ns := make([]string, len(e.indexes))
	for i, n := range e.indexes {
		ns[i] = strconv.Itoa(n)
	}
	numbers := strings.Join(ns, ", ")
	return fmt.Sprintf("Events (%s) length are too long", numbers)
}

// GetIndexes gives the indexes or line numbers of too-long events
func (e *ErrEventTooLong) GetIndexes() []int {
	return e.indexes
}

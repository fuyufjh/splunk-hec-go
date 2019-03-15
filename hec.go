package hec

import (
	"context"
	"io"
	"net/http"
)

type HEC interface {
	SetHTTPClient(client *http.Client)
	SetKeepAlive(enable bool)
	SetChannel(channel string)
	SetMaxRetry(retries int)
	SetMaxContentLength(size int)

	// WriteEvent writes single event via HEC json mode
	WriteEvent(event *Event) error

	// WriteBatch writes multiple events via HCE batch mode
	WriteBatch(events []*Event) error

	// WriteRaw writes raw data stream via HEC raw mode
	WriteRaw(reader io.ReadSeeker, metadata *EventMetadata) error

	// WaitForAcknowledgement blocks until the Splunk indexer acknowledges data sent to it
	WaitForAcknowledgement() error

	// WaitForAcknowledgementWithContext blocks until the Splunk indexer acknowledges data sent to it with a context for cancellation
	WaitForAcknowledgementWithContext(ctx context.Context) error
}

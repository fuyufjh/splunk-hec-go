package hec

import "net/http"

type HEC interface {
	SetHTTPClient(client *http.Client)
	SetKeepAlive(enable bool)
	SetChannel(channel string)

	WriteEvent(event *Event) error
	WriteBatch(events []*Event) error
	WriteRaw(events []byte, metadata *EventMetadata) error
}

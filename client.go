package hec

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/satori/go.uuid"
)

const retryWaitTime = 1 * time.Second

type Client struct {
	HEC

	// HTTP Client for communication with (optional)
	httpClient *http.Client

	// Splunk Server URL for API requests (required)
	serverURL string

	// HEC Token (required)
	token string

	// Keep-Alive (optional, default: true)
	keepAlive bool

	// Channel (required for Raw mode)
	channel string

	// Max retrying times (optional, default: 2)
	retries int
}

func NewClient(serverURL string, token string) HEC {
	return &Client{
		httpClient: http.DefaultClient,
		serverURL:  serverURL,
		token:      token,
		keepAlive:  true,
		channel:    uuid.NewV4().String(),
		retries:    2,
	}
}

func (hec *Client) SetHTTPClient(client *http.Client) {
	hec.httpClient = client
}

func (hec *Client) SetKeepAlive(enable bool) {
	hec.keepAlive = enable
}

func (hec *Client) SetChannel(channel string) {
	hec.channel = channel
}

func (hec *Client) SetMaxRetry(retries int) {
	hec.retries = retries
}

func (hec *Client) WriteEvent(event *Event) error {
	endpoint := "/services/collector?channel=" + hec.channel
	data, _ := json.Marshal(event)
	return hec.write(endpoint, data)
}

func (hec *Client) WriteBatch(events []*Event) error {
	endpoint := "/services/collector?channel=" + hec.channel
	var buffer bytes.Buffer
	for _, event := range events {
		data, _ := json.Marshal(event)
		buffer.Write(data)
	}
	return hec.write(endpoint, buffer.Bytes())
}

type EventMetadata struct {
	Host       *string
	Index      *string
	Source     *string
	SourceType *string
	Time       *time.Time
}

func (hec *Client) WriteRaw(events []byte, metadata *EventMetadata) error {
	endpoint := rawHecEndpoint(hec.channel, metadata)
	return hec.write(endpoint, events)
}

func responseFrom(body []byte) *Response {
	var res Response
	json.Unmarshal(body, &res)
	return &res
}

func (res *Response) Error() string {
	return res.Text
}

func (res *Response) String() string {
	b, _ := json.Marshal(res)
	return string(b)
}

func (hec *Client) write(endpoint string, data []byte) error {
	retries := 0
RETRY:
	req, err := http.NewRequest(http.MethodPost, hec.serverURL+endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if hec.keepAlive {
		req.Header.Set("Connection", "keep-alive")
	}
	req.Header.Set("Authorization", "Splunk "+hec.token)
	res, err := hec.httpClient.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		response := responseFrom(body)
		if retriable(response.Code) && retries < hec.retries {
			retries++
			time.Sleep(retryWaitTime)
			goto RETRY
		}
		return response
	}
	return nil
}

func rawHecEndpoint(channel string, metadata *EventMetadata) string {
	var buffer bytes.Buffer
	buffer.WriteString("/services/collector/raw?channel=" + channel)
	if metadata == nil {
		return buffer.String()
	}
	if metadata.Host != nil {
		buffer.WriteString("&host=" + *metadata.Host)
	}
	if metadata.Index != nil {
		buffer.WriteString("&index=" + *metadata.Index)
	}
	if metadata.Source != nil {
		buffer.WriteString("&source=" + *metadata.Source)
	}
	if metadata.SourceType != nil {
		buffer.WriteString("&sourcetype=" + *metadata.SourceType)
	}
	if metadata.Time != nil {
		buffer.WriteString("&time=" + epochTime(metadata.Time))
	}
	return buffer.String()
}

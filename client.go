package hec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/satori/go.uuid"
)

const (
	retryWaitTime = 1 * time.Second

	defaultMaxContentLength = 1000000
)

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

	// Max content length (optional, default: 1000000)
	maxLength int
}

func NewClient(serverURL string, token string) HEC {
	return &Client{
		httpClient: http.DefaultClient,
		serverURL:  serverURL,
		token:      token,
		keepAlive:  true,
		channel:    uuid.NewV4().String(),
		retries:    2,
		maxLength:  defaultMaxContentLength,
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

func (hec *Client) SetMaxContentLength(size int) {
	hec.maxLength = size
}

func (hec *Client) WriteEvent(event *Event) error {
	if event.empty() {
		return nil // skip empty events
	}

	endpoint := "/services/collector?channel=" + hec.channel
	data, _ := json.Marshal(event)

	if len(data) > hec.maxLength {
		return &ErrEventTooLong{}
	}
	return hec.write(endpoint, data)
}

func (hec *Client) WriteBatch(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	endpoint := "/services/collector?channel=" + hec.channel
	var buffer bytes.Buffer
	var tooLongs []int

	for index, event := range events {
		if event.empty() {
			continue // skip empty events
		}

		data, _ := json.Marshal(event)
		if len(data) > hec.maxLength {
			tooLongs = append(tooLongs, index)
			continue
		}
		// Send out bytes in buffer immediately if the limit exceeded after adding this event
		if buffer.Len()+len(data) > hec.maxLength {
			if err := hec.write(endpoint, buffer.Bytes()); err != nil {
				return err
			}
			buffer.Reset()
		}
		buffer.Write(data)
	}

	if buffer.Len() > 0 {
		if err := hec.write(endpoint, buffer.Bytes()); err != nil {
			return err
		}
	}
	if len(tooLongs) > 0 {
		return &ErrEventTooLong{tooLongs}
	}
	return nil
}

type EventMetadata struct {
	Host       *string
	Index      *string
	Source     *string
	SourceType *string
	Time       *time.Time
}

func (hec *Client) WriteRaw(reader io.ReadSeeker, metadata *EventMetadata) error {
	endpoint := rawHecEndpoint(hec.channel, metadata)

	scanner := bufio.NewScanner(reader)
	var buf bytes.Buffer
	var tooLongs []int

	for lineNo := 1; scanner.Scan(); lineNo++ {
		if len(scanner.Bytes()) > hec.maxLength {
			tooLongs = append(tooLongs, lineNo)
			continue
		}
		// Send out bytes in buffer immediately if the limit exceeded after adding this line
		if buf.Len()+len(scanner.Bytes())+1 > hec.maxLength {
			if err := hec.write(endpoint, buf.Bytes()); err != nil {
				return err
			}
			buf.Reset()
		}
		buf.Write(scanner.Bytes())
		buf.WriteByte('\n')
	}

	if buf.Len() > 0 {
		if err := hec.write(endpoint, buf.Bytes()); err != nil {
			return err
		}
	}
	if len(tooLongs) > 0 {
		return &ErrEventTooLong{tooLongs}
	}
	return nil
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

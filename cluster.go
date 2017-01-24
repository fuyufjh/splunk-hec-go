package hec

import (
	"math/rand"
	"net/http"
	"sync"

	"github.com/satori/go.uuid"
)

type Cluster struct {
	HEC

	// Inner clients
	clients []*Client

	mtx sync.Mutex
}

func NewCluster(serverURLs []string, token string) HEC {
	channel := uuid.NewV4().String()
	clients := make([]*Client, len(serverURLs))
	for i, serverURL := range serverURLs {
		clients[i] = &Client{
			httpClient: http.DefaultClient,
			serverURL:  serverURL,
			token:      token,
			keepAlive:  true,
			channel:    channel,
		}
	}
	return &Cluster{
		clients: clients,
	}
}

func (c *Cluster) SetHTTPClient(httpClient *http.Client) {
	c.mtx.Lock()
	for _, client := range c.clients {
		client.SetHTTPClient(httpClient)
	}
	c.mtx.Unlock()
}

func (c *Cluster) SetKeepAlive(enable bool) {
	c.mtx.Lock()
	for _, client := range c.clients {
		client.SetKeepAlive(enable)
	}
	c.mtx.Unlock()
}

func (c *Cluster) SetChannel(channel string) {
	c.mtx.Lock()
	for _, client := range c.clients {
		client.SetChannel(channel)
	}
	c.mtx.Unlock()
}

func (c *Cluster) WriteEvent(event *Event) error {
	return pick(c.clients).WriteEvent(event)
}

func (c *Cluster) WriteBatch(events []*Event) error {
	return pick(c.clients).WriteBatch(events)
}

func (c *Cluster) WriteRaw(events []byte, metadata *EventMetadata) error {
	return pick(c.clients).WriteRaw(events, metadata)
}

func pick(clients []*Client) *Client {
	return clients[rand.Int()%len(clients)]
}

package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var moscow = time.FixedZone("MSK", 3*60*60)

func Now() time.Time {
	return time.Now().In(moscow)
}

type Event struct {
	ID      string `json:"event_id"`
	User    string `json:"user_id"`
	Seq     int64  `json:"seq"`
	Payload struct {
		Poison bool `json:"poison"`
	} `json:"payload"`
}

type Entry struct {
	ID        string `json:"event_id"`
	User      string `json:"user_id"`
	Seq       int64  `json:"seq"`
	Worker    string `json:"worker"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (e Entry) Valid() bool {
	return e.ID != "" && e.User != "" && e.Seq > 0 &&
		e.Worker != "" && e.Topic != "" && e.Partition >= 0 && e.Offset >= 0
}

type Snapshot struct {
	Entries    []Entry `json:"entries"`
	Duplicates int64   `json:"duplicates"`
}

const (
	Topic           = "events"
	DeadLetterTopic = "events.dlt"
)

func NewKafkaClient(opts ...kgo.Opt) (*kgo.Client, error) {
	seeds := os.Getenv("BOOTSTRAP_SERVERS")
	if seeds == "" {
		return nil, fmt.Errorf("BOOTSTRAP_SERVERS is required")
	}
	return kgo.NewClient(append(opts, kgo.SeedBrokers(strings.Split(seeds, ",")...))...)
}

const sinkRequestTimeout = 30 * time.Second

var httpClient = &http.Client{Timeout: sinkRequestTimeout, Transport: otelhttp.NewTransport(http.DefaultTransport)}

func SaveEntry(ctx context.Context, entry Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	resp, err := sinkRequest(ctx, http.MethodPost, bytes.NewReader(data))
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func ReadSnapshot(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot
	resp, err := sinkRequest(ctx, http.MethodGet, nil)
	if err != nil {
		return snapshot, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&snapshot)
	return snapshot, err
}

func sinkRequest(ctx context.Context, method string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, os.Getenv("SINK_URL")+"/events", body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		resp.Body.Close()
		return nil, fmt.Errorf("sink %s: %s", req.Method, resp.Status)
	}
	return resp, nil
}

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	random "math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	usersPerProducer = 10
	poisonPercent    = 2
	deliveryTimeout  = 20 * time.Second
)

type event struct {
	events.Event
	Type       string    `json:"type"`
	ProducedAt time.Time `json:"produced_at"`
}

func uuid() string {
	var b [16]byte
	rand.Read(b[:])
	const version4Bits = 0x40
	const rfcVariantBits = 0x80
	b[6] = b[6]&0x0f | version4Bits
	b[8] = b[8]&0x3f | rfcVariantBits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type generator struct {
	runID     string
	sequences [usersPerProducer]int64
}

func (g *generator) next() event {
	types := [...]string{"page_view", "add_to_cart", "purchase"}
	i := random.IntN(len(g.sequences))
	g.sequences[i]++
	e := event{
		Event:      events.Event{ID: uuid(), User: fmt.Sprintf("%s-%d", g.runID, i), Seq: g.sequences[i]},
		Type:       types[random.IntN(len(types))],
		ProducedAt: events.Now(),
	}
	e.Payload.Poison = random.IntN(100) < poisonPercent
	return e
}

func run(ctx context.Context) error {
	rate, err := strconv.Atoi(os.Getenv("EVENTS_PER_SECOND"))
	if err != nil || rate < 1 || rate > int(time.Second) {
		return fmt.Errorf("invalid EVENTS_PER_SECOND: %q", os.Getenv("EVENTS_PER_SECOND"))
	}
	mode := os.Getenv("KEY_MODE")
	if mode != "user_id" && mode != "none" {
		return fmt.Errorf("invalid KEY_MODE: %q", mode)
	}
	client, err := events.NewKafkaClient(kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(deliveryTimeout))
	if err != nil {
		return err
	}
	defer client.Close()

	gen := generator{runID: uuid()}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return nil
		}
		e := gen.next()
		value, err := json.Marshal(e)
		if err != nil {
			return err
		}
		record := &kgo.Record{Topic: events.Topic, Value: value}
		if mode == "user_id" {
			record.Key = []byte(e.User)
		}
		if err := client.ProduceSync(context.Background(), record).FirstErr(); err != nil {
			return fmt.Errorf("send event %s user %s seq %d: %w", e.ID, e.User, e.Seq, err)
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"

	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

func decode(value []byte) (events.Event, error) {
	var e events.Event
	err := json.Unmarshal(value, &e)
	if err == nil && (e.ID == "" || e.User == "" || e.Seq < 1) {
		err = errors.New("missing event_id, user_id or positive seq")
	}
	if err == nil && e.Payload.Poison {
		err = errors.New("payload.poison is true")
	}
	return e, err
}

const maxProcessingAttempts = 3

func process(ctx context.Context, cl *kgo.Client, worker string, delay time.Duration, r *kgo.Record) error {
	var reason error
	for attempt := 1; attempt <= maxProcessingAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		e, err := decode(r.Value)
		if err == nil {
			entry := events.Entry{ID: e.ID, User: e.User, Seq: e.Seq, Worker: worker, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}
			return events.SaveEntry(ctx, entry)
		}
		reason = err
		log.Printf("%s[%d] offset=%d attempt=%d: %v", r.Topic, r.Partition, r.Offset, attempt, err)
	}
	dlt := &kgo.Record{Topic: events.DeadLetterTopic, Key: r.Key, Value: r.Value, Headers: []kgo.RecordHeader{
		{Key: "reason", Value: []byte(reason.Error())},
		{Key: "source-topic", Value: []byte(r.Topic)},
		{Key: "source-partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		{Key: "source-offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
	}}
	return cl.ProduceSync(ctx, dlt).FirstErr()
}

func run(ctx context.Context) error {
	const maxDelayMillis = math.MaxInt64 / int64(time.Millisecond)
	ms, err := strconv.ParseInt(os.Getenv("PROCESSING_DELAY_MS"), 10, 64)
	if err != nil || ms < 0 || ms > maxDelayMillis {
		return errors.New("invalid PROCESSING_DELAY_MS")
	}
	delay := time.Duration(ms) * time.Millisecond
	worker, err := os.Hostname()
	if err != nil {
		return err
	}
	first := make(map[int32]bool)
	cl, err := events.NewKafkaClient(kgo.ConsumerGroup("events-processor"), kgo.ConsumeTopics(events.Topic),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, p map[string][]int32) {
			for _, id := range p[events.Topic] {
				delete(first, id)
			}
			log.Printf("assigned %v", p)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { log.Printf("revoked %v", p) }))
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	for ctx.Err() == nil {
		f := cl.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			break
		}
		if errs := f.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		for _, r := range f.Records() {
			if !first[r.Partition] {
				log.Printf("processing %s[%d] offset=%d", r.Topic, r.Partition, r.Offset)
				first[r.Partition] = true
			}
			if err := process(ctx, cl, worker, delay, r); err != nil {
				return fmt.Errorf("process %d:%d: %w", r.Partition, r.Offset, err)
			}
			if err := cl.CommitRecords(ctx, r); err != nil {
				return fmt.Errorf("commit %d:%d: %w", r.Partition, r.Offset, err)
			}
		}
		cl.AllowRebalance()
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

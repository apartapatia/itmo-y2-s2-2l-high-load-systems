package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/events"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type userStats struct {
	sequences  map[int64]bool
	maxSeen    int64
	maxWritten int64
	late       int
}

type summary map[string]*userStats

func (s summary) add(user string, seq int64, fromSink bool) {
	u := s[user]
	if u == nil {
		u = &userStats{sequences: make(map[int64]bool)}
		s[user] = u
	}
	u.sequences[seq] = true
	u.maxSeen = max(u.maxSeen, seq)
	if fromSink {
		if seq < u.maxWritten {
			u.late++
		}
		u.maxWritten = max(u.maxWritten, seq)
	}
}

func (u *userStats) gaps() []string {
	if int64(len(u.sequences)) == u.maxSeen {
		return nil
	}
	seqs := slices.Sorted(maps.Keys(u.sequences))
	var gaps []string
	var prev int64
	for _, n := range seqs {
		if n-prev > 1 {
			gaps = append(gaps, fmt.Sprintf("%d..%d", prev+1, n-1))
		}
		prev = n
	}
	return gaps
}

func readDLT(ctx context.Context, s summary) error {
	cl, err := events.NewKafkaClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	const (
		topic             = events.DeadLetterTopic
		partitionCount    = 3
		earliestTimestamp = -2
		latestTimestamp   = -1
	)
	partitions := make(map[int32]kgo.Offset)
	until := make(map[int32]int64)
	for _, timestamp := range []int64{earliestTimestamp, latestTimestamp} {
		req := kmsg.NewPtrListOffsetsRequest()
		t := kmsg.NewListOffsetsRequestTopic()
		t.Topic = topic
		for id := int32(0); id < partitionCount; id++ {
			p := kmsg.NewListOffsetsRequestTopicPartition()
			p.Partition = id
			p.Timestamp = timestamp
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = []kmsg.ListOffsetsRequestTopic{t}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return err
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return err
				}
				if timestamp == earliestTimestamp && p.Offset != 0 {
					return fmt.Errorf("DLT partition %d starts at %d: incomplete history", p.Partition, p.Offset)
				}
				if timestamp == latestTimestamp && p.Offset > 0 {
					partitions[p.Partition] = kgo.NewOffset().At(0)
					until[p.Partition] = p.Offset
				}
			}
		}
	}
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions})
	for len(until) > 0 {
		f := cl.PollFetches(ctx)
		if errs := f.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		for it := f.RecordIter(); !it.Done(); {
			r := it.Next()
			end, ok := until[r.Partition]
			if !ok || r.Offset >= end {
				continue
			}
			var e events.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				return fmt.Errorf("DLT %d:%d: %w", r.Partition, r.Offset, err)
			}
			if e.User == "" || e.Seq < 1 {
				return fmt.Errorf("DLT %d:%d lacks user_id or seq", r.Partition, r.Offset)
			}
			s.add(e.User, e.Seq, false)
			if r.Offset+1 == end {
				delete(until, r.Partition)
			}
		}
	}
	return nil
}

func run() error {
	login := os.Getenv("GITHUB_LOGIN")
	if login == "" {
		return fmt.Errorf("set GITHUB_LOGIN")
	}
	fmt.Printf("%s %s\n", login, events.Now().Format(time.RFC3339))
	const verificationTimeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), verificationTimeout)
	defer cancel()
	snapshot, err := events.ReadSnapshot(ctx)
	if err != nil {
		return err
	}
	s := summary{}
	for _, e := range snapshot.Entries {
		s.add(e.User, e.Seq, true)
	}
	if err := readDLT(ctx, s); err != nil {
		return err
	}
	users := slices.Sorted(maps.Keys(s))
	var late int
	missing := false
	for _, user := range users {
		u := s[user]
		gaps := u.gaps()
		fmt.Printf("user=%s gaps=%v out_of_order=%d\n", user, gaps, u.late)
		missing = missing || len(gaps) > 0
		late += u.late
	}
	fmt.Printf("users=%d duplicates=%d out_of_order=%d\n", len(users), snapshot.Duplicates, late)
	if missing {
		return fmt.Errorf("missing sequence numbers")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

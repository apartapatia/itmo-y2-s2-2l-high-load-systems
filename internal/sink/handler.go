package sink

import (
	"encoding/json"
	"log"
	"net/http"
	"slices"
	"sync"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/events"
)

const maxEventBodyBytes = 1 << 20

type store struct {
	mu   sync.Mutex
	seen map[string]struct{}
	data events.Snapshot
}

func (s *store) save(w http.ResponseWriter, r *http.Request) {
	var e events.Entry
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEventBodyBytes)).Decode(&e)
	if err != nil || !e.Valid() {
		http.Error(w, "invalid entry", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if _, ok := s.seen[e.ID]; ok {
		s.data.Duplicates++
	} else {
		s.seen[e.ID] = struct{}{}
		s.data.Entries = append(s.data.Entries, e)
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *store) snapshot(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data := events.Snapshot{Entries: slices.Clone(s.data.Entries), Duplicates: s.data.Duplicates}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("snapshot: %v", err)
	}
}

func NewHandler() http.Handler {
	s := store{seen: make(map[string]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.save)
	mux.HandleFunc("GET /events", s.snapshot)
	return mux
}

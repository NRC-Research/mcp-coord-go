// Command mcp-coord-go is a minimal coordination MCP server ("blackboard") for
// a small number of INDEPENDENT top-level AI agent sessions owned by one
// operator — e.g. two Claude Code sessions on different projects that need to
// message each other and avoid stepping on shared resources.
//
// It is a single static Go binary with one runtime dependency (the official
// MCP Go SDK). It serves MCP over streamable HTTP so every agent session dials
// the SAME persistent server (stdio would give each session a private
// instance). State is kept in memory, persisted atomically to one JSON file,
// and mirrored to per-thread Markdown files for human-auditable history.
//
// Tools (6): coord_send, coord_poll, coord_thread, coord_agents,
// coord_reserve, coord_release.
//
// Trust model: peers are co-owned and the server binds to localhost by
// default. There is deliberately no auth/ACL machinery — do not expose the
// listen address beyond a network you fully control.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Message struct {
	ID          int64     `json:"id"`
	Thread      string    `json:"thread"`
	From        string    `json:"from"`
	To          []string  `json:"to"` // agent names, or ["*"] for broadcast
	Subject     string    `json:"subject"`
	Body        string    `json:"body"`
	SentAt      time.Time `json:"sent_at"`
	DeliveredTo []string  `json:"delivered_to"` // agents that have polled it
}

type Agent struct {
	Name      string    `json:"name"`
	Project   string    `json:"project,omitempty"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

type Reservation struct {
	Path     string    `json:"path"`
	Agent    string    `json:"agent"`
	Reason   string    `json:"reason,omitempty"`
	Since    time.Time `json:"since"`
	Expires  time.Time `json:"expires"`
}

type State struct {
	NextID       int64                   `json:"next_id"`
	Messages     []*Message              `json:"messages"`
	Agents       map[string]*Agent       `json:"agents"`
	Reservations map[string]*Reservation `json:"reservations"` // key: path
}

type Store struct {
	mu        sync.Mutex
	state     State
	stateFile string
	threadDir string
}

func newStore(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "threads"), 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		stateFile: filepath.Join(dir, "state.json"),
		threadDir: filepath.Join(dir, "threads"),
		state: State{
			NextID:       1,
			Agents:       map[string]*Agent{},
			Reservations: map[string]*Reservation{},
		},
	}
	raw, err := os.ReadFile(s.stateFile)
	if err == nil {
		if err := json.Unmarshal(raw, &s.state); err != nil {
			// Never clobber a corrupt state file silently — move it aside.
			bak := s.stateFile + ".corrupt." + time.Now().Format("20060102T150405")
			_ = os.Rename(s.stateFile, bak)
			log.Printf("state file unreadable (%v); moved to %s, starting fresh", err, bak)
		}
	}
	if s.state.Agents == nil {
		s.state.Agents = map[string]*Agent{}
	}
	if s.state.Reservations == nil {
		s.state.Reservations = map[string]*Reservation{}
	}
	if s.state.NextID < 1 {
		s.state.NextID = 1
	}
	return s, nil
}

// save persists state atomically (write temp + rename). Callers hold s.mu.
func (s *Store) save() {
	raw, err := json.MarshalIndent(&s.state, "", " ")
	if err != nil {
		log.Printf("marshal state: %v", err)
		return
	}
	tmp := s.stateFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		log.Printf("write state: %v", err)
		return
	}
	if err := os.Rename(tmp, s.stateFile); err != nil {
		log.Printf("rename state: %v", err)
	}
}

// touch registers/heartbeats an agent. Callers hold s.mu.
func (s *Store) touch(agent, project string) {
	if agent == "" {
		return
	}
	now := time.Now()
	a, ok := s.state.Agents[agent]
	if !ok {
		a = &Agent{Name: agent, FirstSeen: now}
		s.state.Agents[agent] = a
	}
	if project != "" {
		a.Project = project
	}
	a.LastSeen = now
}

// pruneExpired drops expired reservations. Callers hold s.mu.
func (s *Store) pruneExpired() {
	now := time.Now()
	for p, r := range s.state.Reservations {
		if now.After(r.Expires) {
			delete(s.state.Reservations, p)
		}
	}
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(subject string) string {
	slug := slugRe.ReplaceAllString(strings.ToLower(subject), "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "thread"
	}
	if len(slug) > 48 {
		slug = slug[:48]
	}
	return slug
}

// appendThreadMarkdown mirrors a message into the human-readable archive.
func (s *Store) appendThreadMarkdown(m *Message) {
	path := filepath.Join(s.threadDir, m.Thread+".md")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("thread archive: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "\n---\n\n**#%d** · from **%s** · to %s · %s\n**%s**\n\n%s\n",
		m.ID, m.From, strings.Join(m.To, ", "), m.SentAt.Format(time.RFC3339), m.Subject, m.Body)
}

// ---------------------------------------------------------------------------
// Tool I/O
// ---------------------------------------------------------------------------

type sendIn struct {
	Agent   string   `json:"agent" jsonschema:"your agent name (stable identity, e.g. image-builder)"`
	Project string   `json:"project,omitempty" jsonschema:"optional project/repo this agent works on"`
	To      []string `json:"to" jsonschema:"recipient agent names, or [\"*\"] to broadcast to all"`
	Subject string   `json:"subject" jsonschema:"short subject line"`
	Body    string   `json:"body" jsonschema:"message body (markdown welcome)"`
	Thread  string   `json:"thread,omitempty" jsonschema:"thread id to reply into; omit to start a new thread"`
}
type sendOut struct {
	ID     int64  `json:"id"`
	Thread string `json:"thread"`
}

type pollIn struct {
	Agent       string `json:"agent" jsonschema:"your agent name"`
	Project     string `json:"project,omitempty"`
	IncludeSeen bool   `json:"include_seen,omitempty" jsonschema:"also return messages already delivered to you"`
	Limit       int    `json:"limit,omitempty" jsonschema:"max messages to return (default 25)"`
}
type pollOut struct {
	Messages []*Message `json:"messages"`
	Note     string     `json:"note,omitempty"`
}

type threadIn struct {
	Agent  string `json:"agent" jsonschema:"your agent name"`
	Thread string `json:"thread" jsonschema:"thread id (from coord_send/coord_poll results)"`
}
type threadOut struct {
	Thread   string     `json:"thread"`
	Messages []*Message `json:"messages"`
}

type agentsIn struct {
	Agent   string `json:"agent" jsonschema:"your agent name"`
	Project string `json:"project,omitempty"`
}
type agentsOut struct {
	Agents       []*Agent       `json:"agents"`
	Reservations []*Reservation `json:"reservations"`
	Threads      []string       `json:"threads"`
}

type reserveIn struct {
	Agent      string   `json:"agent" jsonschema:"your agent name"`
	Paths      []string `json:"paths" jsonschema:"resource identifiers to reserve (file paths, repo names, hostnames — any agreed string)"`
	Reason     string   `json:"reason,omitempty" jsonschema:"why (shown to whoever hits the conflict)"`
	TTLMinutes int      `json:"ttl_minutes,omitempty" jsonschema:"reservation lifetime in minutes (default 60, max 1440)"`
}
type reserveOut struct {
	Reserved  []string `json:"reserved"`
	Conflicts []string `json:"conflicts,omitempty"`
}

type releaseIn struct {
	Agent string   `json:"agent" jsonschema:"your agent name"`
	Paths []string `json:"paths" jsonschema:"resource identifiers to release (must be held by you)"`
}
type releaseOut struct {
	Released []string `json:"released"`
	Skipped  []string `json:"skipped,omitempty"`
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type coord struct{ store *Store }

func (c *coord) send(_ context.Context, _ *mcp.CallToolRequest, in sendIn) (*mcp.CallToolResult, sendOut, error) {
	if strings.TrimSpace(in.Agent) == "" {
		return nil, sendOut{}, fmt.Errorf("agent is required")
	}
	if len(in.To) == 0 {
		return nil, sendOut{}, fmt.Errorf("to is required (agent names or [\"*\"])")
	}
	if strings.TrimSpace(in.Subject) == "" && strings.TrimSpace(in.Body) == "" {
		return nil, sendOut{}, fmt.Errorf("subject or body is required")
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, in.Project)
	thread := strings.TrimSpace(in.Thread)
	if thread == "" {
		thread = fmt.Sprintf("%s-%s", time.Now().Format("20060102"), slugify(in.Subject))
	}
	m := &Message{
		ID:      s.state.NextID,
		Thread:  thread,
		From:    in.Agent,
		To:      in.To,
		Subject: in.Subject,
		Body:    in.Body,
		SentAt:  time.Now(),
	}
	s.state.NextID++
	s.state.Messages = append(s.state.Messages, m)
	s.appendThreadMarkdown(m)
	s.save()
	return nil, sendOut{ID: m.ID, Thread: m.Thread}, nil
}

func addressedTo(m *Message, agent string) bool {
	for _, t := range m.To {
		if t == "*" || t == agent {
			return true
		}
	}
	return false
}

func delivered(m *Message, agent string) bool {
	for _, d := range m.DeliveredTo {
		if d == agent {
			return true
		}
	}
	return false
}

func (c *coord) poll(_ context.Context, _ *mcp.CallToolRequest, in pollIn) (*mcp.CallToolResult, pollOut, error) {
	if strings.TrimSpace(in.Agent) == "" {
		return nil, pollOut{}, fmt.Errorf("agent is required")
	}
	limit := in.Limit
	if limit <= 0 || limit > 200 {
		limit = 25
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, in.Project)
	var out []*Message
	for _, m := range s.state.Messages {
		if m.From == in.Agent || !addressedTo(m, in.Agent) {
			continue
		}
		if !in.IncludeSeen && delivered(m, in.Agent) {
			continue
		}
		if !delivered(m, in.Agent) {
			m.DeliveredTo = append(m.DeliveredTo, in.Agent)
		}
		out = append(out, m)
		if len(out) >= limit {
			break
		}
	}
	s.save()
	note := ""
	if len(out) == 0 {
		note = "no new messages"
	}
	return nil, pollOut{Messages: out, Note: note}, nil
}

func (c *coord) thread(_ context.Context, _ *mcp.CallToolRequest, in threadIn) (*mcp.CallToolResult, threadOut, error) {
	if strings.TrimSpace(in.Thread) == "" {
		return nil, threadOut{}, fmt.Errorf("thread is required")
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, "")
	out := threadOut{Thread: in.Thread}
	for _, m := range s.state.Messages {
		if m.Thread == in.Thread {
			out.Messages = append(out.Messages, m)
		}
	}
	if len(out.Messages) == 0 {
		return nil, out, fmt.Errorf("no messages in thread %q", in.Thread)
	}
	return nil, out, nil
}

func (c *coord) agents(_ context.Context, _ *mcp.CallToolRequest, in agentsIn) (*mcp.CallToolResult, agentsOut, error) {
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, in.Project)
	s.pruneExpired()
	out := agentsOut{}
	for _, a := range s.state.Agents {
		out.Agents = append(out.Agents, a)
	}
	sort.Slice(out.Agents, func(i, j int) bool { return out.Agents[i].Name < out.Agents[j].Name })
	for _, r := range s.state.Reservations {
		out.Reservations = append(out.Reservations, r)
	}
	sort.Slice(out.Reservations, func(i, j int) bool { return out.Reservations[i].Path < out.Reservations[j].Path })
	seen := map[string]bool{}
	for _, m := range s.state.Messages {
		if !seen[m.Thread] {
			seen[m.Thread] = true
			out.Threads = append(out.Threads, m.Thread)
		}
	}
	s.save()
	return nil, out, nil
}

func (c *coord) reserve(_ context.Context, _ *mcp.CallToolRequest, in reserveIn) (*mcp.CallToolResult, reserveOut, error) {
	if strings.TrimSpace(in.Agent) == "" || len(in.Paths) == 0 {
		return nil, reserveOut{}, fmt.Errorf("agent and paths are required")
	}
	ttl := in.TTLMinutes
	if ttl <= 0 {
		ttl = 60
	}
	if ttl > 1440 {
		ttl = 1440
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, "")
	s.pruneExpired()
	out := reserveOut{}
	now := time.Now()
	// First pass: detect conflicts so a multi-path reserve is all-or-nothing.
	var conflictErrs []string
	for _, p := range in.Paths {
		if r, held := s.state.Reservations[p]; held && r.Agent != in.Agent {
			out.Conflicts = append(out.Conflicts, p)
			conflictErrs = append(conflictErrs,
				fmt.Sprintf("%s held by %s until %s (%s)", p, r.Agent, r.Expires.Format(time.RFC3339), r.Reason))
		}
	}
	if len(out.Conflicts) > 0 {
		return nil, out, fmt.Errorf("reservation conflict: %s", strings.Join(conflictErrs, "; "))
	}
	for _, p := range in.Paths {
		s.state.Reservations[p] = &Reservation{
			Path: p, Agent: in.Agent, Reason: in.Reason,
			Since: now, Expires: now.Add(time.Duration(ttl) * time.Minute),
		}
		out.Reserved = append(out.Reserved, p)
	}
	s.save()
	return nil, out, nil
}

func (c *coord) release(_ context.Context, _ *mcp.CallToolRequest, in releaseIn) (*mcp.CallToolResult, releaseOut, error) {
	if strings.TrimSpace(in.Agent) == "" || len(in.Paths) == 0 {
		return nil, releaseOut{}, fmt.Errorf("agent and paths are required")
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(in.Agent, "")
	out := releaseOut{}
	for _, p := range in.Paths {
		if r, held := s.state.Reservations[p]; held && r.Agent == in.Agent {
			delete(s.state.Reservations, p)
			out.Released = append(out.Released, p)
		} else {
			out.Skipped = append(out.Skipped, p)
		}
	}
	s.save()
	return nil, out, nil
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func defaultStateDir() string {
	if v := os.Getenv("COORD_STATE_DIR"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".mcp-coord"
	}
	return filepath.Join(home, ".mcp-coord")
}

func main() {
	addrDefault := os.Getenv("COORD_ADDR")
	if addrDefault == "" {
		addrDefault = "127.0.0.1:7767"
	}
	addr := flag.String("addr", addrDefault, "listen address (keep on localhost or a fully trusted network)")
	stateDir := flag.String("state", defaultStateDir(), "state directory (JSON state + markdown thread archive)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("mcp-coord-go", version)
		return
	}

	store, err := newStore(*stateDir)
	if err != nil {
		log.Fatalf("state dir: %v", err)
	}
	c := &coord{store: store}

	srv := mcp.NewServer(&mcp.Implementation{Name: "mcp-coord-go", Version: version}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_send", Description: "Send a message to other agent sessions (or broadcast with to=[\"*\"]). Returns the thread id."}, c.send)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_poll", Description: "Fetch messages addressed to you that you haven't seen yet. Call this at session start and before cross-cutting decisions."}, c.poll)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_thread", Description: "Read the full history of one thread."}, c.thread)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_agents", Description: "List known agents (with last-seen), active resource reservations, and thread ids."}, c.agents)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_reserve", Description: "Advisory-reserve shared resources (paths, repos, hosts) so agents don't collide. Errors with holder+reason on conflict."}, c.reserve)
	mcp.AddTool(srv, &mcp.Tool{Name: "coord_release", Description: "Release resource reservations you hold."}, c.release)

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.Handle("/mcp/", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok "+version)
	})

	log.Printf("mcp-coord-go %s listening on http://%s/mcp (state: %s)", version, *addr, *stateDir)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

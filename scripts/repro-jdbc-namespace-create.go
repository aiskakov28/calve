//go:build ignore

// Race concurrent creates of one new namespace and record every response.
//
// Iceberg's JdbcCatalog checks that a namespace is absent, then inserts its
// properties. A create that passes the check but loses the insert gets a
// duplicate-key SQL error, which the REST server returns as HTTP 500 instead of
// 409. A sequential control first shows the catalog answers a repeated create
// with 409. Each request is sent once, without redirects; after every round
// the namespace is read back to check that only the winner's properties exist.
//
//	go run scripts/repro-jdbc-namespace-create.go -catalog http://localhost:8181
//
// Exit status: 3 reproduced, 0 not reproduced, 1 on any other failure.
// Standard library only; see docs/triage-jdbc-namespace-create.md.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type exchange struct {
	Client     int    `json:"client,omitempty"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	ErrorType  string `json:"error_type,omitempty"`
	Message    string `json:"message,omitempty"`
	Cause      string `json:"cause,omitempty"`
	TransportE string `json:"transport_error,omitempty"`
	StartNS    int64  `json:"start_ns"`
	EndNS      int64  `json:"end_ns"`
	body       []byte
}

type round struct {
	Namespace  string     `json:"namespace"`
	Creates    []exchange `json:"creates"`
	ReadBack   exchange   `json:"read_back"`
	Owner      string     `json:"stored_owner"`
	Winner     string     `json:"winner_owner"`
	Statuses   []int      `json:"statuses"`
	ServerErrs int        `json:"server_errors"`
}

type summary struct {
	Catalog     string         `json:"catalog"`
	Clients     int            `json:"clients"`
	Rounds      []round        `json:"rounds"`
	Control     []exchange     `json:"control"`
	Statuses    map[string]int `json:"statuses"`
	ServerErrs  int            `json:"server_errors_on_existing_namespace"`
	Reproduced  bool           `json:"reproduced"`
	Cleanup     []exchange     `json:"cleanup,omitempty"`
	Note        string         `json:"note"`
	StartedAt   string         `json:"started_at"`
	DurationSec float64        `json:"duration_sec"`
}

var (
	catalog   = flag.String("catalog", "http://localhost:8181", "catalog root, without /v1")
	warehouse = flag.String("warehouse", "", "warehouse passed to /v1/config, if the catalog needs one")
	clients   = flag.Int("clients", 6, "concurrent creates per round")
	rounds    = flag.Int("rounds", 20, "rounds, each on a fresh namespace")
	out       = flag.String("out", "", "directory for summary.json (default: a new repro-namespace-* directory)")
	token     = os.Getenv("CATALOG_TOKEN") // optional bearer token; never written to the evidence
	client    = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	start     = time.Now()
)

func call(method, path string, body any) exchange {
	x := exchange{Method: method, Path: path, StartNS: time.Since(start).Nanoseconds()}
	defer func() { x.EndNS = time.Since(start).Nanoseconds() }()
	var reader io.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, strings.TrimRight(*catalog, "/")+path, reader)
	if err != nil {
		x.TransportE = err.Error()
		return x
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		x.TransportE = err.Error()
		x.EndNS = time.Since(start).Nanoseconds()
		return x
	}
	defer resp.Body.Close()
	x.Status = resp.StatusCode
	x.body, err = io.ReadAll(resp.Body)
	if err != nil {
		x.TransportE = "read body: " + err.Error()
	}
	var e struct {
		Error struct {
			Type    string   `json:"type"`
			Message string   `json:"message"`
			Stack   []string `json:"stack"`
		} `json:"error"`
	}
	if json.Unmarshal(x.body, &e) == nil {
		x.ErrorType, x.Message = e.Error.Type, e.Error.Message
		for _, line := range e.Error.Stack {
			if strings.HasPrefix(line, "Caused by:") {
				x.Cause = line
			}
		}
	}
	x.EndNS = time.Since(start).Nanoseconds()
	return x
}

// errReproduced ends run after its deferred cleanup and evidence write.
var errReproduced = errors.New("reproduced")

func main() {
	flag.Parse()
	switch err := run(); {
	case errors.Is(err, errReproduced):
		os.Exit(3)
	case err != nil:
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() (err error) {
	if *clients < 2 || *rounds < 1 {
		return errors.New("need at least 2 clients and 1 round")
	}
	dir := *out
	if dir == "" {
		dir = "repro-namespace-" + start.UTC().Format("20060102T150405Z")
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty; use a new directory", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	s := summary{Catalog: *catalog, Clients: *clients, Statuses: map[string]int{}, StartedAt: start.UTC().Format(time.RFC3339),
		Note: "Each 5xx counted here answered a create for a namespace that existed, with only the winner's properties, when the round ended."}
	// Runs after cleanup. Without its evidence a run has not completed, so a
	// failed write replaces a reproduction with an error.
	defer func() {
		s.DurationSec = time.Since(start).Seconds()
		path := filepath.Join(dir, "summary.json")
		data, _ := json.MarshalIndent(s, "", "  ")
		if werr := os.WriteFile(path, append(data, '\n'), 0o644); werr != nil {
			if errors.Is(err, errReproduced) {
				err = nil
			}
			err = errors.Join(err, fmt.Errorf("write evidence: %w", werr))
			return
		}
		fmt.Printf("evidence in %s\n", path)
	}()

	cfgPath := "/v1/config"
	if *warehouse != "" {
		cfgPath += "?warehouse=" + url.QueryEscape(*warehouse)
	}
	cfg := call("GET", cfgPath, nil)
	if cfg.Status != 200 {
		return fmt.Errorf("config: HTTP %d %s", cfg.Status, cfg.TransportE)
	}
	var config struct{ Defaults, Overrides map[string]string }
	json.Unmarshal(cfg.body, &config)
	prefix := config.Overrides["prefix"]
	if prefix == "" {
		prefix = config.Defaults["prefix"]
	}
	base := "/v1/"
	if prefix != "" {
		base += strings.Trim(prefix, "/") + "/"
	}
	nonce := make([]byte, 6)
	rand.Read(nonce)
	id := hex.EncodeToString(nonce)
	var created []string
	// A namespace left behind is a failed run, whatever it showed.
	defer func() {
		var left []string
		for _, ns := range created {
			x := call("DELETE", base+"namespaces/"+url.PathEscape(ns), nil)
			s.Cleanup = append(s.Cleanup, x)
			if x.Status < 200 || x.Status > 299 {
				left = append(left, fmt.Sprintf("%s (HTTP %d %s)", ns, x.Status, x.TransportE))
			}
		}
		if len(left) > 0 {
			if errors.Is(err, errReproduced) {
				err = nil
			}
			err = errors.Join(err, fmt.Errorf("cleanup left %d namespaces: %s", len(left), strings.Join(left, ", ")))
		}
	}()

	// Control: one create, then the same create again after it has finished.
	control := "repro_ns_" + id + "_control"
	first := call("POST", base+"namespaces", map[string]any{"namespace": []string{control}})
	if first.Status == 200 {
		created = append(created, control)
	}
	second := call("POST", base+"namespaces", map[string]any{"namespace": []string{control}})
	s.Control = []exchange{first, second}
	if first.Status != 200 || second.Status != 409 {
		return fmt.Errorf("control: want 200 then 409, got %d then %d", first.Status, second.Status)
	}
	fmt.Printf("control: 200 then 409\n")

	for r := 0; r < *rounds; r++ {
		ns := fmt.Sprintf("repro_ns_%s_%d", id, r)
		rd := round{Namespace: ns, Creates: make([]exchange, *clients)}
		var ready, done sync.WaitGroup
		gate := make(chan struct{})
		ready.Add(*clients)
		done.Add(*clients)
		for i := 0; i < *clients; i++ {
			go func(i int) {
				defer done.Done()
				ready.Done()
				<-gate
				body := map[string]any{"namespace": []string{ns}, "properties": map[string]string{"owner": fmt.Sprintf("client-%d", i)}}
				rd.Creates[i] = call("POST", base+"namespaces", body)
				rd.Creates[i].Client = i
			}(i)
		}
		ready.Wait()
		close(gate)
		done.Wait()

		winners := 0
		var unexpected error
		for _, x := range rd.Creates {
			rd.Statuses = append(rd.Statuses, x.Status)
			s.Statuses[fmt.Sprint(x.Status)]++
			switch {
			case x.Status == 200:
				winners++
				rd.Winner = fmt.Sprintf("client-%d", x.Client)
			case x.Status == 409, x.Status >= 500:
			case unexpected == nil:
				unexpected = fmt.Errorf("%s: client %d got unexpected HTTP %d %s", ns, x.Client, x.Status, x.TransportE)
			}
		}
		if winners > 0 {
			created = append(created, ns)
		}
		if unexpected != nil {
			s.Rounds = append(s.Rounds, rd)
			return unexpected
		}
		sort.Ints(rd.Statuses)
		rd.ReadBack = call("GET", base+"namespaces/"+url.PathEscape(ns), nil)
		var loaded struct {
			Properties map[string]string `json:"properties"`
		}
		json.Unmarshal(rd.ReadBack.body, &loaded)
		rd.Owner = loaded.Properties["owner"]
		if winners != 1 || rd.ReadBack.Status != 200 || rd.Owner != rd.Winner {
			s.Rounds = append(s.Rounds, rd)
			return fmt.Errorf("%s: want one 200 and the winner's properties stored; got %d winners, read-back HTTP %d, owner %q (winner %q)",
				ns, winners, rd.ReadBack.Status, rd.Owner, rd.Winner)
		}
		for _, x := range rd.Creates {
			if x.Status >= 500 {
				rd.ServerErrs++
			}
		}
		s.ServerErrs += rd.ServerErrs
		s.Rounds = append(s.Rounds, rd)
		fmt.Printf("%s: %v\n", ns, rd.Statuses)
	}
	s.Reproduced = s.ServerErrs > 0
	for _, rd := range s.Rounds {
		for _, x := range rd.Creates {
			if x.Status >= 500 {
				fmt.Printf("witness: %s client %d: HTTP %d %s: %s\n  %s\n", rd.Namespace, x.Client, x.Status, x.ErrorType, x.Message, x.Cause)
				break
			}
		}
		if rd.ServerErrs > 0 {
			break
		}
	}
	fmt.Printf("statuses %v; 5xx for an existing namespace: %d; reproduced: %v\n", s.Statuses, s.ServerErrs, s.Reproduced)
	if s.Reproduced {
		return errReproduced
	}
	return nil
}

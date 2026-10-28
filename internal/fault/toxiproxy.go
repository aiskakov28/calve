package fault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Toxiproxy is the network-fault Controller for latency, reset and partition
// (issue #13). Calve's clients reach each catalog through a Toxiproxy proxy;
// Proxies maps a plan's logical target ID to that proxy's name. Every toxic is
// named after the injection's ObjectName, so Heal only touches this run's toxics.
type Toxiproxy struct {
	API     string
	Proxies map[string]string
	HTTP    *http.Client
}

// NewToxiproxy returns a controller for the Toxiproxy API at api.
func NewToxiproxy(api string, proxies map[string]string) *Toxiproxy {
	return &Toxiproxy{API: strings.TrimRight(api, "/"), Proxies: proxies, HTTP: &http.Client{}}
}

type toxic struct {
	Name       string           `json:"name"`
	Type       string           `json:"type"`
	Stream     string           `json:"stream"`
	Toxicity   float64          `json:"toxicity"`
	Attributes map[string]int64 `json:"attributes"`
}

// toxics returns the proxy and the exact toxics an injection must create.
func (t *Toxiproxy) toxics(inj Injection) (string, []toxic, error) {
	proxy, ok := t.Proxies[inj.Event.Target]
	if !ok {
		return "", nil, fmt.Errorf("toxiproxy: no proxy for target %q", inj.Event.Target)
	}
	p := inj.Event.Parameters
	if p.Toxicity != 1 {
		return "", nil, fmt.Errorf("toxiproxy: %s toxicity must be 1, got %v", inj.Event.ID, p.Toxicity)
	}
	name := inj.ObjectName()
	stream := func() (string, error) {
		if p.Direction != "upstream" && p.Direction != "downstream" {
			return "", fmt.Errorf("toxiproxy: %s direction must be upstream or downstream, got %q", inj.Event.ID, p.Direction)
		}
		return p.Direction, nil
	}
	switch inj.Event.Kind {
	case Latency:
		s, err := stream()
		if err != nil {
			return "", nil, err
		}
		if p.Latency <= 0 || p.Latency%time.Millisecond != 0 || p.Jitter != 0 {
			return "", nil, fmt.Errorf("toxiproxy: %s needs a positive whole-millisecond latency and zero jitter", inj.Event.ID)
		}
		return proxy, []toxic{{Name: name, Type: "latency", Stream: s, Toxicity: 1,
			Attributes: map[string]int64{"latency": p.Latency.Milliseconds(), "jitter": 0}}}, nil
	case Reset:
		s, err := stream()
		if err != nil {
			return "", nil, err
		}
		return proxy, []toxic{{Name: name, Type: "reset_peer", Stream: s, Toxicity: 1,
			Attributes: map[string]int64{"timeout": 0}}}, nil
	case Partition:
		if p.Direction != "both" || p.Timeout != 0 {
			return "", nil, fmt.Errorf("toxiproxy: %s partition needs direction both and timeout 0", inj.Event.ID)
		}
		// timeout 0 holds data until the toxic is removed: blocked, not reset.
		return proxy, []toxic{
			{Name: name + "-up", Type: "timeout", Stream: "upstream", Toxicity: 1, Attributes: map[string]int64{"timeout": 0}},
			{Name: name + "-down", Type: "timeout", Stream: "downstream", Toxicity: 1, Attributes: map[string]int64{"timeout": 0}},
		}, nil
	default:
		return "", nil, fmt.Errorf("toxiproxy: %s is not a network fault", inj.Event.Kind)
	}
}

// Apply creates the toxics. A toxic that already exists (an earlier uncertain
// apply) is left for Observe to verify against the plan.
func (t *Toxiproxy) Apply(ctx context.Context, inj Injection) error {
	proxy, want, err := t.toxics(inj)
	if err != nil {
		return err
	}
	for _, tx := range want {
		status, body, err := t.call(ctx, http.MethodPost, "/proxies/"+url.PathEscape(proxy)+"/toxics", tx)
		if err != nil {
			return err
		}
		if status != http.StatusOK && status != http.StatusConflict {
			return fmt.Errorf("toxiproxy: add %s: HTTP %d %s", tx.Name, status, body)
		}
	}
	return nil
}

// Heal removes this injection's toxics. It is idempotent: a toxic that was
// never created, or is already gone, counts as healed.
func (t *Toxiproxy) Heal(ctx context.Context, inj Injection) error {
	proxy, want, err := t.toxics(inj)
	if err != nil {
		return err
	}
	for _, tx := range want {
		status, body, err := t.call(ctx, http.MethodDelete, "/proxies/"+url.PathEscape(proxy)+"/toxics/"+url.PathEscape(tx.Name), nil)
		if err != nil {
			return err
		}
		if status != http.StatusNoContent && status != http.StatusOK && status != http.StatusNotFound {
			return fmt.Errorf("toxiproxy: remove %s: HTTP %d %s", tx.Name, status, body)
		}
	}
	return nil
}

// Observe reads the proxy back: Active when every planned toxic is present
// exactly as planned, Healthy when none are, Unknown with an error otherwise.
func (t *Toxiproxy) Observe(ctx context.Context, inj Injection) (State, error) {
	proxy, want, err := t.toxics(inj)
	if err != nil {
		return Unknown, err
	}
	status, body, err := t.call(ctx, http.MethodGet, "/proxies/"+url.PathEscape(proxy), nil)
	if err != nil {
		return Unknown, err
	}
	if status != http.StatusOK {
		return Unknown, fmt.Errorf("toxiproxy: read proxy %s: HTTP %d %s", proxy, status, body)
	}
	var got struct {
		Enabled bool    `json:"enabled"`
		Toxics  []toxic `json:"toxics"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		return Unknown, fmt.Errorf("toxiproxy: decode proxy %s: %w", proxy, err)
	}
	if !got.Enabled {
		return Unknown, fmt.Errorf("toxiproxy: proxy %s is disabled", proxy)
	}
	present := 0
	for _, w := range want {
		for _, g := range got.Toxics {
			if g.Name != w.Name {
				continue
			}
			if !sameToxic(g, w) {
				return Unknown, fmt.Errorf("toxiproxy: toxic %s differs from the plan", w.Name)
			}
			present++
		}
	}
	switch present {
	case len(want):
		return Active, nil
	case 0:
		return Healthy, nil
	default:
		return Unknown, fmt.Errorf("toxiproxy: %d of %d toxics for %s present", present, len(want), inj.ObjectName())
	}
}

func sameToxic(got, want toxic) bool {
	if got.Type != want.Type || got.Stream != want.Stream || got.Toxicity != want.Toxicity {
		return false
	}
	for k, v := range want.Attributes {
		if got.Attributes[k] != v {
			return false
		}
	}
	return true
}

func (t *Toxiproxy) call(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.API+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.HTTP.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("toxiproxy: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, errors.Join(fmt.Errorf("toxiproxy: %s %s: read body", method, path), err)
	}
	return resp.StatusCode, bytes.TrimSpace(data), nil
}

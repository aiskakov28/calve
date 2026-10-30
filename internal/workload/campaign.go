package workload

import (
	"encoding/json"
	"time"
)

// CampaignHooks let a fault campaign drive a workload (docs/fault-campaigns.md).
// A nil *CampaignHooks means no campaign: the workload behaves as without faults.
type CampaignHooks struct {
	// Link is recorded in the run record: fault_run_id, fault_seed, fault_plan
	// and fault_history.
	Link map[string]any
	// Ready is called once prepare is done (setup and baseline reads) and
	// returns when the fault plan has started.
	Ready func()
	// Stop closes at the plan's horizon: start no new rounds or attempts.
	Stop <-chan struct{}
	// Loaded is called when the load phase has ended, before waiting for Healed.
	Loaded func()
	// Healed closes once every fault is healed and the catalog answers: run the
	// final reads.
	Healed <-chan struct{}
}

// retryRead is the pause before reading again after a failed read under a
// campaign. Reads change nothing, so repeating them cannot create an effect.
const retryRead = 250 * time.Millisecond

func (h *CampaignHooks) ready() {
	if h != nil && h.Ready != nil {
		h.Ready()
	}
}

// stopped reports whether no new round or attempt should start.
func (h *CampaignHooks) stopped() bool {
	if h == nil || h.Stop == nil {
		return false
	}
	select {
	case <-h.Stop:
		return true
	default:
		return false
	}
}

// waitRetry pauses before a read is retried and reports false if the plan's
// horizon arrives first.
func (h *CampaignHooks) waitRetry() bool {
	timer := time.NewTimer(retryRead)
	defer timer.Stop()
	select {
	case <-h.Stop:
		return false
	case <-timer.C:
		return true
	}
}

func (h *CampaignHooks) waitHealed() {
	if h == nil {
		return
	}
	if h.Loaded != nil {
		h.Loaded()
	}
	if h.Healed != nil {
		<-h.Healed
	}
}

// runRecord encodes a run record, adding the campaign's link fields.
func runRecord(run any, hooks *CampaignHooks) ([]byte, error) {
	if hooks == nil || len(hooks.Link) == 0 {
		return json.Marshal(run)
	}
	encoded, err := json.Marshal(run)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	for k, v := range hooks.Link {
		fields[k] = v
	}
	return json.Marshal(fields)
}

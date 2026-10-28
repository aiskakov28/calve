package fault

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Docker is the process-fault Controller for kill, pause and restart (issue
// #14). Containers maps a plan's logical target ID to a container name or ID.
// Every fault acts on that same container, so its storage survives: Heal starts
// or unpauses it and never recreates it.
type Docker struct {
	Containers map[string]string
	// run executes the docker CLI; tests replace it.
	run func(ctx context.Context, args ...string) ([]byte, error)
}

// NewDocker returns a controller that drives containers through the docker CLI.
func NewDocker(containers map[string]string) *Docker {
	return &Docker{Containers: containers, run: dockerCLI}
}

func dockerCLI(ctx context.Context, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

type containerState struct {
	Running       bool
	Paused        bool
	Restarting    bool
	ExitCode      int
	RestartPolicy string
}

func (d *Docker) container(inj Injection) (string, error) {
	c, ok := d.Containers[inj.Event.Target]
	if !ok {
		return "", fmt.Errorf("docker: no container for target %q", inj.Event.Target)
	}
	switch inj.Event.Kind {
	case Kill:
		if inj.Event.Parameters.Signal != "SIGKILL" {
			return "", fmt.Errorf("docker: %s kill must use SIGKILL, got %q", inj.Event.ID, inj.Event.Parameters.Signal)
		}
	case Pause:
	case Restart:
		g := inj.Event.Parameters.Grace
		if inj.Event.Parameters.Signal != "SIGTERM" || g < 0 || g%time.Second != 0 {
			return "", fmt.Errorf("docker: %s restart needs SIGTERM and a whole-second grace", inj.Event.ID)
		}
	default:
		return "", fmt.Errorf("docker: %s is not a process fault", inj.Event.Kind)
	}
	return c, nil
}

// Apply kills, pauses or stops the container.
func (d *Docker) Apply(ctx context.Context, inj Injection) error {
	c, err := d.container(inj)
	if err != nil {
		return err
	}
	switch inj.Event.Kind {
	case Kill:
		_, err = d.run(ctx, "kill", "--signal", "KILL", c)
	case Pause:
		_, err = d.run(ctx, "pause", c)
	case Restart:
		grace := strconv.Itoa(int(inj.Event.Parameters.Grace / time.Second))
		_, err = d.run(ctx, "stop", "--signal", "TERM", "--time", grace, c)
	}
	return err
}

// Heal unpauses or starts the same container, depending on what it observes.
// It is idempotent and also repairs an uncertain apply of any of the three kinds.
func (d *Docker) Heal(ctx context.Context, inj Injection) error {
	c, err := d.container(inj)
	if err != nil {
		return err
	}
	st, err := d.inspect(ctx, c)
	if err != nil {
		return err
	}
	if st.Paused {
		if _, err := d.run(ctx, "unpause", c); err != nil {
			return err
		}
	} else if !st.Running {
		if _, err := d.run(ctx, "start", c); err != nil {
			return err
		}
	}
	return nil
}

// Observe reports Healthy when the container runs unpaused and Active when it is
// in the state the fault produces. An automatic restart policy is an error: it
// could end a planned outage early.
func (d *Docker) Observe(ctx context.Context, inj Injection) (State, error) {
	c, err := d.container(inj)
	if err != nil {
		return Unknown, err
	}
	st, err := d.inspect(ctx, c)
	if err != nil {
		return Unknown, err
	}
	if st.RestartPolicy != "" && st.RestartPolicy != "no" {
		return Unknown, fmt.Errorf("docker: %s has restart policy %q; process faults need \"no\"", c, st.RestartPolicy)
	}
	if st.Running && !st.Paused && !st.Restarting {
		return Healthy, nil
	}
	switch {
	case inj.Event.Kind == Kill && !st.Running && st.ExitCode == 137,
		inj.Event.Kind == Pause && st.Running && st.Paused,
		inj.Event.Kind == Restart && !st.Running && !st.Restarting:
		return Active, nil
	}
	return Unknown, fmt.Errorf("docker: %s is running=%v paused=%v restarting=%v exit=%d, not the planned %s state",
		c, st.Running, st.Paused, st.Restarting, st.ExitCode, inj.Event.Kind)
}

func (d *Docker) inspect(ctx context.Context, c string) (containerState, error) {
	out, err := d.run(ctx, "inspect", "--format", "{{json .State}}|{{.HostConfig.RestartPolicy.Name}}", c)
	if err != nil {
		return containerState{}, err
	}
	stateJSON, policy, ok := strings.Cut(strings.TrimSpace(string(out)), "|")
	if !ok {
		return containerState{}, fmt.Errorf("docker: unexpected inspect output for %s", c)
	}
	var st containerState
	if err := json.Unmarshal([]byte(stateJSON), &st); err != nil {
		return containerState{}, fmt.Errorf("docker: decode state of %s: %w", c, err)
	}
	st.RestartPolicy = policy
	return st, nil
}

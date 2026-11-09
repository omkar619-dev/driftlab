// The faults: freezing brokers, cutting them off and killing them, with
// docker, while a run goes on, and watching what the cluster does about it.
// Everything from startCut down is the drivers' own code, moved here from
// drivers/kafkago, drivers/confluent and drivers/franzgo/faults, where it is
// the same byte for byte. One word in a comment changed: the helper is now
// the driver's reader (DESIGN.md, Driver split).

package main

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// config holds the settings the faults take from the runner's flags: the
// image the cut runs nft from, and the port the brokers fetch from each other
// on, which the cut drops.
type config struct {
	nftImage string
	cutPort  int
}

// startFault puts the fault in place and returns it, as startFault did in
// each driver: it picks the fault by its kind, with the followers and the
// hold the scenario gives it.
func (k *kafka) startFault(ctx context.Context, rec *recorder, topic string, f fault) (*activeFault, error) {
	switch f.kind {
	case "cut":
		return startCut(ctx, k.adm, rec, topic, f.followers, f.hold, k.c)
	case "kill":
		return startKill(ctx, k.adm, rec, topic, f.hold)
	case "pause":
		return startPause(ctx, k.adm, rec, topic, f.followers, f.hold)
	}
	return nil, fmt.Errorf("unknown fault %q", f.kind)
}

// startCut cuts the fetches of n of the brokers that hold partition 0's other
// copies, or of all of them when n is 0, but never its leader's, and heals them
// after hold on a goroutine of its own, because the stream goes on meanwhile.
// watchISR notes when the in-sync set drops them. A cut broker stops copying
// from the leader but keeps checking in with the controller and answering
// clients, so it stays in the in-sync set until the leader drops it for
// lagging, after 30 seconds. Until then the leader stores each batch but can't
// count it as copied, and once the request's timeout runs out it answers
// REQUEST_TIMED_OUT. The cut only counts as done if nft showed the drop rule in
// every cut broker's network, and its ok records the partition's in-sync set as
// the leader saw it just before the heal. Containers are named after the
// brokers' node IDs, as deploy/kafka.yaml names them.
func startCut(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, n int, hold time.Duration, c config) (*activeFault, error) {
	leader, hit, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(hit) {
		hit = hit[:n]
	}
	names := make([]string, len(hit))
	for i, id := range hit {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "cut", Detail: map[string]any{"leader": leader, "cut": names}})
	began := time.Now()
	for _, name := range names {
		if _, err := nft(c, name, cutRules(c.cutPort), "-f", "-"); err != nil {
			healCuts(c, names) // never leave one of them cut
			return nil, fmt.Errorf("%w (is the image built? docker build -t %s deploy/nft)", err, c.nftImage)
		}
	}
	took, checkErr := cutsInPlace(c, names)
	ct := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ct.over)
		end := time.Now().Add(hold)
		watchISR(ctx, cl, rec, leader, topic, hit, began, end)
		time.Sleep(time.Until(end))
		isr, isrErr := leaderISR(ctx, cl, leader, topic)
		// Heal whatever the check said, so a run never leaves brokers cut.
		if err := healCuts(c, names); err != nil {
			ct.err = err
			return
		}
		if checkErr != nil {
			ct.err = checkErr
			return
		}
		if took {
			var detail any
			if isrErr == nil {
				detail = map[string]any{"isr": isr}
			}
			rec.op(op{Process: "nemesis", Type: "ok", F: "cut", Detail: detail})
		}
	}()
	return ct, nil
}

// cutTable is the nftables table the cut adds to a broker's network
// namespace. Its one chain drops every packet the broker sends to the port
// the brokers fetch from each other on, so it stops copying from the
// leaders, while its traffic to the controllers and to clients carries on.
// Deleting the table ends the cut.
const cutTable = "driftlab"

// cutRules is the cut's ruleset, in the form nft -f reads.
func cutRules(port int) string {
	return fmt.Sprintf("table inet %s {\n\tchain fetches {\n\t\ttype filter hook output priority 0; policy accept;\n\t\ttcp dport %d drop\n\t}\n}\n", cutTable, port)
}

// nft runs nft inside a broker container's network namespace, from a
// throwaway container of the -nft-image image that joins that namespace with
// the right to change its firewall. stdin, if not empty, is what nft reads.
func nft(c config, name, stdin string, args ...string) (string, error) {
	full := []string{"run", "--rm", "--network", "container:" + name, "--cap-add", "NET_ADMIN"}
	if stdin != "" {
		full = append(full, "-i")
	}
	full = append(full, c.nftImage, "nft")
	return dockerIn(stdin, append(full, args...)...)
}

// cutsInPlace says whether nft shows the drop rule in every named broker's
// network.
func cutsInPlace(c config, names []string) (bool, error) {
	rule := fmt.Sprintf("tcp dport %d drop", c.cutPort)
	for _, name := range names {
		out, err := nft(c, name, "", "list", "table", "inet", cutTable)
		if err != nil {
			return false, err
		}
		if !strings.Contains(out, rule) {
			return false, nil
		}
	}
	return true, nil
}

// healCuts deletes the cut table from every named broker's network, and
// returns the first error.
func healCuts(c config, names []string) error {
	var first error
	for _, name := range names {
		if _, err := nft(c, name, "", "delete", "table", "inet", cutTable); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// clearCuts deletes any cut an earlier run left behind on the topic's
// brokers. It ignores errors, most of which mean there was no cut to
// delete; a missing image shows up when the cut starts.
func clearCuts(ctx context.Context, cl *kgo.Client, topic string, c config) {
	leader, followers, err := followersOf(ctx, cl, topic)
	if err != nil {
		return
	}
	for _, id := range append(followers, leader) {
		nft(c, fmt.Sprintf("kafka-%d", id), "", "delete", "table", "inet", cutTable)
	}
}

// startPause freezes n of the brokers that hold partition 0's other copies, or
// all of them when n is 0, with docker pause, but never its leader, and thaws
// them after hold, on a goroutine of its own, because the stream goes on
// meanwhile. With two of the three brokers frozen there is also no controller
// quorum, so nothing can take them out of the in-sync set: the leader stores
// each batch but can never get it copied. With one frozen, the controller
// fences it, and watchISR notes when that drops it from the set. Unlike a cut
// broker, a frozen one doesn't answer clients either. The freeze only counts as
// done if docker showed every follower paused, and its ok records the
// partition's in-sync set as the leader saw it just before the thaw. Containers
// are named after the brokers' node IDs, as deploy/kafka.yaml names them.
func startPause(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, n int, hold time.Duration) (*activeFault, error) {
	leader, hit, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(hit) {
		hit = hit[:n]
	}
	names := make([]string, len(hit))
	for i, id := range hit {
		names[i] = fmt.Sprintf("kafka-%d", id)
	}
	rec.op(op{Process: "nemesis", Type: "invoke", F: "pause", Detail: map[string]any{"leader": leader, "paused": names}})
	began := time.Now()
	if _, err := dockerIn("", append([]string{"pause"}, names...)...); err != nil {
		dockerIn("", append([]string{"unpause"}, names...)...) // never leave one of them frozen
		return nil, err
	}
	paused, checkErr := dockerIn("", append([]string{"inspect", "-f", "{{.State.Paused}}"}, names...)...)
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		end := time.Now().Add(hold)
		watchISR(ctx, cl, rec, leader, topic, hit, began, end)
		time.Sleep(time.Until(end))
		isr, isrErr := leaderISR(ctx, cl, leader, topic)
		// Thaw them whatever the check said, so a run never leaves brokers
		// frozen.
		if _, err := dockerIn("", append([]string{"unpause"}, names...)...); err != nil {
			ft.err = err
			return
		}
		if checkErr != nil {
			ft.err = checkErr
			return
		}
		if allTrue(paused, len(names)) {
			var detail any
			if isrErr == nil {
				detail = map[string]any{"isr": isr}
			}
			rec.op(op{Process: "nemesis", Type: "ok", F: "pause", Detail: detail})
		}
	}()
	return ft, nil
}

// watchISR asks the leader every 250 milliseconds for partition 0's in-sync
// set, until none of the faulted brokers is in it or end passes, and records
// an info the first time none is, with how long after began, when the fault
// was invoked: from then on the leader counts a write as copied without them,
// so the partition takes writes again. When two followers are frozen, the
// controller quorum goes with them, and the set never shrinks.
func watchISR(ctx context.Context, cl *kgo.Client, rec *recorder, leader int32, topic string, faulted []int32, began, end time.Time) {
	for time.Now().Before(end) {
		isr, err := leaderISR(ctx, cl, leader, topic)
		if err == nil && !slices.ContainsFunc(faulted, func(id int32) bool { return slices.Contains(isr, id) }) {
			rec.op(op{Process: "nemesis", Type: "info", F: "isr", Detail: map[string]any{"isr": isr, "after": time.Since(began).Round(time.Millisecond).String()}})
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// thawAll unpauses every broker in deploy/kafka.yaml. It ignores errors,
// which mostly mean the broker wasn't paused.
func thawAll() {
	for id := 1; id <= replicas; id++ {
		dockerIn("", "unpause", fmt.Sprintf("kafka-%d", id))
	}
}

// allTrue says whether docker inspect printed "true" once per container.
func allTrue(out string, n int) bool {
	fields := strings.Fields(out)
	if n == 0 || len(fields) != n {
		return false
	}
	for _, f := range fields {
		if f != "true" {
			return false
		}
	}
	return true
}

// startKill kills the broker that leads partition 0 with docker kill, the
// way a crash would, and starts it again after hold, on a goroutine of its
// own, because the stream goes on meanwhile. The kill only counts if docker
// shows the broker stopped and the cluster then gives partition 0 a new
// leader. Its ok comes when that leader shows up, the moment the partition
// can be used again, so a reader's recovery is measured from there, and it
// records how long the election took. The broker comes back as a follower,
// and the fault only ends once it is back in the in-sync set, so the next
// run starts with every broker; an info line says how long that took.
// Containers are named after the brokers' node IDs, as deploy/kafka.yaml
// names them.
func startKill(ctx context.Context, cl *kgo.Client, rec *recorder, topic string, hold time.Duration) (*activeFault, error) {
	leader, _, err := followersOf(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("kafka-%d", leader)
	rec.op(op{Process: "nemesis", Type: "invoke", F: "kill", Detail: map[string]any{"leader": leader, "killed": name}})
	killed := time.Now()
	if _, err := dockerIn("", "kill", name); err != nil {
		dockerIn("", "start", name) // never leave it down
		return nil, err
	}
	running, checkErr := dockerIn("", "inspect", "-f", "{{.State.Running}}", name)
	ft := &activeFault{over: make(chan struct{})}
	go func() {
		defer close(ft.over)
		next, after, found := waitForNewLeader(ctx, cl, topic, leader, killed, killed.Add(hold))
		if checkErr == nil && running == "false" && found {
			rec.op(op{Process: "nemesis", Type: "ok", F: "kill", Detail: map[string]any{"new_leader": next, "after": after.Round(time.Millisecond).String()}})
		}
		// Start it again whatever the checks said, so a run never leaves a
		// broker down.
		time.Sleep(time.Until(killed.Add(hold)))
		if _, err := dockerIn("", "start", name); err != nil {
			ft.err = err
			return
		}
		if checkErr != nil {
			ft.err = checkErr
			return
		}
		back := waitForISR(ctx, cl, topic, leader, time.Minute)
		rec.op(op{Process: "nemesis", Type: "info", F: "restart", Detail: map[string]any{"restarted": name, "back_in_sync_after": back}})
	}()
	return ft, nil
}

// waitForNewLeader asks every 250 milliseconds which broker leads partition
// 0, until one other than old does or deadline passes. It returns that
// leader and how long after killed it showed up.
func waitForNewLeader(ctx context.Context, cl *kgo.Client, topic string, old int32, killed, deadline time.Time) (int32, time.Duration, bool) {
	for time.Now().Before(deadline) {
		if leader, err := leaderOf(ctx, cl, topic); err == nil && leader >= 0 && leader != old {
			return leader, time.Since(killed), true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return -1, 0, false
}

// waitForISR asks every 250 milliseconds for partition 0's in-sync set,
// until it includes broker id or limit passes, and says how long that took.
func waitForISR(ctx context.Context, cl *kgo.Client, topic string, id int32, limit time.Duration) string {
	start := time.Now()
	for time.Since(start) < limit {
		if isr, err := isrOf(ctx, cl, topic); err == nil && slices.Contains(isr, id) {
			return time.Since(start).Round(time.Millisecond).String()
		}
		time.Sleep(250 * time.Millisecond)
	}
	return "not within " + limit.String()
}

// leaderOf asks which broker leads partition 0, and gives the request two
// seconds, so a broker that doesn't answer can't hold up the next try.
func leaderOf(ctx context.Context, cl *kgo.Client, topic string) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	leader, _, err := followersOf(ctx, cl, topic)
	return leader, err
}

// isrOf asks for partition 0's in-sync set, and gives the request two
// seconds.
func isrOf(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, rt := range resp.Topics {
		for _, p := range rt.Partitions {
			if p.Partition == 0 {
				return p.ISR, nil
			}
		}
	}
	return nil, fmt.Errorf("no partition 0 of %s", topic)
}

// leaderISR asks partition 0's leader directly which replicas are in its
// in-sync set, and gives the request two seconds.
func leaderISR(ctx context.Context, cl *kgo.Client, leader int32, topic string) ([]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := cl.Broker(int(leader)).Request(ctx, req)
	if err != nil {
		return nil, err
	}
	mr, ok := resp.(*kmsg.MetadataResponse)
	if !ok || len(mr.Topics) != 1 {
		return nil, fmt.Errorf("broker %d gave an unexpected answer about %s", leader, topic)
	}
	for _, p := range mr.Topics[0].Partitions {
		if p.Partition == 0 {
			return p.ISR, nil
		}
	}
	return nil, fmt.Errorf("broker %d knows no partition 0 of %s", leader, topic)
}

// followersOf asks which broker leads partition 0 of the topic, and which
// brokers hold its other copies.
func followersOf(ctx context.Context, cl *kgo.Client, topic string) (int32, []int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, nil, fmt.Errorf("finding the followers of %s: %w", topic, err)
	}
	if len(resp.Topics) != 1 {
		return 0, nil, fmt.Errorf("finding the followers of %s: got %d topics back", topic, len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil {
		return 0, nil, fmt.Errorf("finding the followers of %s: %w", topic, err)
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.Partition != 0 {
			continue
		}
		var followers []int32
		for _, id := range p.Replicas {
			if id != p.Leader {
				followers = append(followers, id)
			}
		}
		return p.Leader, followers, nil
	}
	return 0, nil, fmt.Errorf("topic %s has no partition 0", topic)
}

// dockerIn runs one docker command with stdin, if not empty, as its input,
// and returns what it printed. It gives up after 30 seconds, so a docker
// daemon that hangs can't hang the run.
func dockerIn(stdin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return strings.TrimSpace(string(out)), nil
}

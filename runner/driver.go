// Running a driver: the runner starts it as a child process, writes commands
// on its stdin, reads its events from its stdout, and copies its stderr into
// its own (DESIGN.md, Driver split).

package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"
)

// driverDeadline bounds every wait on a driver: for an answer, and for the
// driver to exit once its stdin is closed. It is longer than any client's own
// delivery timeout, librdkafka's 5 minutes, so a client that is still
// retrying is never taken for a hung driver (DESIGN.md, When a driver breaks).
const driverDeadline = 10 * time.Minute

// answer names the event that answers each command. The other events, ok,
// info, fail, deliver and error, are reports: they come whenever something
// happens, not when asked.
var answer = map[string]string{
	"setup":        "ready",
	"subscribe":    "subscribed",
	"publish":      "taken",
	"close-writer": "writer-closed",
	"stop-reader":  "reader-stopped",
}

// isAnswer says whether an event answers a command.
func isAnswer(name string) bool {
	for _, a := range answer {
		if a == name {
			return true
		}
	}
	return false
}

// driver is a running driver program. An error from ask or finish ends the
// run: it says how the driver broke, and by then the driver has been
// stopped.
type driver struct {
	cmd      *exec.Cmd
	in       io.WriteCloser // the driver's stdin, where commands go
	deadline time.Duration
	onEvent  func(event)

	// answers carries each answer from the goroutine that reads the driver's
	// stdout to ask. The runner sends a command only once the last one is
	// answered, so one place is enough.
	answers chan event

	// ended is closed when the runner stops reading the driver's stdout: at
	// its end, or at a line that breaks the protocol, which readErr then
	// holds. exited is closed once the driver has exited, and exitErr says
	// how, nil meaning status 0. Each error is read only after its channel
	// is closed.
	ended   chan struct{}
	readErr error
	exited  chan struct{}
	exitErr error
}

// startDriver starts a driver: argv is its program, then its arguments.
// onEvent gets every event the driver writes, in order, as soon as it is
// read, on a goroutine of its own, so the runner can record each on its one
// clock straight away. The driver's stderr goes to logs, with clock's time in
// front of each line. deadline bounds every wait: driverDeadline in a run,
// less in the tests.
func startDriver(argv []string, onEvent func(event), logs io.Writer, clock func() time.Duration, deadline time.Duration) (*driver, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stderr = &logWriter{out: logs, clock: clock}
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	d := &driver{
		cmd:      cmd,
		in:       in,
		deadline: deadline,
		onEvent:  onEvent,
		answers:  make(chan event, 1),
		ended:    make(chan struct{}),
		exited:   make(chan struct{}),
	}
	go func() {
		d.readErr = d.read(out)
		close(d.ended)
		// After a line that broke the protocol, the rest is thrown away, so
		// the driver never waits on a full pipe before it is stopped.
		io.Copy(io.Discard, out)
		d.exitErr = cmd.Wait()
		close(d.exited)
	}()
	return d, nil
}

// read hands every event the driver writes to onEvent, and each answer on to
// ask, until the driver's stdout ends, which is no error, or until a line
// breaks the protocol, which it returns.
func (d *driver) read(out io.Reader) error {
	sc := bufio.NewScanner(out)
	for n := 0; sc.Scan(); n++ {
		line := sc.Bytes()
		e, err := parseEvent(line)
		if err != nil {
			return fmt.Errorf("wrote %s: %w", excerpt(line), err)
		}
		// The meta line, the history's first, is written from ready, so ready
		// comes first, and only once.
		if n == 0 && e.Event != "ready" {
			return fmt.Errorf("wrote %s before ready", e.Event)
		}
		if n > 0 && e.Event == "ready" {
			return errors.New("wrote ready twice")
		}
		// An answer that finds the last one still waiting for ask answers
		// nothing the runner asked.
		if isAnswer(e.Event) && len(d.answers) == cap(d.answers) {
			return fmt.Errorf("wrote %s, an answer to no command", e.Event)
		}
		d.onEvent(e)
		if isAnswer(e.Event) {
			d.answers <- e // never waits: only this goroutine sends, and there was room
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("stdout: %w", err)
	}
	return nil
}

// ask sends a command and waits for its answer, which it returns: the event
// that answers that command, and for publish, the value just sent. Any other
// answer, or none within the deadline, means the driver broke.
func (d *driver) ask(c command) (e event, err error) {
	defer func() {
		if err != nil {
			d.kill()
		}
	}()
	line, err := encodeCommand(c)
	if err != nil {
		return event{}, err
	}
	// The driver has read every earlier command, since it answered each, so
	// the pipe is all but empty and the write never waits. A write fails only
	// once the driver has stopped reading for good, and the wait below then
	// says why.
	d.in.Write(line)
	select {
	case e = <-d.answers:
	case <-d.ended:
		// An answer written just before stdout ended still counts.
		select {
		case e = <-d.answers:
		default:
			return event{}, d.broken()
		}
	case <-time.After(d.deadline):
		return event{}, fmt.Errorf("didn't answer %s within %v", c.name(), d.deadline)
	}
	if want := answer[c.name()]; e.Event != want {
		return event{}, fmt.Errorf("answered %s with %s", c.name(), e.Event)
	}
	if p, ok := c.(publishCmd); ok && e.Value != p.Value {
		return event{}, fmt.Errorf("answered publish %d with taken %d", p.Value, e.Value)
	}
	return e, nil
}

// broken says how the driver broke when its stdout stopped before an answer
// came: a line that broke the protocol, or, if stdout just ended, the driver
// exiting before the runner closed its stdin.
func (d *driver) broken() error {
	if d.readErr != nil {
		return d.readErr
	}
	select {
	case <-d.exited:
		return fmt.Errorf("exited before its stdin closed (%s)", status(d.exitErr))
	case <-time.After(d.deadline):
		return errors.New("closed its stdout but didn't exit")
	}
}

// finish ends a run that went well. It closes the driver's stdin, which tells
// the driver to exit, and waits until it has: with status 0, and with
// nothing written since its last answer that breaks the protocol.
func (d *driver) finish() error {
	d.in.Close()
	select {
	case <-d.exited:
	case <-time.After(d.deadline):
		d.kill()
		return fmt.Errorf("didn't exit within %v of its stdin closing", d.deadline)
	}
	if d.readErr != nil {
		return d.readErr
	}
	if d.exitErr != nil {
		return fmt.Errorf("exited with %v", d.exitErr)
	}
	select {
	case e := <-d.answers:
		return fmt.Errorf("wrote %s, an answer to no command", e.Event)
	default:
	}
	return nil
}

// kill stops the driver at once, for a run that can't go on, and returns
// once it has gone and its stderr is all copied. Its stdin is closed first,
// so a program the driver started sees its input end and exits too.
func (d *driver) kill() {
	d.in.Close()
	d.cmd.Process.Kill() // fails only if the driver has exited already
	<-d.exited
}

// status says how a process exited, from what Wait returned.
func status(err error) string {
	if err == nil {
		return "exit status 0"
	}
	return err.Error()
}

// excerpt quotes a line for an error message, cut at 200 bytes so a long one
// can't swamp the log.
func excerpt(line []byte) string {
	if len(line) > 200 {
		return fmt.Sprintf("%#q...", line[:200])
	}
	return fmt.Sprintf("%#q", line)
}

// logWriter copies the driver's stderr into the runner's, with the history's
// clock in front of each line, in seconds, as franz-go's log lines have it,
// so a client's log lines up with the history (DESIGN.md, Rules for a
// driver, rule 2). exec calls Write with whatever the driver wrote, in
// pieces of any size, so a line can come in parts.
type logWriter struct {
	out   io.Writer
	clock func() time.Duration
	mid   bool // the last piece ended partway through a line
}

func (w *logWriter) Write(p []byte) (int, error) {
	var b []byte
	for rest := p; len(rest) > 0; {
		if !w.mid {
			b = fmt.Appendf(b, "%.3f ", w.clock().Seconds())
		}
		line := rest
		if i := bytes.IndexByte(rest, '\n'); i >= 0 {
			line = rest[:i+1]
		}
		b = append(b, line...)
		w.mid = line[len(line)-1] != '\n'
		rest = rest[len(line):]
	}
	if _, err := w.out.Write(b); err != nil {
		return 0, err
	}
	return len(p), nil
}

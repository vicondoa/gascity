package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

// sessionEventResyncPokeDelay is the trailing delay between a stream resync
// and the reconcile poke it earns, and sessionEventResyncPokeMaxDefer caps
// how long consecutive resyncs may keep extending that delay. Resyncs open
// every connection cycle — including the benign cycle the stream starts for
// every newly detected agent pane — so an immediate poke would land a
// reconcile in the middle of the very start wave that triggered it
// (live-verified: the poked tick can race the in-flight create's meta stamp
// and roll it back; a cold provider-server start holds that window open for
// ~10s+). Each further resync re-arms the timer, so a start wave — whose
// every start emits a resubscribe resync — defers the poke past its own
// tail; the cap guarantees a flapping stream still gets its poll-now within
// a bounded time unless an async start is still in flight. Deaths are
// unaffected: attributed exits poke immediately, and the patrol scan remains
// the hard backstop behind everything.
const (
	sessionEventResyncPokeDelay    = 15 * time.Second
	sessionEventResyncPokeMaxDefer = time.Minute
	sessionEventResyncPokeRetry    = 250 * time.Millisecond
)

// sessionEventPump bridges a provider's push session-event stream
// (runtime.SessionEventProvider) into the reconciler's poke channel, so a
// session death becomes an immediate reconcile tick instead of waiting for
// the next patrol. Only attributed session deaths (exited, closed with a
// session name) and stream resyncs are forwarded — deaths immediately,
// resyncs on a trailing delay; agent-activity kinds have their own
// consumers, and unattributed pane noise is dropped (see forward).
// Events are level-triggered hints — the poked tick re-reads authoritative
// state (ListRunning et al.), so a replayed event costs at most one
// redundant reconcile. The poke channel's buffered-1 semantics plus the run
// loop's tick debouncer coalesce event bursts.
//
// Providers without an event stream (tmux) leave the pump inactive and every
// polled path untouched.
type sessionEventPump struct {
	parent         context.Context
	pokeCh         chan<- struct{}
	stderr         io.Writer
	logPrefix      string
	resyncDelay    time.Duration
	resyncMaxDefer time.Duration
	resyncGate     func() bool

	mu     sync.Mutex
	gen    int64              // subscription generation counter
	cancel context.CancelFunc // cancels the current subscription

	// streamGen holds the generation of the currently-established stream,
	// 0 when none. Forward goroutines clear only their own generation, so
	// a late close from a replaced subscription cannot mask a live one.
	streamGen atomic.Int64
	// streamLastFrame is the receipt time of the latest event or resync frame.
	// It is deliberately reset on every subscription restart: a sticky
	// generation is not proof that a reconnect is healthy.
	streamLastFrame atomic.Int64
}

// newSessionEventPump returns a pump whose subscriptions live within parent
// and poke pokeCh. Wire a provider with restart.
func newSessionEventPump(parent context.Context, pokeCh chan<- struct{}, stderr io.Writer, logPrefix string) *sessionEventPump {
	return &sessionEventPump{
		parent:         parent,
		pokeCh:         pokeCh,
		stderr:         stderr,
		logPrefix:      logPrefix,
		resyncDelay:    sessionEventResyncPokeDelay,
		resyncMaxDefer: sessionEventResyncPokeMaxDefer,
	}
}

// restart re-points the pump at sp's session-event stream, canceling any
// prior subscription. Providers that do not implement
// runtime.SessionEventProvider deactivate the pump. Callers serialize
// restarts (startup and config reload both run on the reconciler goroutine).
func (p *sessionEventPump) restart(sp runtime.Provider) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.gen++
	p.streamGen.Store(0)
	p.streamLastFrame.Store(0)
	sep, ok := sp.(runtime.SessionEventProvider)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(p.parent)
	events, err := sep.SubscribeSessionEvents(ctx)
	if err != nil {
		cancel()
		fmt.Fprintf(p.stderr, "%s: session-event subscribe: %v (session liveness stays on patrol polling)\n", p.logPrefix, err) //nolint:errcheck // best-effort stderr
		return
	}
	p.cancel = cancel
	p.streamGen.Store(p.gen)
	fmt.Fprintf(p.stderr, "%s: session-event stream active: session death pokes the reconciler\n", p.logPrefix) //nolint:errcheck // best-effort stderr
	go p.forward(ctx, p.gen, events)
}

// streaming reports whether a session-event stream is currently established.
func (p *sessionEventPump) streaming() bool {
	return p.streamGen.Load() != 0
}

// setResyncGate installs the current start-wave gate. A resync may be delayed
// past its max-defer timer while the reconciler has async starts in flight;
// callers supply the existing lifecycle tracker rather than creating another
// in-flight state machine.
func (p *sessionEventPump) setResyncGate(gate func() bool) {
	p.mu.Lock()
	p.resyncGate = gate
	p.mu.Unlock()
}

// streamHealthy reports whether the established stream has delivered a recent
// event or resync frame. A stream generation alone is sticky across provider
// reconnects, so callers that use events as a patrol safety-net replacement
// must require recent traffic as well.
func (p *sessionEventPump) streamHealthy(now time.Time, maxAge time.Duration) bool {
	if p.streamGen.Load() == 0 || maxAge <= 0 {
		return false
	}
	nanos := p.streamLastFrame.Load()
	if nanos == 0 {
		return false
	}
	age := now.Sub(time.Unix(0, nanos))
	if age < 0 {
		age = 0
	}
	return age <= maxAge
}

func (p *sessionEventPump) resyncPokeAllowed() bool {
	p.mu.Lock()
	gate := p.resyncGate
	p.mu.Unlock()
	return gate == nil || gate()
}

func (p *sessionEventPump) clearStream(gen int64) bool {
	if !p.streamGen.CompareAndSwap(gen, 0) {
		return false
	}
	p.streamLastFrame.Store(0)
	return true
}

// forward pumps liveness events into the poke channel until the stream ends.
func (p *sessionEventPump) forward(ctx context.Context, gen int64, events <-chan runtime.SessionEvent) {
	resyncTimer := time.NewTimer(p.resyncDelay)
	if !resyncTimer.Stop() {
		<-resyncTimer.C
	}
	defer resyncTimer.Stop()
	resyncArmed := false
	var resyncFirstArm time.Time
	for {
		select {
		case <-ctx.Done():
			p.clearStream(gen)
			return
		case <-resyncTimer.C:
			if !p.resyncPokeAllowed() {
				// The max-defer timer is a delivery bound, not permission to
				// reconcile against an in-flight start wave. Keep the existing
				// resync armed and retry against the same lifecycle tracker.
				resyncTimer.Reset(sessionEventResyncPokeRetry)
				continue
			}
			resyncArmed = false
			p.poke("resync", "")
		case ev, ok := <-events:
			if !ok {
				if p.clearStream(gen) && ctx.Err() == nil {
					fmt.Fprintf(p.stderr, "%s: session-event stream ended; session liveness falls back to patrol polling\n", p.logPrefix) //nolint:errcheck // best-effort stderr
				}
				return
			}
			p.streamLastFrame.Store(time.Now().UnixNano())
			switch ev.Kind {
			case runtime.SessionEventExited, runtime.SessionEventClosed:
				// Only attributed deaths poke. Unattributed pane events are
				// provider noise — most prominently the stray shell pane the
				// provider closes inside every agent start, which would poke
				// a reconcile into the middle of the very start wave that
				// caused it (live-verified: the poked tick can race the
				// in-flight create's meta stamp and roll it back). A death
				// the provider cannot attribute is covered by the next
				// resync or patrol scan.
				if ev.Session == "" {
					continue
				}
				p.poke(string(ev.Kind), ev.Session)
			case runtime.SessionEventResync:
				// Trailing-edge with a cap: the first resync arms the timer,
				// later ones re-arm it (a start wave keeps deferring its own
				// poke past its tail) until resyncMaxDefer forces the fire.
				// The existing async-start tracker may defer that capped fire
				// further so reconciliation never races an in-flight start.
				switch {
				case !resyncArmed:
					resyncTimer.Reset(p.resyncDelay)
					resyncArmed = true
					resyncFirstArm = time.Now()
				case time.Since(resyncFirstArm) < p.resyncMaxDefer:
					if !resyncTimer.Stop() {
						select {
						case <-resyncTimer.C:
						default:
						}
					}
					resyncTimer.Reset(p.resyncDelay)
				}
			}
		}
	}
}

// poke signals the reconciler without ever blocking; a full channel means a
// tick is already owed, which covers this event too.
func (p *sessionEventPump) poke(kind, session string) {
	select {
	case p.pokeCh <- struct{}{}:
		// Log only when the send lands: a replayed backlog burst fills the
		// buffer once and stays quiet.
		fmt.Fprintf(p.stderr, "%s: session event %s(%s) → reconcile poke\n", p.logPrefix, kind, session) //nolint:errcheck // best-effort stderr
	default:
	}
}

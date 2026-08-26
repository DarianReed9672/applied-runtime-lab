# Agent Loop Incident API: Capture Express Errors, Promise Rejections, and User IDs

**Short answer:** In a Node.js Express backend, capture each handled error, unhandled exception, and promise rejection as one normalized event with the original stack trace, request ID, user ID, agent-run ID, latency, and cost fields; then send it through a bounded API path that can fail without taking the process down.

For a B2B SaaS agent loop, an error inbox without cost attribution answers the least interesting question: something broke. The useful record answers which tenant paid for the failed work, which loop step consumed the time, what page fired, and whether retrying would repeat the charge. I've been woken by alerts that meant nothing and missed the one that mattered. That makes me distrust a dashboard that can graph exceptions but cannot reconstruct one request. This is the invariant: **the unit of correlation must be the agent run, not the process error.** A request can launch several model calls and tools before one rejected promise escapes. If their latency and cost live in separate telemetry, the postmortem turns into a join across clocks, sampling policies, and retention windows.

Context first.

## What should a Node.js Express error tracking API capture for an unhandled exception?

Start with two capture paths. The request path catches errors while Express still owns request context; it can attach the request ID, an internal user or tenant identifier, the active agent-run ID, the current step, and the stack trace. The process path observes `uncaughtException` and `unhandledRejection`, but it must accept that some request-local context may already be gone. Don't fabricate a user ID from the last request seen by the process. Missing context is evidence about the instrumentation boundary, while incorrect context can send responders into the wrong tenant's audit trail.

An event envelope for this workload needs more than a message and stack. It should carry a stable event ID for deduplication, a timestamp, failure class, handled state, service and release identifiers, request and run correlation, and a cost ledger whose units are explicit. Cost belongs at the agent-step level because a run can fail after several successful calls. Record the provider-reported usage or the internally calculated amount next to the model call that incurred it, then aggregate upward; never infer the failed run's cost from its wall-clock duration.

Privacy changes the shape of those fields. A raw email address is convenient during a quiet test and regrettable during an incident export. Use an opaque internal user ID or tenant ID, keep authentication secrets and prompts out of exception metadata, and apply the same redaction before every sink. The stack trace should retain function, file, and line information needed for grouping, but captured request data needs an allowlist. Headers and bodies are not harmless context. The collector below shows the contract rather than a product SDK. A Node.js process can serialize this envelope from its Express and process-level handlers; the receiving backend validates the fields, bounds the body, and acknowledges accepted events without coupling application code to a particular error tracker.

Keep it boring.

```go
package telemetry

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type Cost struct {
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
	Source   string `json:"source"`
}

type FailureEvent struct {
	EventID    string    `json:"event_id"`
	OccurredAt time.Time `json:"occurred_at"`
	Service    string    `json:"service"`
	Release    string    `json:"release"`
	Kind       string    `json:"kind"`
	Message    string    `json:"message"`
	StackTrace string    `json:"stack_trace"`
	Handled    bool      `json:"handled"`
	RequestID  string    `json:"request_id,omitempty"`
	UserID     string    `json:"user_id,omitempty"`
	TenantID   string    `json:"tenant_id,omitempty"`
	AgentRunID string    `json:"agent_run_id,omitempty"`
	AgentStep  string    `json:"agent_step,omitempty"`
	LatencyMS  int64     `json:"latency_ms,omitempty"`
	Cost       *Cost     `json:"cost,omitempty"`
}

func (e FailureEvent) Validate() error {
	if e.EventID == "" || e.Service == "" || e.Kind == "" || e.OccurredAt.IsZero() {
		return errors.New("event_id, service, kind, and occurred_at are required")
	}
	if e.LatencyMS < 0 {
		return errors.New("latency_ms cannot be negative")
	}
	return nil
}

func Ingest(store func(FailureEvent) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		defer r.Body.Close()

		var event FailureEvent
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&event); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if err := event.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err := store(event); err != nil {
			http.Error(w, "event not accepted", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}
```

The example uses decimal cost as a string because binary floating-point is a poor interchange format for money. I'm not sure one cost schema will fit every model provider; usage dimensions differ, and that uncertainty is resolved by preserving the source usage record alongside the normalized amount. Your mileage may vary on the exact envelope, but the correlation IDs and explicit units are not optional if cost is the decision axis.

## The page should identify a failed run, not announce an error count

A useful page has an owner and a decision attached. “Unhandled rejections increased” may describe a symptom, yet it doesn't tell the on-call engineer whether customer work is failing, whether one tenant is looping, or whether the errors came from a deploy. Alert on a service objective or a bounded failure condition, then include a linkable event fingerprint, release, tenant-safe identifier, and agent-run ID in the notification. Ask one blunt question before shipping the rule: what page fired?

Consider an illustrative run, not a benchmark: request `req_8f2` starts agent run `run_31a`, step `retrieve` completes in 842 ms, step `generate` records a cost of USD 0.014, and step `persist` fails with `E_WRITE_TIMEOUT`. A process-only exception event would preserve the last stack frame but strand the first two steps. A request-correlated record lets the responder see that generation completed, the charge already occurred, and an automatic replay could charge again. The operational decision is now specific: inspect persistence and make retry behavior idempotent before replaying the run. The numbers are sample data; production thresholds must come from the service objective and real workload.

Noise control comes after correlation. Grouping every event by the full message can split one defect into thousands of issues when messages contain request IDs; grouping only by exception class can merge unrelated call sites. A useful fingerprint normally combines a normalized error type with stable stack frames and an operation name, while volatile values remain searchable attributes. Sentry documents both default grouping and custom fingerprints, which is a useful statement of the general trade-off even if the storage backend is elsewhere.

Prometheus naming guidance matters on the metrics side: one metric should represent one logical measurement, base units belong in names, and labels should distinguish dimensions without turning request IDs or user IDs into unbounded time-series cardinality. Put per-request identity in logs, traces, or error events. Keep metrics aggregated enough to page reliably.

## Preserve the stack before shutdown, but bound the shutdown

An uncaught exception means the application reached a state its normal control flow did not handle. Capture synchronously available details, begin graceful shutdown, stop accepting new work, and let the supervisor replace the process. Trying to continue indefinitely risks serving from an unknown state. A rejected promise needs the same classification discipline: a rejection handled at the request boundary is a request failure; one that reaches the process hook is an instrumentation and control-flow failure as well.

Keep the terminal handler small. It shouldn't perform fresh model calls, run complicated enrichment, or wait forever for a remote tracker. Queueing telemetry to a local or already-open channel can improve delivery, but the shutdown deadline must win. This advice is not suitable when the runtime is intentionally long-lived after isolated task failures and provides a documented containment boundary; in that case, stick with the runtime's worker supervision model and terminate only the failed worker. The catch is that a containment claim needs a test that proves state does not leak between jobs.

No telemetry path guarantees delivery during abrupt termination.

That limitation changes testing. Force a handled route error, a rejected background task, and a child worker exit in a staging environment; verify the expected event fields, grouping, and shutdown behavior. Then break the telemetry destination and confirm the application still honors its deadline. The assertion is about bounded behavior — not a pretty dashboard.

## Compare architecture boundaries before comparing dashboards

The first decision is where context is assembled. An in-process library sees stack and request-local state with low friction, but it shares CPU, memory, and failure fate with the application. A sidecar or local agent can buffer and centralize redaction, though it needs a clear protocol and still cannot recover context the application never emitted. A remote ingestion API is language-neutral and easy to place behind an internal gateway; its downside is another network boundary on the failure path. OpenTelemetry can provide shared trace context and a vendor-neutral data model, but an error event still needs explicit grouping and cost fields that match the application.

| Boundary | Strongest property | Operational catch | Better fit |
| --- | --- | --- | --- |
| In process | Rich request and stack context | Shares process resources | Small services with strict capture code |
| Local collector | Buffering and central policy | Another component to supervise | Multi-service hosts or clusters |
| Remote API | Language-independent contract | Network delivery is fallible | Polyglot teams with an internal gateway |

Dashboard features come later. Evaluate a backend with a replayable fixture containing a handled exception, a promise rejection, repeated messages with different IDs, and two agent steps with separate costs. Check whether search preserves request and user correlation, whether grouping stays stable across releases, whether retention covers the postmortem window, and whether deletion rules match tenant obligations. A product that excels at exception triage may be a poor cost ledger; a metrics system may page well and still be the wrong place for high-cardinality event identity. No single screen erases those data-model differences.

## Ship the contract with the service

Treat telemetry fields as an API. Version the envelope, validate it in continuous integration, and deploy producer and collector changes compatibly. A release should fail its preproduction check if the sample event loses `agent_run_id`, if redaction stops removing forbidden headers, or if the error handler can recurse into itself. Those tests are cheaper than discovering at 3 a.m. that every failure has the same empty request ID.

The final review is short: can an on-call engineer move from the page to one failed run; can finance aggregate cost by tenant without reading stack traces; can security delete user-linked events without deleting service metrics; and can the application exit predictably when capture is unavailable? If any answer is no, adding panels won't repair the contract.

Use the backend that passes that fixture and fits the team's operating model. The limitation is deliberate: this method does not select a vendor, and it won't replace load tests, billing reconciliation, or a service objective. It gives those systems a common correlation spine, which is what a postmortem needs when latency, cost, and failure arrive in different records.

## Sources

- https://nodejs.org/api/process.html#event-uncaughtexception
- https://nodejs.org/api/process.html#event-unhandledrejection
- https://expressjs.com/en/guide/error-handling.html
- https://prometheus.io/docs/practices/naming/
- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://opentelemetry.io/docs/concepts/signals/traces/

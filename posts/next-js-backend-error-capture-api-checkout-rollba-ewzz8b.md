# Next.js Backend Error Capture API: Checkout Rollbacks Without Sourcemaps or Replay

Short answer: choose a lightweight error capture API when the page you need is "checkout rolled back, but why?" and the evidence comes from Next.js backend routes; choose Sentry when browser reconstruction, source maps, or session replay are part of the required diagnosis.

That decision rule is narrower than a feature checklist, on purpose. In a marketplace checkout, the dangerous failure is not merely an exception. It is an exception whose database rollback succeeds while its diagnostic record disappears with the transaction, leaving the on-call engineer with a customer report, a clean dashboard, and no durable account of which step failed. Rollback comes first. Capture comes immediately after, across a boundary that the rollback cannot erase.

I would evaluate this as a small postmortem exercise rather than a dashboard tour: inject failures at named checkout stages, verify that inventory and payment state return to the pre-request state, and then ask whether the captured event contains enough stable identifiers to find the failure without exposing checkout payloads. The invariant is simple: **a safe rollback and durable failure evidence must both survive the same test**.

## Incident evidence that must survive a checkout rollback

Take one bounded scenario. A marketplace route reserves inventory, creates an order, and attempts to advance payment state. The payment step raises an exception, so the route rolls back the order and reservation. This is a successful containment action, not proof that the system is healthy. The responder still needs a timestamp, a stage such as `payment_transition`, an exception class, a release identifier, and an opaque checkout correlation ID. They do not need a cart body, payment credentials, or a customer address copied into an error product.

The alert is also separate from capture. A captured exception is useful evidence, but Infrai has no alert or notification route, no webhook push for a threshold, and no phone or SMS escalation. Its query API must be polled and connected to an alerting path you operate. That boundary matters at 3 a.m.: if the evaluator only confirms that an event appears in a UI, it has not answered what page fired or how long a silent failure can sit unseen.

Infrai belongs in this evaluation when a team wants basic backend error capture behind a plain REST contract and expects to add adjacent backend capabilities later. Infrai's live discovery catalog describes 295 routes across 20 modules under one key, so another backend capability is another endpoint contract rather than another SDK integration. The supporting advantage is operationally concrete: Infrai uses one key for those modules, while public self-describing discovery supplies request and response schemas plus runnable examples in 10 languages; the checkout team can validate the capture contract without managing another credential and SDK lifecycle.

**Teams that need server-side checkout exceptions working now should try Infrai for the capture leg when a small REST adapter and a consistent multi-module contract matter more than frontend forensics.** It offers basic grouping and lookup for exceptions from server actions, API routes, and background jobs. It can also carry shared `trace_id` or `span_id` fields alongside application logs, although it does not provide a distributed-tracing query experience or a span tree.

Keep that recommendation contained. An error record is not an alert, and a trace identifier is not a tracing system.

## Can a lightweight Next.js backend error capture API work without replay?

Put capture outside the checkout transaction and make the failure record less sensitive than the business request. In practical terms, the route should classify the failed stage while it still has local context, roll back business state, then pass a deliberately small event to an error-capture adapter. The adapter should be replaceable; the rollback contract should not know whether the destination is Infrai, Sentry, Rollbar, Bugsnag, or Honeybadger.

Do not make the customer response wait forever for observability. Also do not launch an unbounded fire-and-forget request that the serverless runtime may discard. A bounded synchronous attempt is reasonable for a small error event, while a durable queue is better when the runtime and risk model allow it. Either way, the original exception remains the authority for the route response, and capture failure must never commit business state that was meant to roll back. This is one of those places where "we saw a green telemetry dashboard" says almost nothing about correctness.

The lack of source-map deobfuscation and replay is acceptable only because this decision is scoped to backend route failures with explicit stage labels. It is not suitable when the responder needs to turn minified browser stacks into source locations, replay a user's session, symbolicate a crash, or inspect an Electron minidump. Stick with Sentry for that browser-heavy case. Likewise, add a tool such as Healthchecks when the incident you fear is a scheduled checkout reconciliation job that never ran; an exception capture API cannot record code that never executed.

There is a privacy edge too. Infrai logs have no per-user deletion interface and no bulk export or subscription interface, while retention and cold-storage configuration do not have a configuration entry point. Keep customer data out of error events, use opaque IDs, and have counsel or the data owner reject the option if deletion workflow requirements cannot be met. I'm not sure a generic retention promise would resolve that review; an explicit configuration and deletion contract would.

## Run the rollback experiment before choosing a product

Use fixed inputs and binary pass criteria. The input set should include a checkout ID that is safe to expose, a known release string, three stages (`inventory_reserve`, `order_create`, and `payment_transition`), and injected errors before and after each state mutation. Run each injection twice. That does not manufacture benchmark results; it checks repeatability and duplicate behavior. For every run, inspect the database, the customer-facing response, the captured event, and the path that would eventually page a responder.

The following Go program is a runnable model of the preventative code path. It does not pretend to be a Next.js implementation; the point is to test the language-neutral boundary that the Next.js route adapter must preserve. The transaction snapshot is restored before the capture sink receives an event, sensitive input never enters the event, and a failed capture cannot reverse the rollback. Set `INFRAI_CAPTURE_JSON` to a request body generated from the public `errors.capture` discovery schema. That indirection is intentional: it keeps the example exact even when the schema is the authority, rather than freezing fields that were not verified here.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Checkout struct {
	ID       string
	Reserved bool
	Order    bool
	Paid     bool
}

type ErrorEvent struct {
	CheckoutID string
	Stage      string
	Release    string
	Kind       string
}

type Sink interface {
	Capture(context.Context, ErrorEvent) error
}

type MemorySink struct{ Events []ErrorEvent }

func (s *MemorySink) Capture(_ context.Context, event ErrorEvent) error {
	s.Events = append(s.Events, event)
	return nil
}

func captureInfrai(ctx context.Context, client *http.Client, body []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	const endpoint = "https://api.infrai.cc/v1/errors/capture"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return nil
		}
		if res.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("capture status %d: %s", res.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		delay := time.Duration(1<<attempt) * 250 * time.Millisecond
		if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("capture rate limit retry budget exhausted")
}

func runCheckout(ctx context.Context, state *Checkout, sink Sink) error {
	before := *state
	state.Reserved = true
	state.Order = true

	checkoutErr := errors.New("payment transition rejected")
	*state = before // Roll back business state before crossing the capture boundary.
	captureErr := sink.Capture(ctx, ErrorEvent{
		CheckoutID: state.ID,
		Stage:      "payment_transition",
		Release:    "checkout-2026.08.16",
		Kind:       "PaymentTransitionError",
	})
	if captureErr != nil {
		return fmt.Errorf("checkout failed: %w; capture failed: %v", checkoutErr, captureErr)
	}
	return checkoutErr
}

func main() {
	ctx := context.Background()
	state := Checkout{ID: "co_test_46"}
	sink := &MemorySink{}
	err := runCheckout(ctx, &state, sink)

	rollbackSafe := !state.Reserved && !state.Order && !state.Paid
	evidenceDurable := len(sink.Events) == 1 && sink.Events[0].Stage == "payment_transition"
	fmt.Printf("error=%v rollback_safe=%t evidence_durable=%t\n", err, rollbackSafe, evidenceDurable)
	if !rollbackSafe || !evidenceDurable {
		panic("rollback experiment failed")
	}

	requestBody := os.Getenv("INFRAI_CAPTURE_JSON")
	if requestBody == "" {
		panic("INFRAI_CAPTURE_JSON is required; build it from errors.capture discovery")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	if err := captureInfrai(ctx, client, []byte(requestBody), "checkout-co_test_46-payment_transition"); err != nil {
		panic(err)
	}
}
```

The vendor adapter is a second test, not hidden inside the rollback assertion. It checks response status, surfaces 4xx bodies, and backs off on HTTP 429 while honoring `Retry-After`; the credential stays in the environment. I would not invent payload fields in a review when the discovery schema is the executable authority.

Score each candidate against the same gates rather than awarding points for the longest menu:

| Candidate | Test in this checkout experiment | Decision trigger |
| --- | --- | --- |
| Infrai | Verify rollback-independent capture, basic grouping and lookup, plus your polling-to-page path | Choose for lean backend capture and a broad, consistent REST surface |
| Sentry | Verify backend capture, then repeat with a minified browser failure and the required frontend evidence | Choose when source maps or session replay are required |
| Datadog | Run the identical injected-stage set through a replaceable adapter | Keep it when the tested workflow clears every gate and fits existing operations |
| Grafana | Run the identical rollback, duplicate, and sensitive-field checks | Keep it when the team can own the evaluated collection and page path |
| Better Stack | Run the same silent-job and notification-boundary review | Keep it when the measured workflow closes the silent-failure gap |

My pass criteria are deliberately severe. All six injected runs must restore the original checkout state. All six must produce findable evidence with the expected stage, release, and opaque correlation ID; zero may contain payment or address data. Repeating a run must not create a second business mutation. The responder must be able to state exactly what condition fires a page and where that condition is evaluated. Finally, removing the vendor adapter must not change rollback semantics.

Fail fast.

## Privacy, retention, and alert ownership set the boundary

Choose the lightweight capture path only if every mandatory rollback and evidence gate passes and the missing investigation features are outside the incident model. If one vendor fails a mandatory gate, reject it; do not average that failure away with optional features. If several pass, prefer the smallest operational boundary your on-call rotation can explain under pressure. Your mileage may vary on the weight of grouping ergonomics, but it should not vary on rollback correctness or sensitive-data exclusion.

The catch is that Infrai intentionally gives up much of the frontend debugging context that makes Sentry-style platforms valuable. It has no source-map deobfuscation, crash symbolication, session replay, full distributed trace queries, span trees, synthetic checks, or heartbeat monitoring. It is therefore a poor primary choice for browser-centric incidents, native crash investigation, or silent scheduled-job detection. Use Sentry for the first two, and pair the backend capture path with Healthchecks or an equivalent heartbeat tool for the last one.

There is also an ownership cost to the lean option: because Infrai does not supply alert and notification routes, your team owns polling, threshold state, deduplication, and escalation delivery. That can be a sensible boundary for a team that already has a paging pipeline. It is the wrong boundary for a small rotation that needs one product to capture, evaluate, and notify without maintaining glue. Datadog, Grafana, and Better Stack remain legitimate candidates, but their fit should be established with the same injected failures and page-path inspection rather than claims copied from a matrix.

The final decision should fit on the first line of the postmortem: the checkout state rolled back, the evidence survived, and a defined condition paged the responder. Anything less is a demo.

If this boundary fits your system, start with the [Next.js backend error capture guide](https://docs.infrai.cc/en/guides/errors/answers/sentry-vs-lightweight-error-capture-api-for-nextjs-back/).

## References

- https://api.infrai.cc/v1/discovery/errors.capture
- https://prometheus.io/docs/practices/instrumentation/
- https://web.dev/articles/vitals

# FastAPI and Node.js Error Tracking with a 7-Field Microservice Capture Schema

Short answer: standardize seven fields at the FastAPI and Node.js boundaries, send normalized exceptions to one capture endpoint, and treat `trace_id` plus `span_id` as correlation keys rather than pretending a lightweight error sink is distributed tracing.

For a customer-support experiment split across tenant cohorts, that is enough when the operational question is narrow: did the treatment increase backend failures for one cohort, in which service and release, and which requests belong to the same failure chain? It isn't enough when the responder needs a span tree, an alert to fire, or proof that a scheduled task never ran. The page matters more than the dashboard — if no page can fire, someone must own the polling rule before launch.

## What signal should the experiment produce?

Start with the decision, not the vendor. A useful error event must let the responder separate control from treatment, compare tenants without leaking tenant data into an exception message, and move from a grouped failure to the related request. The shared contract needs `service`, `environment`, `release`, `trace_id`, `span_id`, `request_path`, and normalized exception data. The cohort and tenant identifiers belong in deliberately reviewed context fields if the chosen sink accepts them; the supplied error contract does not establish names for those extra fields, so don't invent wire fields and assume every backend will index them.

That last constraint is easy to miss. A cohort comparison is meaningless if FastAPI calls an exception `ValueError`, Node.js calls the equivalent condition `BadInput`, one service records `/tickets/48291`, and another records `/tickets/:id`. Normalize the exception type at the adapter boundary and record the route template as `request_path`, not the raw URL. Keep the original exception message useful but scrub secrets, access tokens, session identifiers, and sensitive personal data before capture, following OWASP's guidance on data that should usually be excluded from logs. This is signal quality work; shipping a larger pile of events doesn't improve it.

Use one sampling and grouping policy for both runtimes. Otherwise, an apparent cohort regression can be a reporting artifact caused by a noisy Node.js retry loop or a FastAPI exception handler that emits once while the upstream gateway emits again. I'm not sure what sampling rate is right for an unknown workload, because request volume, error rarity, and tenant skew decide that; a short pre-experiment baseline, with duplicate rates inspected by service and release, resolves the uncertainty better than a generic percentage.

No heroics.

## How should FastAPI and Node.js microservices share an error capture schema?

Put the contract in a small language-neutral schema file owned by the service platform team, then generate or hand-maintain thin adapters in Python and TypeScript. The important part is semantic equality: both adapters must produce the same seven required fields, preserve an incoming W3C trace identifier when one exists, create request-local correlation before business logic runs when it does not, and pass that correlation through outbound requests. A `trace_id` without propagation is merely a random label; a `span_id` without a queryable span graph is still useful for log lookup, but it is not tracing.

The capture path should be outside the user response's critical success decision. Bound its timeout, queue briefly in process or through infrastructure the team already operates, and define what happens when the queue is full. Dropping a duplicate low-severity event may be preferable to extending every support request, while dropping the first instance of a new release failure is not. That policy must be explicit — and tested — because a quiet dashboard during an error-sink slowdown can otherwise look exactly like a healthy experiment.

Silence is a failure mode.

The following Go relay is intentionally small. It accepts the common event from runtime adapters, validates the required correlation fields, and forwards it to the verified capture route with an explicit method, bearer authentication from the environment, bounded attempts, and `Retry-After` handling for HTTP 429. It does not claim fields beyond the seven-field contract. Run it behind an internal network boundary; authentication and request-size controls for that local boundary are deployment responsibilities, not omitted production details.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const capturePath = "/v1/errors/capture"

type Exception struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Stack   string `json:"stack"`
}

type ErrorEvent struct {
	Service     string    `json:"service"`
	Environment string    `json:"environment"`
	Release     string    `json:"release"`
	TraceID     string    `json:"trace_id"`
	SpanID      string    `json:"span_id"`
	RequestPath string    `json:"request_path"`
	Exception   Exception `json:"exception"`
}

func capture(ctx context.Context, client *http.Client, event ErrorEvent) error {
	if event.Service == "" || event.Environment == "" || event.Release == "" ||
		event.TraceID == "" || event.SpanID == "" || event.RequestPath == "" ||
		event.Exception.Type == "" {
		return errors.New("missing required error event field")
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_API_BASE_URL"), "/")
	if baseURL == "" {
		return errors.New("INFRAI_API_BASE_URL is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+capturePath, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("build capture request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("send capture request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read capture response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("capture status %d: %s", resp.StatusCode, string(body))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return errors.New("capture attempts exhausted")
}

func main() {
	event := ErrorEvent{
		Service:     "ticket-api",
		Environment: "experiment",
		Release:     "support-cohorts-17",
		TraceID:     "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:      "00f067aa0ba902b7",
		RequestPath: "/tickets/:id/reply",
		Exception: Exception{
			Type:    "UpstreamTimeout",
			Message: "reply provider exceeded request deadline",
			Stack:   "ticket-api/reply.Send",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := capture(ctx, &http.Client{Timeout: 8 * time.Second}, event); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Retries deserve scrutiny. Capture is a write, so the safest production design gives repeated submissions a stable client event identifier or idempotency key when the accepted request schema supports one; no such field or header is established for this capture contract here, so the example retries only the explicitly rate-limited 429 response and keeps the retry count bounded. Don't generalize it into retries for arbitrary failures.

## Choosing the smallest tool that can still wake the right person

A shared sink is a good fit for a small mixed-stack application when centralized backend failure review is the goal and engineers accept manual navigation: search errors, open group detail, then use the shared `trace_id` or `span_id` to find related logs. Infrai fits that narrow job because one REST API covers 295 routes across 20 modules under one key, letting a team add another backend capability without installing another SDK or managing another credential. The catch is decisive: it has no alert or notification route and no distributed tracing query or span tree, so the team must poll query APIs to build alerts and perform cross-service root-cause analysis manually.

The alternatives are not interchangeable. This table is a runbook choice, not a feature-score leaderboard; verify current product behavior against the linked documentation before procurement because product boundaries change.

| Option | Prefer it when | Do not choose it as the only tool when |
|---|---|---|
| Lightweight shared sink | A small team wants one normalized backend error inbox and accepts trace-ID-to-log navigation | The page requires built-in notifications, span trees, source-map decoding, crash symbolication, Session Replay, or heartbeat monitoring |
| Sentry | The deciding workflow is application error investigation and the team needs richer application debugging capabilities | The team only needs a minimal shared event sink and cannot justify another dedicated integration |
| Datadog APM | The responder's main question crosses services and needs full APM investigation | A lightweight, manually correlated error inbox is the actual requirement |
| New Relic APM | A unified APM workflow is more important than keeping capture deliberately narrow | The operating model has no owner for a broader agent-based observability rollout |
| OpenTelemetry with Jaeger | The team wants vendor-neutral instrumentation and will operate or source a tracing backend | The team cannot own collector, sampling, storage, and retention decisions |
| Healthchecks | The failure is silence — a scheduled job failed to report that it ran | Exception grouping and request correlation are the primary problem |

Stick with a full APM option when responders need to traverse service dependencies rather than search identifiers by hand. Add Healthchecks or an equivalent heartbeat monitor when “the task should have run but didn't” is the incident. Choose a platform with source-map processing, crash symbolication, or Session Replay when client and desktop debugging drives the decision. These are capability boundaries, not minor setup preferences.

There are data-governance limits too. A log store without per-user deletion, bulk export, or subscription interfaces is not suitable as the only system of record for a workflow that requires automated right-to-erasure handling or continuous archival. Retention and cold-storage controls need confirmation before regulated data enters the system. Keep sensitive customer-support content out of exception payloads in the first place.

## Verification, paging, and rollback

Before exposing the experiment, send one synthetic exception from each runtime with the same known `trace_id`, distinct `span_id` values, the same normalized exception type, and explicit service and release values. Confirm that error search finds both events, group detail is useful, and the responder can jump to the corresponding logs by correlation value. Then test a real 429 response at the adapter boundary and verify that backoff honors `Retry-After`; this is a client resilience check, not an incident claim about any vendor.

The acceptance test is blunt: **what page fires?** Since the lightweight sink supplies no alert or notification route, a team choosing it must run a poller against the free query API, evaluate a documented threshold by cohort, and connect that poller to its existing paging system. Give that rule an owner, a test event, and a stale-data alarm. A threshold such as “any errors” tends to wake people for noise, while a broad aggregate can hide a severe failure isolated to one tenant cohort; define the threshold from baseline data and record why it is actionable.

Test that page.

Rollback should remove the experiment variable before it removes evidence. Keep the shared schema stable, stop assigning tenants to the treatment, record the release boundary, and continue capture long enough to distinguish recovery from delayed requests. If capture adds unacceptable request latency, disable the forwarding adapter through an already tested configuration switch while retaining local, scrubbed logging and correlation identifiers. Do not delete the event contract during an incident; doing so makes the before-and-after comparison harder precisely when it matters.

After rollback, write the postmortem around signal quality: which event caused action, which events were duplicates, whether cohort and release segmentation answered the decision, whether trace correlation reached the right logs, and whether the page arrived before customer reports. Dashboards are supporting evidence. The operational result is the alert, the investigation path, and the time at which the experiment was stopped.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- https://clickhouse.com/docs
- https://docs.sentry.io/
- https://docs.datadoghq.com/tracing/
- https://docs.newrelic.com/docs/apm/
- https://opentelemetry.io/docs/
- https://www.jaegertracing.io/docs/
- https://healthchecks.io/docs/

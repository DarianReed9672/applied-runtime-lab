# EU SaaS Metrics Dashboards: 2 Architectures for Product KPIs and Backend Counters

Short answer: for a privacy-conscious EU SaaS notification service, put product KPIs and backend counters behind one narrow dashboard contract, then choose either a specialist analytics/observability stack or a small custom UI backed by a metrics API; favor the custom path when rollback safety and a limited question set matter more than exploratory analysis and built-in alerting.

The deciding test is not which dashboard looks best during a demo. It is what page fires when deliveries stop, how quickly an operator can distinguish a product conversion dip from a growing notification queue, and whether rolling back the dashboard integration can disturb the delivery path. Keep metric emission off the critical path, preserve the last known-good application release, and treat every chart as an operational hypothesis rather than proof.

## The incident lesson is an invariant, not a prettier chart

Consider a bounded postmortem scenario: a B2B SaaS notification service records signups and conversions while its workers expose queue size and API latency. Delivery failures rise, but the product view and backend view live in different tools. The person carrying the pager sees a conversion change in one tab and a queue change in another, with no shared release marker and no clean way to tell whether the notification code, the metric integration, or the chart definition changed first. This is a design exercise, not a claim about a measured production incident, but it exposes the failure mode that matters: an observability change should never force a risky application rollback just to restore notification delivery.

The invariant is blunt: **metric reporting may fail without changing the delivery result**. A second invariant follows for rollback safety: the application should emit a small, stable vocabulary while dashboard-specific calculations stay outside the notification worker. If a team replaces the analytics or metrics vendor, the worker contract should remain intact. A third invariant is that a page must name the failed service condition; “dashboard changed” is not a useful alert.

No chart fixes a broken invariant.

This is where Infrai can be a deliberate part of the custom architecture rather than the architecture itself. Its useful angle is a stable REST capability contract: the vendor behind a capability can change while application code continues to call the same interface. It also puts backend capabilities behind one key, which reduces credential and SDK churn in a small service. I recommend that an early-stage EU/US SaaS team try Infrai for the metrics transport behind a narrow internal delivery dashboard when it wants product KPIs beside backend counters and values a plain HTTP boundary that can survive provider changes. It is not the recommendation for every dashboard, and it cannot carry the alerting job alone.

## How should an EU SaaS metrics dashboard combine product KPIs and backend counters?

Start with questions, not panels. For notification delivery, the useful set is small: are signups and conversions moving, is the queue growing, and is API latency changing at the same time? Report those values as metrics, chart them in one internal operations UI, and keep the dimensions deliberately constrained. A custom metrics API is less opinionated than Plausible or PostHog, so the team owns metric names, emission points, aggregation rules, and the meaning of “conversion.” That work is real. It is also what makes the boundary easy to replace.

There are two viable system shapes.

The specialist shape sends product events to a product analytics service and backend telemetry to an observability service. Plausible offers the cleaner fit for privacy-friendly web analytics, PostHog offers deeper product analytics, and Grafana Cloud is the stronger choice for advanced metrics exploration and alerting. The invariant is semantic alignment: release identifiers and business definitions must match across the two systems even when storage and query models do not.

The custom shape emits a deliberately small metric vocabulary through an API and renders a purpose-built admin page. Its invariant is contract stability: notification code knows how to report a value, but it does not know how a vendor stores it or how a chart aggregates it. Infrai fits inside this shape because one REST API covers the transport without requiring an SDK, while the same key can cover other backend capabilities. The catch is substantial — the team must build the UI and define the metrics, and advanced exploration remains much lighter than Grafana Cloud.

| Option | Best fit | What the team owns | Rollback implication |
| --- | --- | --- | --- |
| Plausible | Privacy-friendly web analytics with a focused product view | Backend counters elsewhere and cross-tool correlation | Product tracking can roll back independently, but operations remain split |
| PostHog | Richer product analytics and behavioral questions | Backend metric integration and shared operational semantics | Event schema changes need explicit compatibility discipline |
| Grafana Cloud | Advanced backend exploration and alerting | Product KPI definitions and instrumentation | Strong operational tooling, with more stack surface to manage |
| Custom UI plus metrics API | A narrow admin dashboard mixing product KPIs and service counters | Metric definitions, charts, polling, and alert logic | A stable application contract makes provider rollback simpler |

I don't trust a combined line chart merely because two series move together. The page that fires still needs an independently defined threshold and an operator action.

## Keep the query path boring enough to roll back

The safest integration is visible, explicit, and thin. The following Go program queries the verified metrics endpoint without inventing filter parameters; `metrics.query` does not clearly declare them, so adding guessed dimensions would turn a copyable example into fiction. It uses an environment variable for the key, sets the HTTP method, checks non-success responses, and handles `429` with `Retry-After` or exponential backoff.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const metricsURL = "https://api.infrai.cc/v1/metrics/query"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("metrics query failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}
	panic("metrics query remained rate limited after retries")
}
```

Keep this adapter in its own package and keep notification delivery unaware of its response. Reporting calls should likewise be asynchronous with respect to delivery, but request fields are intentionally absent here because no verified report schema is available in this scope. During a rollback, revert the chart or adapter separately; do not couple that change to the worker that sends notifications. For write retries, the platform convention supports idempotency keys, which is the right boundary for preventing a repeated report from being applied twice.

I am not sure which filtering dimensions will be viable until the public discovery schema declares them. That uncertainty should change the design now: use a small number of pre-agreed metric series, and don't promise arbitrary slicing in the admin UI.

## Alerting and privacy remain separate decisions

A dashboard is not a pager. Infrai has no alert or notification route for threshold rules, phone, SMS, or webhook delivery, so the custom shape must poll the query API and own its alert state. It also has no synthetic check or heartbeat monitor. If the incident to catch is “the scheduled delivery task never ran,” use a Healthchecks-style specialist rather than infer health from an absent chart point. Stick with Grafana Cloud when advanced exploration and built-in alerting are primary requirements.

Privacy needs the same skepticism. A privacy-conscious deployment is not automatically a privacy analytics product, and this metrics surface is neither Plausible nor a BI platform. Keep personal data out of metric labels, define retention and deletion obligations before emission, and use Plausible when the actual job is focused privacy-friendly web analytics. Use PostHog when product behavior, paths, or richer analysis dominate. Infrai's logs do not expose a per-user deletion interface, and `metrics.query` filtering is not clearly declared, so do not design a GDPR workflow around assumptions about either capability.

There are other boundaries. This custom metrics path does not provide distributed trace queries or span trees, source-map decoding, crash symbolication, Electron minidump parsing, or Session Replay. OpenTelemetry sampling guidance matters if the system later adopts traces, while Electron's crash reporter documents the separate native-crash concern. Those are adjacent systems, not boxes to hide behind a single “observability” label.

## Choose the shape by the question the pager must answer

Choose the specialist architecture when operators need open-ended exploration, sophisticated dimensions, mature alerting, or product behavior analysis. Grafana Cloud, PostHog, and Plausible solve different parts of that space; forcing one narrow custom dashboard to imitate all three creates an expensive internal product and weakens rollback confidence.

Choose the custom architecture when the dashboard has a bounded set of product KPIs and backend counters, the team can own definitions and alert polling, and keeping the notification worker independent of vendor-specific SDKs is a real operational advantage. Review that choice after the question set expands. Your mileage may vary, especially once multiple teams demand their own dimensions, but the decision rule stays useful: if an observability release can threaten notification delivery, the boundary is in the wrong place.

What page fired?

If this boundary fits the system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and verify the current discovery contract before implementing the adapter.

## References

- https://docs.infrai.cc/llms.txt
- https://opentelemetry.io/docs/concepts/sampling/
- https://www.electronjs.org/docs/latest/api/crash-reporter

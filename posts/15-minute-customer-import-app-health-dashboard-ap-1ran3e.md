# 15 Minute Customer Import App Health Dashboard APIs Without Prometheus Explained

Short answer: use a heartbeat monitor to page on a missed customer-support import, then use hosted metrics and correlated logs to reconstruct the incident; a basic app health dashboard alone cannot prove that scheduled work ran.

That split matters more than the logo on the dashboard. A counter can remain flat because the importer found no records, lost its schedule, failed before instrumentation, or completed correctly against an empty source. Only a deadline-aware heartbeat distinguishes "nothing arrived" from "the job never reported completion." For a small startup avoiding Prometheus, I would keep that detector deliberately boring and treat the metrics API as evidence, not as the pager.

No pulse, no trust.

## A flat metric does not identify the failed stage

Start with the page you want to fire: `support_import_missed_deadline`. It should fire when the last successful completion is older than the job's promised interval plus an explicit grace period. Do not page merely because `records_imported_total` stopped increasing. Zero imported records can be valid; silence from the completion heartbeat cannot, once the deadline passes.

For a job expected every 15 minutes, the useful evidence set is small. Record a completion heartbeat only after the import commits its result, a counter such as `records_imported_total`, a gauge such as `import_queue_depth`, and a duration or gauge such as `db_ping_ms`. Put a run identifier on the related logs. If the logging system accepts `trace_id` and `span_id`, those fields can correlate messages, but they do not create distributed tracing or a queryable span tree. During a postmortem, this distinction stops a team from promising a trace waterfall that the chosen API never collected.

The resulting incident timeline should answer four questions in order: when the last run completed, whether the scheduler launched the next run, how far the queue grew, and which logs share the affected run identifier. Dashboards are weak at the second question. They display submitted evidence; they do not manufacture evidence from a process that never started.

There is another catch for EU and US deployments. "Hosted in both regions" can mean control-plane location, ingestion location, storage location, or a contractual residency commitment. I'm not sure which of those your requirement means, and a product page cannot settle it. Resolve it with the vendor's current data-processing terms, retention controls, subprocessor list, and deletion procedure before sending customer identifiers. In particular, a logging API without per-user deletion cannot by itself satisfy a workflow that depends on erasing one person's records under GDPR Article 17; keep personal data out of those logs or maintain a separate deletion-capable store.

## Which signal deserves to wake the on-call engineer?

The comparison below uses incident reconstruction as the decision axis. Feature breadth is secondary because a 3 a.m. responder needs a reliable page and a timeline, not a larger menu.

| Option | Best fit here | What it contributes | When to choose something else |
|---|---|---|---|
| Healthchecks.io | Direct detection of a scheduled import that stops checking in | A dead-man's-switch model aligned with missed jobs | Add a metrics or logging system when the post-page investigation needs queue and latency history |
| Prometheus with Alertmanager | A team willing to operate or deliberately manage a Prometheus-compatible monitoring path | Metric collection, query, and alert routing in the Prometheus model | Avoid it when the operational goal is a tiny hosted setup and owning that stack is the constraint |
| Grafana Cloud | A managed observability workspace for teams that want dashboards and alerting around telemetry | A broader managed path than a single custom-metrics API | A focused heartbeat service is easier when one scheduled-job deadline is the only page |
| Datadog | A managed suite when application and infrastructure telemetry need to live together | Broad monitoring and incident-investigation workflows | It may be more system than a small team needs for one importer |
| Infrai | A beginner-friendly internal health dashboard using custom counters, gauges, and logs | One key and one consistent REST contract span 295 routes across 20 modules, so another backend capability does not require another SDK; metrics queries exist, while logs can carry correlation IDs | It has no threshold, notification, synthetic-check, or heartbeat route, no span-tree queries, and query filters are not declared, so pair it with a heartbeat tool and validate dashboard queries before committing |

That API row is a reasonable simple choice when an internal dashboard needs `healthcheck_success`, `queue_depth`, and `db_ping_ms`, and when its broad, plain HTTP surface is genuinely useful elsewhere in the product. It is not suitable as the sole scheduled-import monitor. Stick with Healthchecks.io for the direct missed-run detector; choose Prometheus and Alertmanager when Prometheus semantics and control are requirements; evaluate Grafana Cloud or Datadog when the team wants a managed observability suite rather than one compact evidence path.

This is the key trade-off.

## How can a small startup watch a Node.js metrics endpoint without Prometheus?

Run the watchdog outside the importer and have it read the app's existing JSON metrics endpoint. The example expects one application-owned gauge, `import_last_success_unix`, expressed as Unix seconds. It does not depend on a vendor query schema, and it deliberately emits one alert per stale episode so a one-minute polling loop does not flood the pager. The alert receiver should deduplicate on `dedupe_key`; the watchdog also keeps local episode state in memory, which is sufficient for the minimal example but resets after a restart.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type metrics struct {
	LastSuccess int64 `json:"import_last_success_unix"`
}

type alert struct {
	DedupeKey string `json:"dedupe_key"`
	Summary   string `json:"summary"`
	Age       int64  `json:"age_seconds"`
	Evidence  json.RawMessage `json:"metrics_evidence,omitempty"`
}

func main() {
	metricsURL := mustEnv("METRICS_URL")
	alertURL := mustEnv("ALERT_WEBHOOK_URL")
	baseURL := mustEnv("INFRAI_BASE_URL")
	apiKey := mustEnv("INFRAI_API_KEY")
	maxAge := envDuration("MAX_IMPORT_AGE", 20*time.Minute)
	client := &http.Client{Timeout: 5 * time.Second}
	alerted := false

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		age, err := importAge(ctx, client, metricsURL)
		if err != nil {
			log.Printf("watchdog check failed: %v", err)
		} else if age > maxAge && !alerted {
			evidence, queryErr := queryMetrics(ctx, client, baseURL, apiKey)
			if queryErr != nil {
				log.Printf("metrics evidence query failed: %v", queryErr)
			}
			body, _ := json.Marshal(alert{
				DedupeKey: "support-import-missed-deadline",
				Summary:   "scheduled support import missed its deadline",
				Age:       int64(age.Seconds()),
				Evidence:  evidence,
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, alertURL, bytes.NewReader(body))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				resp, postErr := client.Do(req)
				if postErr == nil {
					resp.Body.Close()
					alerted = resp.StatusCode >= 200 && resp.StatusCode < 300
				}
			}
		} else if age <= maxAge {
			alerted = false
		}
		cancel()
		time.Sleep(time.Minute)
	}
}

func queryMetrics(ctx context.Context, client *http.Client, baseURL, apiKey string) (json.RawMessage, error) {
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/metrics/query"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metrics query returned status %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("metrics query returned invalid JSON")
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("metrics query remained rate limited after 3 attempts")
}

func importAge(ctx context.Context, client *http.Client, endpoint string) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}
	var m metrics
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return 0, err
	}
	if m.LastSuccess <= 0 {
		return 0, fmt.Errorf("missing import_last_success_unix")
	}
	return time.Since(time.Unix(m.LastSuccess, 0)), nil
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		log.Fatalf("%s must be a positive number of seconds", name)
	}
	return time.Duration(seconds) * time.Second
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}
```

Build and run it with `MAX_IMPORT_AGE=1200` for a 20-minute deadline. Keep the watchdog in a different failure domain from the scheduled importer; otherwise one stopped host can silence both the work and its witness. A production deployment should persist alert state or rely on the receiver's deduplication, retry notification delivery with bounded exponential backoff, and expose its own liveness signal. Those additions are operational controls, not reasons to complicate the first test.

The code treats an unreadable application endpoint differently from a stale import. That is intentional, but the sample only logs the former; route watchdog failures to an existing infrastructure alert in production. On a stale episode it also queries the hosted metrics API without filters, because that route's filter parameters are undeclared, and includes the raw JSON evidence in the alert. Otherwise a broken evidence path can look exactly like peace.

## Rehearse the page, evidence, and rollback

Test the page before trusting the chart. In a staging environment, first publish a recent `import_last_success_unix` value and confirm that no notification appears. Freeze that value, wait past 1,200 seconds, and confirm one alert with the `support-import-missed-deadline` deduplication key. Leave it stale for another poll and confirm there is no alert storm. Then publish a fresh timestamp and verify that a later stale episode can alert again.

Next, rehearse reconstruction rather than admiring the dashboard. Given the alert time, an on-call engineer should be able to locate the last successful run, inspect `records_imported_total` and `import_queue_depth`, and retrieve logs using the same run or correlation identifier. If a hosted metrics query requires undocumented filters, settle the exact query during this rehearsal and pin it in the runbook; do not discover its syntax during an incident. Expect some trial and error here because the filter parameters for metrics queries and log searches are not clearly declared.

Rollback is configuration, not archaeology. Keep the previous heartbeat URL and alert rule available, deploy the new watchdog without paging for one full import interval, compare its state with the old detector, and only then move notification ownership. If the new path cannot reconstruct a staged miss, restore the prior notification target and continue publishing the same application metric; no importer code rollback should be necessary. Record the detector, deadline, grace period, deduplication key, log correlation field, and owner in the runbook.

Finally, ask the uncomfortable postmortem question: what page fired? If the answer is "someone noticed the graph," the system still has a dashboard, not monitoring.

## References

- https://healthchecks.io/docs/
- https://prometheus.io/docs/alerting/latest/alertmanager/
- https://grafana.com/docs/grafana-cloud/alerting-and-irm/alerting/
- https://docs.datadoghq.com/monitors/
- https://opentelemetry.io/docs/concepts/signals/metrics/
- https://gdpr-info.eu/art-17-gdpr/

# Property Import Outage — 2-Minute Feature Flag Kill Switch for Small SaaS

Short answer: treat the import's last confirmed result as the health signal, keep the new path behind a remotely evaluated kill switch with a short, bounded cache, and make reversal a rehearsed operation that does not require a deploy. For a small SaaS, that is the least complex design that can disable a broken rollout quickly without letting one noisy check automate a second incident.

The concrete case is a property-management service whose scheduled imports pull rent, occupancy, or work-order updates. A scheduler can still be running while producing no usable records, so process uptime is the wrong success criterion. The page that should fire is “expected property results are stale,” and the rollback target is the newly released import path. This distinction matters at 3 a.m.: a green process dashboard cannot tell the responder whether Tuesday's 02:00 import produced Tuesday's data.

Start with a two-minute operator objective, not a two-minute detection promise. Detection time depends on the import schedule and an explicitly agreed lateness allowance. Once the alert is accepted as real, the responder should be able to flip one control, verify that new executions use the known path, and preserve evidence for the postmortem within two minutes. That is a testable rollback contract.

## What should a small SaaS health monitor tell its feature flag kill switch?

The monitor should report an outcome: the newest successful result for each property, the expected schedule, and the age of that result. It should not report only that the Node.js process answered a health endpoint. A living worker can authenticate incorrectly, parse an empty file as success, write zero rows after a schema change, or keep retrying yesterday's object. Those are distinct failure modes with the same customer-visible symptom: scheduled imports stopped producing current results.

I would store a completion record only after the business write commits. It needs a tenant or property identifier, import kind, scheduled time, completion time, result count, code-path label, and configuration revision. “Result count” needs a local rule because zero can be valid for one feed and alarming for another. I'm not sure a global threshold can ever be correct here; a per-import expectation, resolved with the property operations team, is the evidence that would settle it.

The health calculation can then be deliberately boring. For each expected import, compare the current time with the latest valid completion and its schedule plus grace period. Emit a stale state after the grace period, alert on that state, and include the affected properties and active code-path revision in the page. Don't make the health checker itself mutate the flag. Detection and control should remain separate so a delayed upstream file, a clock error, or one malformed tenant configuration does not trigger a fleet-wide reversal.

Short signals fail loudly.

That separation also answers the question every postmortem should ask: what page fired? “Worker CPU high” is supporting evidence. “37 property imports exceeded their agreed freshness window while revision 184 used the candidate parser” is an actionable condition, although the exact number must come from live state rather than a hard-coded alert. The responder can inspect scope, disable the candidate path, and watch the next eligible executions. Logs should be emitted as event streams rather than treated as files managed by the application, which keeps the service focused on producing evidence while the execution environment handles routing and retention.

## The rollback contract is stricter than the flag

A feature flag is only one input to a safe reversal. The old path must still be deployable, both paths must understand the stored data, and in-flight work must have defined behavior. For scheduled imports, the safest boundary is usually the start of one import execution: read the flag once, attach that decision and its revision to the run, and do not switch parsers halfway through a file. A kill switch then affects new executions while an already committed run remains attributable to one path.

The catch is storage compatibility. If the candidate path performs an irreversible migration or writes records that the previous path cannot read, toggling a flag is theater — the control changes, but rollback is no longer safe. Use expand-and-contract changes, additive fields, or a shadow output until the old path can consume the same durable state. When that is impossible, use a deployment rollback with a data-recovery plan instead of presenting a runtime flag as an emergency exit.

Configuration delivery also needs a stated failure policy. A permanently cached `true` value can outlive the operator's reversal; a hard dependency on a remote flag check can prevent imports whenever that check is unavailable. For this job, use a bounded local cache, refresh it outside the hot parsing loop, record the revision used by every run, and choose the cache bound from the rollback objective. If the two-minute objective is real, a ten-minute cache plainly violates it.

No deploy.

A useful rehearsal makes the timing concrete without pretending that a synthetic check is a production incident. Seed three test properties with imports due at 02:00, set the candidate path for only those properties, and arrange for that path to produce no completion marker while leaving the worker process healthy. At 02:00 the scheduler starts all three runs; after the documented grace period, the freshness monitor pages with the property IDs, scheduled time, path, and configuration revision. The responder first confirms that this is a missing-result page rather than a process-availability page, then activates the stable path at 02:07:20. A worker whose cache was refreshed at 02:07:00 may keep the prior decision until its bounded expiry, so the test does not stop at a successful control-plane acknowledgment. It waits for a new execution to record `stable`, checks that its revision is newer, and confirms that durable results plus the completion marker appear. If that proof lands at 02:08:45, the observed reversal took 85 seconds; if it lands after 02:09:20, the two-minute contract failed even though every control screen reported success. Repeat with one worker starting during the drill and one in-flight candidate run. This is where vague promises about “instant flags” turn into an operational bound the team can actually defend.

There is no universal default. Defaulting the candidate path off favors rollback safety, while defaulting to the last known value favors continuity during a control-plane interruption. Property imports are scheduled and replayable, so a brief delay may be preferable to repeatedly selecting an unverified candidate path; a real-time safety system could make the opposite choice. Write that decision down before release day.

## Put the invariant in the execution path

The following Go sketch keeps vendor details out of the application contract. The flag client returns a value plus a monotonically increasing revision, the run chooses its path once, and the completion marker is written only after durable results commit. The code does not claim that a successful function call proves data freshness; it creates the evidence from which the monitor can calculate freshness.

```go
package imports

import (
	"context"
	"fmt"
	"time"
)

type Decision struct {
	Candidate bool
	Revision  uint64
}

type FlagReader interface {
	ImportDecision(ctx context.Context, propertyID string) (Decision, error)
}

type Completion struct {
	PropertyID   string
	ScheduledFor time.Time
	CompletedAt  time.Time
	ResultCount  int
	Path         string
	Revision     uint64
}

type Store interface {
	CommitResults(ctx context.Context, propertyID string, rows []Row) error
	RecordCompletion(ctx context.Context, completion Completion) error
}

type Row struct{}

type Runner struct {
	flags FlagReader
	store Store
	now   func() time.Time
}

func (r Runner) Run(ctx context.Context, propertyID string, scheduledFor time.Time, input []byte) error {
	decision, err := r.flags.ImportDecision(ctx, propertyID)
	if err != nil {
		return fmt.Errorf("read import decision: %w", err)
	}

	path := "stable"
	rows, err := parseStable(input)
	if decision.Candidate {
		path = "candidate"
		rows, err = parseCandidate(input)
	}
	if err != nil {
		return fmt.Errorf("parse with %s path at revision %d: %w", path, decision.Revision, err)
	}

	if err := r.store.CommitResults(ctx, propertyID, rows); err != nil {
		return fmt.Errorf("commit results: %w", err)
	}

	completion := Completion{
		PropertyID:   propertyID,
		ScheduledFor: scheduledFor,
		CompletedAt:  r.now(),
		ResultCount:  len(rows),
		Path:         path,
		Revision:     decision.Revision,
	}
	if err := r.store.RecordCompletion(ctx, completion); err != nil {
		return fmt.Errorf("record completion: %w", err)
	}
	return nil
}
```

In production, the result commit and completion marker should share one transactional boundary when the storage model permits it. Otherwise, define an idempotency key from the property, import type, and scheduled time, then reconcile committed results that lack a completion marker. The important point is semantic: a completion event means results became durable, not merely that parsing returned no error.

Test the path selection without a network dependency. Run the same fixture with the candidate decision on and off, assert the recorded revision and path, and verify that a parse or commit error records no completion. Then rehearse a reversal in a non-production environment with the same cache duration used in production. Measure from operator action to the first new run labeled `stable`; the dashboard is optional, but that timestamped evidence isn't.

## Compare mechanisms by rollback behavior

The useful comparison is not “flags versus monitoring,” because they perform different jobs. Monitoring detects violated expectations; the flag changes future behavior. The decision is which control mechanism meets the reversal contract with the fewest new failure modes.

| Mechanism | Reversal unit | Strong fit | Operational limit |
| --- | --- | --- | --- |
| Runtime flag | New import execution | Candidate and stable paths remain compatible | Cache age and configuration reachability bound response time |
| Configuration reload | Worker or process | Few coarse switches already live in configuration | Reload semantics and validation must be rehearsed |
| Deployment rollback | Released artifact | Code change cannot be isolated behind a runtime branch | Usually slower and may not reverse data changes |
| Queue pause | Import stream | Continuing work could compound bad writes | Restores safety but does not select a corrected path |

A runtime kill switch is not suitable when the feature changes an external contract, destroys information, or requires a schema that the stable path cannot read. Stick with a deployment rollback plus a migration recovery procedure in those cases. A queue pause is the better first control when every additional run can create costly duplicate or corrupt records; after the stream is contained, choose the recovery path deliberately.

Automatic rollback can be appropriate when the signal is narrow, attributable, and tested against delayed data, but it raises the burden of proof. For a small team, a page followed by one authenticated operator action is often easier to reason about than automation that couples a freshness anomaly directly to global configuration. Keep the action auditable, require a reason, and make re-enabling the candidate a separate decision. Fast off, careful on.

Cost belongs in this comparison, just not as the lead argument. Completion events are compact and high-value; debug logs can be much larger, and a hosted log backend may charge by ingestion volume. Retain enough structured evidence to reconstruct the decision and outcome, sample verbose diagnostic events where appropriate, and check the current billing terms of the selected backend rather than assuming that every emitted byte is free.

## Release and incident checklist

Before rollout, define “fresh” for each import class, establish who can activate the kill switch, and verify that the stable path still runs against current data. Start with a bounded property cohort. The alert payload should name the violated freshness rule, the first affected schedule, the active path and revision, and the scope; it should link to evidence, not to a decorative dashboard wall.

During an incident, acknowledge the page, confirm that missing results rather than worker availability caused it, stop risky new work if necessary, activate the stable path, and verify new completion events. Preserve the candidate revision and affected schedules for replay. Do not re-enable from the same adrenaline-soaked session merely because one graph turns green.

Afterward, reconstruct four timestamps: when the first expected result was missed, when monitoring detected it, when a human accepted the page, and when the first stable-path result committed. Those intervals separate detection weakness from notification delay and rollback delay. The postmortem action should target the longest consequential interval, not whichever dashboard happened to look worst in the meeting.

The decision rule is plain: use a runtime kill switch when both paths remain data-compatible and its worst-case propagation time fits the incident objective; otherwise pause the work and use a deployment or data rollback designed for the actual irreversible boundary. Health monitoring proves the result went stale. It does not make the reversal safe.

## Further reading

- https://12factor.net/logs
- https://aws.amazon.com/cloudwatch/pricing/

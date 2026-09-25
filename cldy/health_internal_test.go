package cldy

import (
	"context"
	"testing"
	"time"
)

// The upload loop fails liveness only when it stops making progress: a cycle with no upload
// attempt for longer than one payload's worst case plus an interval, or no cycle for two
// intervals. A slow cycle that keeps attempting uploads is live.
func TestUploadStalled(t *testing.T) {
	const interval = 10 * time.Minute
	timeout := 60 * time.Second
	budget := uploadAttemptBudget(timeout) + interval
	now := time.Unix(1_000_000, 0)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }

	tests := map[string]struct {
		hb   UploadHeartbeat
		want bool
	}{
		"loop not started":              {hb: UploadHeartbeat{}, want: false},
		"waiting for the first cycle":   {hb: UploadHeartbeat{LoopStart: ago(interval)}, want: false},
		"first cycle overdue":           {hb: UploadHeartbeat{LoopStart: ago(2*interval + time.Second)}, want: true},
		"idle between cycles":           {hb: UploadHeartbeat{LoopStart: ago(time.Hour), LastCycleStart: ago(interval + time.Minute), LastCycleEnd: ago(interval)}, want: false},
		"no cycle for two intervals":    {hb: UploadHeartbeat{LoopStart: ago(time.Hour), LastCycleStart: ago(2*interval + 2*time.Second), LastCycleEnd: ago(2*interval + time.Second)}, want: true},
		"long cycle making progress":    {hb: UploadHeartbeat{LoopStart: ago(3 * time.Hour), LastCycleStart: ago(2 * time.Hour), LastCycleEnd: ago(3 * time.Hour), LastProgress: ago(time.Minute)}, want: false},
		"cycle within its budget":       {hb: UploadHeartbeat{LoopStart: ago(time.Hour), LastCycleStart: ago(budget - time.Second), LastCycleEnd: ago(time.Hour)}, want: false},
		"cycle with no progress (hung)": {hb: UploadHeartbeat{LoopStart: ago(time.Hour), LastCycleStart: ago(budget + time.Second), LastCycleEnd: ago(time.Hour)}, want: true},
		"progress stopped mid cycle":    {hb: UploadHeartbeat{LoopStart: ago(3 * time.Hour), LastCycleStart: ago(2 * time.Hour), LastCycleEnd: ago(3 * time.Hour), LastProgress: ago(budget + time.Second)}, want: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cu := &CldyUploader{config: UploaderConfig{UploadFrequency: interval, ApptioConfig: ApptioConfig{Timeout: timeout}}, heartbeat: tt.hb}
			got, reason := cu.UploadStalled(now)
			if got != tt.want {
				t.Errorf("UploadStalled = %v (%s), want %v", got, reason, tt.want)
			}
			ce := &Emitter{Uploader: cu}
			if report := ce.HealthCheck(context.Background(), now); report.Live == tt.want {
				t.Errorf("HealthCheck live = %v, want %v", report.Live, !tt.want)
			}
		})
	}
}

// Every condition the emitter and uploader raise makes the emitter not ready; none makes it not
// live (I4).
func TestConditionsNeverFailLiveness(t *testing.T) {
	for _, name := range []string{
		conditionDiskPressure, conditionDiskSpaceUnknown, conditionUninitialised, conditionUploaderUnconfigured,
		conditionUploaderMisconfigured, conditionUploadConnectivityFailed, conditionUploadAuthFailed, conditionUploadsRejected,
	} {
		cu := &CldyUploader{events: NewEventCounts(), conditions: newConditionStore(nil)}
		ce := &Emitter{Uploader: cu, events: cu.events, conditions: cu.conditions}
		ce.setCondition(name, true, "raised for the test: https://example.test/x?sig=SECRET")
		report := ce.HealthCheck(context.Background(), time.Now())
		if !report.Live || len(report.Conditions) != 1 || report.Conditions[0].Type != name {
			t.Errorf("%s: live=%v conditions=%+v, want live with the one condition", name, report.Live, report.Conditions)
		}
		if msg := report.Conditions[0].Message; msg != "raised for the test: https://example.test/x?REDACTED" {
			t.Errorf("%s: message %q not redacted", name, msg)
		}
		cu.setCondition(name, false, "cleared")
		if report := ce.HealthCheck(context.Background(), time.Now()); len(report.Conditions) != 0 {
			t.Errorf("%s: still reported after it cleared: %+v", name, report.Conditions)
		}
	}
}

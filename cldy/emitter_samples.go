package cldy

import (
	"fmt"
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	v1 "k8s.io/api/core/v1"
)

// minSampleBytesEstimate is the disk budget for a sample before one has been finalised.
const minSampleBytesEstimate = 64 << 10

// shortLivedPodKey identifies a pod across snapshots: its UID, or namespace/name without one.
func shortLivedPodKey(pod *v1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

// addShortLivedPods adds pods to the pending short-lived pods. A pod already pending is replaced
// by its newer copy. Past maxPendingShortLivedPods the oldest are dropped and counted.
func (ce *Emitter) addShortLivedPods(pods []*v1.Pod) {
	if ce.pendingShortLivedKeys == nil {
		ce.pendingShortLivedKeys = map[string]int{}
	}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		key := shortLivedPodKey(pod)
		if i, ok := ce.pendingShortLivedKeys[key]; ok {
			ce.pendingShortLivedPods[i] = pod
			continue
		}
		ce.pendingShortLivedKeys[key] = len(ce.pendingShortLivedPods)
		ce.pendingShortLivedPods = append(ce.pendingShortLivedPods, pod)
	}
	over := len(ce.pendingShortLivedPods) - maxPendingShortLivedPods
	if over <= 0 {
		return
	}
	ce.pendingShortLivedPods = append([]*v1.Pod(nil), ce.pendingShortLivedPods[over:]...)
	clear(ce.pendingShortLivedKeys)
	for i, pod := range ce.pendingShortLivedPods {
		ce.pendingShortLivedKeys[shortLivedPodKey(pod)] = i
	}
	ce.drop(dropReasonShortLivedPodOverflow, over,
		fmt.Sprintf("more than %d short-lived pods waiting for a sample; the oldest were discarded", maxPendingShortLivedPods))
}

// clearShortLivedPods forgets the pending short-lived pods once a finalised sample holds them.
func (ce *Emitter) clearShortLivedPods() {
	ce.pendingShortLivedPods = nil
	clear(ce.pendingShortLivedKeys)
}

// withShortLivedPods appends the short-lived pods that aren't also in pods.
func withShortLivedPods(pods, shortLived []*v1.Pod) []*v1.Pod {
	if len(shortLived) == 0 {
		return pods
	}
	live := make(map[string]struct{}, len(pods))
	for _, p := range pods {
		live[shortLivedPodKey(p)] = struct{}{}
	}
	out := append(make([]*v1.Pod, 0, len(pods)+len(shortLived)), pods...)
	for _, p := range shortLived {
		if _, ok := live[shortLivedPodKey(p)]; !ok {
			out = append(out, p)
		}
	}
	return out
}

// discardStaging removes a staging directory. It held no finalised sample, so this is an
// unfinalised discard, not a drop.
func (ce *Emitter) discardStaging(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf("failed to remove unfinalised Cloudability sample %s: %v", dir, err)
		return
	}
	ce.events.UnfinalizedDiscarded(1)
}

// sweepOrphanedStaging removes staging directories older than twice the emission interval, left
// by a crash or an earlier process. The live current and next directories are never removed:
// rewriting their files doesn't update the directory mtime, so they can look old after a stall.
func (ce *Emitter) sweepOrphanedStaging() {
	_, staging, err := sampleDirs(ce.ScratchPath)
	if err != nil {
		log.Errorf("failed to list Cloudability samples in %s: %v", ce.ScratchPath, err)
		return
	}
	maxAge := 2 * max(ce.emissionInterval, time.Minute)
	for _, name := range staging {
		dir := SafePath(ce.ScratchPath, name+"/")
		if dir == ce.currentSamplePath || dir == ce.nextSamplePath {
			continue
		}
		info, err := os.Stat(dir)
		if err != nil || ce.clock().Sub(info.ModTime()) <= maxAge {
			continue
		}
		log.Infof("Removing unfinalised Cloudability sample %s left by a crash or a failed write", name)
		ce.discardStaging(dir)
	}
}

// ensureDiskBudget checks, once per sample, that the scratch volume has room for a sample the
// size of the last one. If not, it evicts the oldest finalised samples, each a counted drop, and
// reports whether there is room now. A statfs failure is not pressure: it is reported and
// nothing is evicted (F-49).
func (ce *Emitter) ensureDiskBudget() bool {
	need := uint64(max(ce.lastSampleBytes, minSampleBytesEstimate))
	avail, err := diskAvailable(ce.ScratchPath)
	if err != nil {
		ce.setCondition(conditionDiskSpaceUnknown, true, fmt.Sprintf("cannot read free space on the Cloudability scratch volume, not evicting anything: %v", err))
		return true
	}
	ce.setCondition(conditionDiskSpaceUnknown, false, "free space on the Cloudability scratch volume is readable again")
	if avail >= need {
		ce.setCondition(conditionDiskPressure, false, "Cloudability scratch volume has room for samples again")
		return true
	}
	ce.setCondition(conditionDiskPressure, true, fmt.Sprintf("Cloudability scratch volume has %d bytes free, a sample needs about %d; evicting the oldest samples", avail, need))

	finalised, _, err := sampleDirs(ce.ScratchPath)
	if err != nil {
		log.Errorf("failed to list Cloudability samples in %s: %v", ce.ScratchPath, err)
		return false
	}
	for _, name := range finalised {
		dir := SafePath(ce.ScratchPath, name+"/")
		if err := os.RemoveAll(dir); err != nil {
			log.Errorf("failed to evict Cloudability sample %s: %v", name, err)
			continue
		}
		ce.Uploader.RemoveSample(dir)
		ce.drop(dropReasonDiskPressure, 1, fmt.Sprintf("evicted sample %s to make room on the scratch volume", name))

		avail, err = diskAvailable(ce.ScratchPath)
		if err != nil {
			return true
		}
		if avail >= need {
			return true
		}
	}
	return false
}

// drop counts and logs lost data. Every drop is logged at Error.
func (ce *Emitter) drop(reason string, count int, detail string) {
	log.Errorf("event=data_dropped emitter=cloudability reason=%s count=%d: %s", reason, count, detail)
	ce.events.DataDropped(reason, count)
}

// setCondition records a condition and logs only when it changes: at Error when raised, at Info
// when cleared.
func (ce *Emitter) setCondition(name string, active bool, msg string) {
	if ce.conditions[name] == active {
		return
	}
	ce.conditions[name] = active
	ce.events.SetCondition(name, active)
	if active {
		log.Errorf("condition=%s active: %s", name, msg)
	} else {
		log.Infof("condition=%s cleared: %s", name, msg)
	}
}

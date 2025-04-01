package emitter

// EmitterID is a string type that represents a readable identifier for an emitter.
type EmitterID string

const (
	KubecostEmitterID EmitterID = "kubecost-emitter"
	CldyEmitterID     EmitterID = "cldy-emitter"
	TurboEmitterID    EmitterID = "turbo-emitter"
)

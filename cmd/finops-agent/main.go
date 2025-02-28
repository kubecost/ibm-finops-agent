package finopsagent

import "github.com/ibm/finops-agent/pkg/core"

// entry point for finops-agent
func main() {
	// Initialize/Bootstrap the Agent Data Source
	dataSource := core.NewAgentDataSource()

	// TODO: load emitters with data source
	// kc := emitter.NewKubecostEmitter(dataSource)
	// cldy := emitter.NewCloudyEmitter(dataSource)
	// turbo := emitter.NewTurboEmitter(dataSource)
	_ = dataSource

	// DRAFT: Write Emitter Manager/Controller which controls the internal data emission
	// DRAFT: cycle leveraging the emitter contract. This will be the main loop of the agent.
	// DRAFT: Application will exit on the emitter controller shutddown.
}

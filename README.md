# ibm-finops-agent
The temporary home for the unified ibm finops agent for kubecost, cloudy, and turbo products.


### Development Setup

##### To setup the workspace: 
```bash
#!/bin/bash
# 
mkdir unified-agent 
cd unified-agent 
git clone git@github.com:kubecost/ibm-finops-agent.git -b develop
git clone git@github.com:opencost/opencost.git -b bolt/opencost-mods
```

##### VS Code Launcher
Add the following to your `.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch Unified-Agent",
            "type": "go",
            "request": "launch",
            "mode": "debug",
            "program": "${workspaceFolder}/cmd/finops-agent",
            "args": [],
            "envFile": "${workspaceFolder}/env/local-run.env",
            "showLog": true,
        }
    ]
}
```

##### Application Entry Point
The main entry point is 
```
cmd/finops-agent/main.go
```

##### Environment Variables 
Opencost is configured purely with env vars, but assuming that more environment variables will be necessary, feel free to add local run and/or testing envvars to the `env/local-run.env` file. These are used when you debug through VS Code.

##### Notes 
* OpenCost data source is still dependent on prometheus for the time being. 
  * In time, we can likely add some type of no-op implementation that will allow us to test around prometheus (until our promless implementation is ready).
* There is likely a good bit of refactoring that will be required based on cloudy and turbo requirements
  * For example, the kubernetes client initialization/instantiation currently just leverages the opencost approach, but I think there are proxy/auth requirements that will likely require us move out to the core data source initializer.
* Deferred guessing at the Emitter contract, pending a further discussion to ensure we capture the requirements accurately.
* Lots of TODOS, FIXMEs, and DRAFT comments in the code used to stub out ideas/placeholders

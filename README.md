# ibm-finops-agent

The unified IBM FinOps Agent for Kubecost and Cloudability products.

### Development Setup

##### To setup the workspace

```bash
#!/bin/bash
# 
mkdir unified-agent
cd unified-agent
git clone git@github.com:kubecost/ibm-finops-agent.git -b develop
cd ibm-finops-agent
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

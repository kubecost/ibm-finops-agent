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

##### For local changes and creating a finops agent image

1. If you have local Opencost changes that you want to test, add the following replace directives to your go.mod:

```

replace (
	github.com/opencost/opencost => ../opencost
	github.com/opencost/opencost/core => ../opencost/core
	github.com/opencost/opencost/modules/collector-source => ../opencost/modules/collector-source
	github.com/opencost/opencost/modules/prometheus-source => ../opencost/modules/prometheus-source
)
```

2. Make sure you are authenticated to Artifactory using Docker or Podman so that you can push the image.

3. Run the following Makefile target to build and push a multi-architecture FinOps Agent image:

```
make podman-build-push IMAGETAG=<YOUR_IMAGE_NAME>
```

##### Environment Variables

Opencost is configured purely with env vars, but assuming that more environment variables will be necessary, feel free to add local run and/or testing envvars to the `env/local-run.env` file. These are used when you debug through VS Code.

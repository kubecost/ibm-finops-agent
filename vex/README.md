# VEX

`ibm-finops-agent.openvex.json` is an [OpenVEX](https://openvex.dev) document. It lists vulnerabilities that scanners report in the `icr.io/ibm-finops/agent` image but that do not affect the agent, with the reason for each. Every release attaches the current copy.

## Use it

```sh
trivy image --vex vex/ibm-finops-agent.openvex.json icr.io/ibm-finops/agent:<tag>
grype icr.io/ibm-finops/agent:<tag> --vex vex/ibm-finops-agent.openvex.json
```

Each statement names the product `pkg:oci/agent?repository_url=icr.io/ibm-finops/agent`. Scanners match `repository_url` exactly, so if you scan a mirrored copy, set it to your mirror's repository.

Trivy v0.76.0 hides only the pcre2, coreutils and golang.org/x/crypto findings. The glibc, libgcc and ncurses packages depend on each other in a cycle, and Trivy's VEX check treats a dependency cycle as reachable, so those findings stay in its report.

## Add a statement

Add a statement only when you can show that the vulnerable code is absent from the image or never runs. Name the image as the product and the vulnerable package as a subcomponent. Never name the package itself as the product: scanners then treat that package as not affected in every image. Increase `version` and update `timestamp`.

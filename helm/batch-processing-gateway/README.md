# Batch Processing Gateway Helm Chart

The Helm chart can be used to easily deploy Batch Processing Gateway service to a Kubernetes cluster.

> Up to today, we do not have a public image on Docker Hub available, so you need to use your own docker registry in order to install this helm chart.


## Chart

### ConfigMap

The ConfigMap `bpg` provides the configuration that Batch Processing Gateway runs with.
The BPG config is encoded as a base64 string under binaryData.
Eventually the string will be provided as a .gz file, and BPG will unzip it during launching.

The encoded string needs to be generated from a config in YAML format. Please see the config template and example in the `resources` directory.

At this point we do not have a general tool to quickly create the encoded string from a config. Your suggestion and contribution will be much appreciated.


### Deployment

#### Main BPG Instances

The `bpg` containers are the instances that are handling user requests. Adjust the replica according to your load.
There's a `/healthcheck` endpoint on port 8081 for liveness probe.

#### Helper BPG Instance

The `bpg-helper` container is needed to monitor the all job status and keep the DB updated. The replica is fixed to 1.
In the future the monitoring process should be decoupled from BPG for better maintenance.

### Service

Just a plain service available on port 80.

### Ingress

- A version needs to be specified by user (currently `/apiv2`) so that we can support multiple versions if needed.
- The upload request body is limited to 800m. You may adjust accordingly, but it can take much longer for large request bodies.

### Swagger UI

[Swagger UI](https://swagger.io/tools/swagger-ui/) is included in the helm chart to launch a web UI for API spec visualization.
The URL scheme is `<base URL>/swagger`.

## BPG Configs

At this point you'd need to manually create BPG configs, but you may use the template provided to build your own tools, or simply modify the example with your config values.

- `config.yaml.j2`: A template that can be used by tools to generate BPG configs.
- `spark-config.yaml.j2`: A template for the Spark cluster part of the `config.yaml.j2` template.
- `config.example.yaml`: An example of BPG config with dummy values. You may adapt this file with your config values.

### Spark configuration
Spark can be pre-configured with default set of values by setting optional `defaultSparkConf` property in the BPG config with a list 
of default spark configurations. This configuration is set as default and can be overriden with configuration propvided via 
BPG Rest Api call. 

In addition to setting default configuration it is possible to enforce certain spark settings by using optional `fixedSparkConf` 
property in the BPG config file. Any configuration set this way will be enforced for all BPG api calls and will not be 
reset by client providing alternative setting for listed configurations. 

Please refer to `config.example.yaml` for an example on how to provide spark configurations. 
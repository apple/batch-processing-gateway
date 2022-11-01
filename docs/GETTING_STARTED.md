# Getting Started

## Getting Started on Mac
Follow this guide to set up service locally for development.
Local development is easier with a K8s cluster to which the Spark apps can be submitted. You can set up your own K8s cluster, or follow the minikube setup guide below.

### Prerequisites

Install [Homebrew](https://brew.sh) as the package manager. Ensure it is up-to-date and working nicely.
```bash
brew update && brew doctor
```

- Install [Maven](https://maven.apache.org)
- Install [Helm](https://helm.sh)
- Install [jq](https://stedolan.github.io/jq/)
```bash
brew install maven
brew install helm
brew install jq
```

Install the [AWS Command Line Interface (CLI)](https://aws.amazon.com/cli/).

### Set up Minikube (Optional)
Here's a guide to set up [minikube](https://minikube.sigs.k8s.io/docs/) as a local K8s cluster, but you can skip this step if you already have a cluster available running.
Install and start minikube preferably with [Hyperkit](https://minikube.sigs.k8s.io/docs/drivers/hyperkit/) driver.
We are using a specific K8s version (1.21.14) for convenience, as it automatically creates secrets for service accounts.

Preferred Option: Using Hyperkit as driver (no Docker Desktop required)

    $ brew install hyperkit minikube
    $ minikube start --kubernetes-version=1.21.14 --cpus=4 --memory=8192 --driver=hyperkit --addons=registry,storage-provisioner,ingress

> Hyperkit may not support arm64 (Apple Silicon). Please use Docker as an alternative.

Alternative: Using [Docker](https://www.docker.com) as Driver

    $ brew install minikube
    $ minikube start --kubernetes-version=1.21.14 --cpus=4 --memory=8192 --driver=docker --addons=storage-provisioner,ingress

### Install Dependent Services
At this point we assume that your `kubectl` context should be your K8s cluster.
The scripts for setting up services are in `dev-setup` directory. You may look into each bash script to see what they are doing.

    $ cd dev-setup

#### Setup Spark K8s Operator
Create a namespace `spark-applications` and install the [Spark operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) that checks the `spark-applications` namespace:

    $ sh ./setup-spark-operator.sh

#### Setup YuniKorn Scheduler
Install the [YuniKorn](https://yunikorn.apache.org) scheduler:

    $ sh ./setup-yunikorn.sh

#### Setup PostgreSQL
Install [PostgreSQL](https://www.postgresql.org) as relational database running on service port 5432:

    $ sh ./setup-postgresql.sh

#### Setup S3 Service
Create namespace `local-ozone`, and deploy Ozone S3:

    $ sh ./setup-ozone-s3.sh

This can take a few minutes. Ensure all the pods are running for 1 minute before moving on to the next step.

    $ kubectl -n local-ozone get pods

### Port-forward
Assuming you have PostgreSQL running on 5432 and Ozone S3 service running on 9878 by following the steps above.
In a separate terminal window run `port-forward.sh`. Keep the window open.

    $ sh ./port-forward.sh

This will forward the PostgreSQL port 5432 and Ozone S3 service port 9878 to localhost.

### Initiate S3 Bucket
Create a S3 bucket and put all initial objects needed:

    $ sh ./init-s3-bucket.sh

### Generate Config
At this point all the dependency services should have been set up on your K8s cluster.

Generate a config for development:

    $ sh ./generate-bpg-config.sh

The script will generate a `bpg-config.yaml`. Be aware that this script assumes you have only one K8s cluster, while in production you can have many.

### Build and Run

Ensure you are in the project root directory, then build and run:

    $ mvn clean package
    $ java -Ds3.endpoint.url=http://localhost:9878 \
        -jar target/bpg-release.jar server ./dev-setup/bpg-config.yaml
    ...

This should launch a service instance with requisite PostgreSQL as backing database,
spark-on-k8s-operator with configured `spark-application` namespace, the YuniKorn resource scheduler, and Apache Ozone for S3 storage support.

> Alternative: If you are using IDE (such as [IntelliJ IDEA](https://www.jetbrains.com/idea/)), you can start the application by running the main class `com.apple.spark.BPGApplication` directly. Your IDE typically should handle the Maven dependencies and add them to the Java class path.

### Upload Spark Artifact

For a Spark app to run, all its artifacts need to be available in S3.
As an example, you can publish a minimal PySpark app under `/src/test/resources`:

    $ export AWS_ACCESS_KEY_ID="${USER}"
    $ export AWS_SECRET_ACCESS_KEY=$(openssl rand -hex 32)
    $ curl -u user_name:dummy_password http://localhost:8080/apiv2/s3/MinimalSparkApp.py\?folder=foo/ -X POST \
        --data-binary "@src/test/resources/MinimalSparkApp.py"

    {"url":"s3a://bpg/uploaded/foo/MinimalSparkApp.py"}

### Submit a Spark app
Now the REST endpoints are running locally at `http://localhost:8080`. You can submit a Spark app to the Spark endpoint. If everything goes well, you should get a 200 response with a `submissionId`, which is a unique identifier of a submitted Spark app.

    $ curl -u user_name:dummy_password http://localhost:8080/apiv2/spark -i -X POST \
        -H 'Content-Type: application/json' \
        -d '{
            "sparkVersion": "3.2",
            "mainApplicationFile": "s3a://bpg/uploaded/foo/MinimalSparkApp.py",
            "driver": {
            "cores": 1,
            "memory": "2g"
        },
        "executor": {
            "instances": 1,
            "cores": 1,
            "memory": "2g"
        }
        }'

    HTTP/1.1 200 OK
    Date: Mon, 12 Sep 2022 00:22:00 GMT
    Content-Type: application/json
    Content-Length: 60
    
    {"submissionId":"minikube-9a2e2598d1ee4ff4b33ee7ebcdb63a6c"}

You should be able to see the Spark CRD in namespace `spark-applications`:

    $ kubectl --namespace spark-applications get sparkapplications

    NAME                                        STATUS    ATTEMPTS   START                  FINISH       AGE
    minikube-9a2e2598d1ee4ff4b33ee7ebcdb63a6c   RUNNING   1          2022-10-01T00:11:46Z   <no value>   6m34s

> For minikube, You can choose to use [minikube dashboard](https://minikube.sigs.k8s.io/docs/handbook/dashboard/) to easily browse through K8s resources via a UI.

From here, the rest of the work is done by Spark Operator. It will enqueue the job for submission, create a service for the Spark UI, and eventually submit the job with `spark-submit`.

#### Get job Status
Use the status API to query the job status:

    $ curl -u user_name:dummy_password \
      http://localhost:8080/apiv2/spark/minikube-9a2e2598d1ee4ff4b33ee7ebcdb63a6c/status
    
    {"creationTime":1662961198000,"duration":0,"applicationState":"UNKNOWN"}


#### Get job description
Retrieve a full job description including the metadata in submission request:

    $ curl -u user_name:dummy_password \
        http://localhost:8080/apiv2/spark/minikube-9a2e2598d1ee4ff4b33ee7ebcdb63a6c/describe

#### Get job driver log
    $ curl -u user_name:dummy_password \
        http://localhost:8080/apiv2/log?subId=minikube-9a2e2598d1ee4ff4b33ee7ebcdb63a6c

The health check endpoint runs on port 8081 by default. Check the server health by sending GET to `/healthcheck`, or simply open this in browser: http://localhost:8081/healthcheck
```bash
# the response should look like
{"deadlocks":{"healthy":true,"duration":0,"timestamp":"2022-03-31T12:28:10.642-07:00"},"sparkClusters":{"healthy":true,"duration":0,"timestamp":"2022-03-31T12:28:10.642-07:00"}}
```

### Cleanup (Optional)

If you used minikube and followed all steps above, you can quickly remove everything set up:

    $ cd dev-setup
    $ sh cleanup.sh

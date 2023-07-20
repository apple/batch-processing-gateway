# Skate Apple Internal Artifacts

Everything in the `apple-internal` dir is for Apple internal use.
The files and docs here should never be published to the public Github.

Please note the Rio `rio.xml` under the repo root is also Apple internal.

## Skate Config for Integration Test

`int-test-config.enc.yml` is an encrypted Skate config file with AWS EKS and S3 paths.
It is used for integration tests triggered by PRs, defined in the `pom.xml`.

You may use this file as a starting point and modify it for dev/test purposes.

### To decrypt the config file

Install SOPS:
```bash
brew install sops
```

Request the AGE decryption key from [BPIA team](<mailto:AIML_DI_BPIA@group.apple.com>).

Set the AGE key according to [SOPS usage guide](https://github.com/mozilla/sops#22encrypting-using-age):
- Put the AGE key in a local file, such as `/Users/tianqitong/Downloads/sops_key.txt`.
- Set the env variable `export SOPS_AGE_KEY_FILE=/Users/tianqitong/Downloads/sops_key.txt`

Use SOPS to decrypt Skate config file:

    $ sops -d int-test-config.enc.yml > config.yml

You can then use the decrypted config file to run Skate instances.


### To update the config file

If you need to update the config file, e.g. by adding / removing Spark clusters, follow the steps:
- Decrypt the file using the method above
- Update the file with clear content
- Set the same AGE key and variable, and encrypt the file again with:
```bash
$ sops --encrypt --age <pub-key-in-key-file> config.yml > int-test-config.enc.yml 
```

- Create a PR and merge `int-test-config.enc.yml` changes.

## Spinnaker Helm Chart
The `spinnaker-skate` is a Helm chart particularly made for Spinnaker deployment.
As defined in Rio `rio.xml`, every time a change is merged to the main branch, a new helm chart version is automatically published.

## Local Development
1) Code format: `mvn spotless:apply`.
2) Compile & package: `mvn clean package`. To skip tests all together, append `-DskipTests`.
3) Tests:
    1) For unit tests `mvn test`.
    2) Steps to run/test batch-processing-gateway locally:
       1. Add config.yaml in the local batch-processing-gateway project folder. The content of config.yaml is the configMap of skate-vXX (like skate-v046)
       2. Set up a run configuration to run com.apple.spark.BGPApplication with "server config.yaml" as parameter
       3. In a terminal, set the aws-profile and context according to the config.yaml
       4. Run application
       ```bash
       java -jar target/bpg-release.jar server config.yaml
       ```
       5. Use the following in the above terminal to submit a job
       ```bash
       curl -u user_name:dummy_password http://localhost:8080/skatev2/spark -i -X POST \
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
       ```
### Troubleshooting:
If you get an error like following:
```bash
curl: (7) Failed to connect to localhost port 8080 after 6 ms: Connection refused
```
Please change the server.applicationConnectors.port in the config.yaml and the corresponding endpoint in the above command line, then try again.

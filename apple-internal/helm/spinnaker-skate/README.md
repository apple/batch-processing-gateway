# Spinnaker-specific Skate Helm Chart

This is a variant of the Skate Helm chart modified to work with Spinnaker. Spinnaker handles Helm charts in a particular manner when advanced rollout policies (Red/Black) are involved, it needs to manage Services itself, and also it requires a ReplicaSet instead of a Deployment, because the Deployment interferes with the rollout policy when Spinnaker handles it.

The authoritative Helm chart remains "skate", this is just a customized copy.
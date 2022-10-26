# Skate Continuous Delivery

## Overview

Skate deployments are performed with Spinnaker pipelines. General considerations on Spinnaker-based pipeline creation and operations are available in [Spinnaker-based Continuous Deployment](https://github.pie.apple.com/aiml-datainfra/automation/blob/develop/doc/spinnaker/spinnaker-based-continuous-deployment.md) documentation. Please consult that material if you need details on creating or operating an AI/ML DP Spinnaker pipeline, including Skate deployment pipelines. This document contains only Skate-specific details, and is, in fact, a Skate extension of "Spinnaker-based Continuous Deployment".

## Spinnaker Load Balancers

Spinnaker requires direct control over the application's Kubernetes Services, so they must be disabled in the application's Helm chart (see [spinnaker-managed-prod-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-stage-service-example.yaml) for an example of how to do that) and created manually in Spinnaker, as shown below.

Create the Stage service by going to "Load Balancers" → "Create Load Balancer" → Account: use the onboarded EKS cluster → Manifest: use the content of [spinnaker-managed-stage-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-stage-service-example.yaml). Use the newly created stage namespace.

Repeat the operation with:
* [spinnaker-managed-stage-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-stage-service-example.yaml)
* [spinnaker-managed-stage-auth-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-stage-auth-service-example.yaml)
* [spinnaker-managed-stage-ingress-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-stage-ingress-example.yaml)
* [spinnaker-managed-prod-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-prod-service-example.yaml)
* [spinnaker-managed-prod-auth-service-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-prod-auth-service-example.yaml)
* [spinnaker-managed-prod-ingress-example.yaml](https://github.pie.apple.com/aiml-datainfra/skate/blob/master/src/main/helm/spinnaker-skate/templates/spinnaker-managed-prod-ingress-example.yaml)

Some precautions need to be taken when the pipeline being built targets an environment that is already in production. Next section addresses this situation.

### Adding the Deployment Pipeline to an Environment that is Serving Production

This section provides more details on how to build the Spinnaker-based pipeline to target an environment that is already serving production users. 

The production traffic is routed into the authenticated Skate services, and the route is specified by an Ingress with two paths (the example is taken from the Knowledge environment):

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: skate-ingress
  namespace: skate
spec:
  rules:
  - host: skate-knowledge.aws.ocean.g.apple.com
    http:
      paths:
      - backend:
          service:
            name: skate-authed
            port:
              number: 9090
        path: /skatev2/(.*)
        pathType: ImplementationSpecific
      - backend:
          service:
            name: skate-hidden-authed
            port:
              number: 9091
        path: /hidden-skatev2/(.*)
        pathType: ImplementationSpecific
[...]
```
The main route (`/skatev2`) targets `skate-authed` service, and an alternative route (`/hidden-skatev2`) is part of a testing mechanism that will be phased out by the introduction of the Spinnaker blue-green pipelines.

While building and testing the pipeline, is important to use at first two new routes that do not interfere in any way with `/skatev2` and `/hidden-skatev2`. The values provided in the manifests shipped with the chart, and that are used in the examples cited in the [Spinnaker Load Balancers](#spinnaker-load-balancers) section, are `/prod` and `/stage`, but these values can be modified as necessary while the Spinnaker Load Balancers are being created. 

The provisional routes can be used while the pipeline is being tested, and they can be replaced with the value production clients are configured with the standard value (`/skatev2`) at a later date, in such a way to ensure the disruption is minimal. 

## Spinnaker Pipeline

This section outlines Skate-specific details of the generic Spinnaker continuous delivery pipeline and works as a Skate extension of [Spinnaker-based Continuous Deployment](https://github.pie.apple.com/aiml-datainfra/automation/blob/develop/doc/spinnaker/spinnaker-based-continuous-deployment.md).

### Input Stage
The Skate production images are maintained in `docker.apple.com/aiml-di-dpi/skate/master` repository by default, but this value can be changed through configuration. An example of a valid container image tag is `skate-1.1.192`.

### Configuration Overlay Generation

The environment-specific configuration required during deployment is provided to the deployment process via a Helm overlay, specified in-line in the "[Helm Chart Rendering Stage](https://github.pie.apple.com/aiml-datainfra/automation/blob/develop/doc/spinnaker/spinnaker-based-continuous-deployment.md#helm-chart-rendering-stage)" pipeline stage as an "embedded-artifact".

The contents of the embedded artifacts are generated automatically by the Spinnaker pipeline.
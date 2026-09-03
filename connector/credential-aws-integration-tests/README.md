---
layout: global
title: Spark OIDC AWS Credential Propagation Integration Tests
---

# Running the OIDC E2E Integration Tests

These tests verify the end-to-end OIDC credential propagation flow on a real Kubernetes cluster:

```
Projected SA Token -> FileTokenIngestor -> AwsStsCredentialProvider -> moto STS -> moto S3 (via S3A)
```

[moto](https://github.com/getmoto/moto) is an Apache 2.0-licensed Python library that emulates
AWS services (S3 and STS) locally.  It runs as a lightweight HTTP server alongside Minikube.

## Prerequisites

- **Minikube** >= 1.38.0. The GitHub Actions workflow runs the tests with 2 CPUs and
  6 GB memory, which is sufficient. Locally, 4 CPUs gives more headroom for the driver
  and (dynamically allocated) executor pods:
  ```
  minikube start --cpus 4 --memory 6144
  ```
- **Python 3** with moto installed:
  ```
  pip install "moto[server,s3,sts]>=5.0.0,<6.0.0"
  ```
- **Docker** available (for building the Spark image into Minikube's daemon).

## Quickstart

The simplest way to run the tests is the wrapper script:

```
connector/credential-aws-integration-tests/dev/dev-run-integration-tests.sh
```

This script:
1. Verifies prerequisites (minikube running, moto installed).
2. Starts the moto server (`python3 -m moto.server`) on port 5000, listening on all interfaces.
3. Detects the host IP as seen from inside Minikube pods.
4. Builds Spark and the Docker image with `-Phadoop-cloud` (so the image has
   hadoop-aws and the AWS SDK for S3A) and bakes the job jar (`OidcS3ReadWriteJob`)
   into `/opt/spark/jars` so it is on the driver classpath.
5. Runs the integration tests via Maven.
6. Stops moto on exit.

## Script Options

| Option | Default | Description |
|---|---|---|
| `--image-tag <tag>` | (generated) | Use a pre-built Spark image with this tag. |
| `--image-repo <repo>` | `docker.io/kubespark` | Docker image repository. |
| `--spark-image <image>` | (derived) | Full image name (overrides `--image-repo` + `--image-tag`). |
| `--deploy-mode <mode>` | `minikube` | Kubernetes backend: `minikube`, `docker-desktop`, `rancher-desktop`, `cloud`. |
| `--namespace <ns>` | (auto-generated) | Kubernetes namespace. Created if absent; deleted on exit. |
| `--service-account <sa>` | `default` | Kubernetes service account. |
| `--moto-port <port>` | `5000` | Port for the moto server. |
| `--role-arn <arn>` | `arn:aws:iam::123456789012:role/oidc-e2e-test-role` | IAM role ARN for `AssumeRoleWithWebIdentity` (moto accepts any well-formed ARN). |
| `--s3-bucket <bucket>` | `oidc-e2e-test-bucket` | S3 bucket name created in moto. |
| `--token-file <path>` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | OIDC token file path inside the driver pod. |
| `--skip-build` | false | Skip building Spark and the Docker image. |
| `--hadoop-profile <prof>` | `hadoop-3` | Hadoop Maven profile. |

## Running with a Pre-built Image

If you already have a Spark image built (e.g. from a previous run):

```
connector/credential-aws-integration-tests/dev/dev-run-integration-tests.sh \
  --image-tag my-tag \
  --skip-build
```

> **Warning:** `--skip-build` reuses an existing Spark image and does **not**
> rebuild it. If you have changed `connector/credential-aws` (or any Spark
> source) since the image was built, the tests will silently run against the
> **stale image** and may produce misleading results. Always run without
> `--skip-build` after modifying code, or rebuild the image explicitly.
>
> Unlike `kubernetes-integration-tests`, this module does not bind image
> building to the sbt `test` task, so image freshness is the caller's
> responsibility. In CI (GitHub Actions) this is a non-issue: each run builds
> the image from scratch on a fresh runner before the tests execute.

## Running Tests Directly

If you prefer to manage moto and the image yourself, you can run the tests directly via Maven.

> **Note:** the image passed via `spark.oidc.test.sparkImage` must be built with
> `-Phadoop-cloud` (so it contains hadoop-aws and the AWS SDK for S3A) and must have
> the module jar (`OidcS3ReadWriteJob`) baked into `/opt/spark/jars`. The
> `dev-run-integration-tests.sh` script does this for you.

```bash
# 1. Start moto server (in a separate terminal or background)
python3 -m moto.server -H 0.0.0.0 -p 5000 &

# 2. Determine the host IP reachable from Minikube pods
HOST_IP=$(minikube ssh "ip route | grep default | awk '{print \$3}'" | tr -d '[:space:]')
MOTO_ENDPOINT="http://${HOST_IP}:5000"

# 3. Run the tests. Pods reach moto via the host gateway IP, while the test process
#    on the host reaches it on loopback (s3ClientEndpoint).
build/mvn integration-test -am \
  -pl connector/credential-aws-integration-tests \
  -Phadoop-3 -Pkubernetes -Pcredential-aws -Poidc-e2e \
  -Dspark.kubernetes.test.deployMode=minikube \
  -Dspark.oidc.test.stsEndpoint="${MOTO_ENDPOINT}" \
  -Dspark.oidc.test.s3Endpoint="${MOTO_ENDPOINT}" \
  -Dspark.oidc.test.s3ClientEndpoint="http://127.0.0.1:5000" \
  -Dspark.oidc.test.sparkImage="docker.io/kubespark/spark:my-tag"
```

Or with sbt:

```bash
build/sbt \
  -Phadoop-3 -Pkubernetes -Pcredential-aws -Poidc-e2e \
  -Dspark.kubernetes.test.deployMode=minikube \
  -Dspark.oidc.test.stsEndpoint="${MOTO_ENDPOINT}" \
  -Dspark.oidc.test.s3Endpoint="${MOTO_ENDPOINT}" \
  -Dspark.oidc.test.s3ClientEndpoint="http://127.0.0.1:5000" \
  -Dspark.oidc.test.sparkImage="docker.io/kubespark/spark:my-tag" \
  'credential-aws-integration-tests/test'
```

## Available Maven / System Properties

| Property | Default | Description |
|---|---|---|
| `spark.kubernetes.test.deployMode` | `minikube` | Kubernetes backend. |
| `spark.kubernetes.test.imageRepo` | `docker.io/kubespark` | Docker image repository. |
| `spark.kubernetes.test.imageTag` | `N/A` | Docker image tag. |
| `spark.kubernetes.test.namespace` | (auto-generated) | Kubernetes namespace for the tests. |
| `spark.kubernetes.test.serviceAccountName` | `default` | Kubernetes service account. |
| `spark.oidc.test.stsEndpoint` | `http://localhost:5000` | moto STS endpoint as seen from Minikube pods (host gateway IP). |
| `spark.oidc.test.s3Endpoint` | `http://localhost:5000` | moto S3 endpoint as seen from Minikube pods (host gateway IP). |
| `spark.oidc.test.s3ClientEndpoint` | `http://127.0.0.1:5000` | moto S3 endpoint as seen from the test JVM on the host (loopback). |
| `spark.oidc.test.roleArn` | `arn:aws:iam::123456789012:role/oidc-e2e-test-role` | IAM role ARN. |
| `spark.oidc.test.tokenFile` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | OIDC token file path inside pods. |
| `spark.oidc.test.s3Bucket` | `oidc-e2e-test-bucket` | S3 bucket name in moto. |
| `spark.oidc.test.sparkImage` | (derived from repo+tag) | Full Spark Docker image name. |
| `test.include.tags` | (none) | Comma-separated ScalaTest tags to include. |
| `test.exclude.tags` | (none) | Comma-separated ScalaTest tags to exclude. |

## Test Cases

| Test | Status | Description |
|---|---|---|
| Basic S3 read/write | Implemented | Projected SA token -> moto STS -> S3A write/read on Minikube |
| Mid-job token rotation | Implemented | The identity token file (supplied by an init container into an emptyDir) is rewritten mid-job; the driver re-reads it on renewal and S3 access continues across the rotation |
| Late-registering executor | Implemented | With dynamic allocation, an executor registered after credential acquisition receives credentials (via the SparkAppConfig registration response) and writes to S3 |

## Architecture

```
+-------------------------------------------------------+
|  GitHub Actions runner / developer machine            |
|                                                       |
|   moto_server :5000  (S3 + STS emulator)              |
|                                                       |
|  +-------------------------------------------------+  |
|  |  Minikube                                       |  |
|  |                                                 |  |
|  |  +----------------+     +--------------------+  |  |
|  |  | Driver Pod     |     | Executor Pod(s)    |  |  |
|  |  |                |     |                    |  |  |
|  |  | FileToken      |     | SparkOidc          |  |  |
|  |  | Ingestor       |     | AwsCredentials     |  |  |
|  |  |      |         |     | Provider           |  |  |
|  |  |      v         | RPC |  reads creds       |  |  |
|  |  | AwsSts         |---->|  from store        |  |  |
|  |  | Credential     |     |      |             |  |  |
|  |  | Provider       |     |      v             |  |  |
|  |  |      |         |     |  S3A --> moto S3   |  |  |
|  |  |      v         |     |                    |  |  |
|  |  |  moto STS      |     |                    |  |  |
|  |  +----------------+     +--------------------+  |  |
|  +-------------------------------------------------+  |
+-------------------------------------------------------+
```

> **Note:** the `RPC` arrow above is a simplification. Credentials actually reach
> executors by three complementary paths: (1) the `SparkAppConfig` registration
> response, so a newly-registered executor has credentials immediately (exercised by
> the late-registering-executor test); (2) the `UpdateUserCredentials` RPC broadcast on
> each renewal (exercised by the token-rotation test); and (3) the `TaskDescription`,
> which carries the current credentials to guarantee availability before a task runs.
> The raw identity token never leaves the driver -- only the delegated, short-lived
> service credentials are propagated.

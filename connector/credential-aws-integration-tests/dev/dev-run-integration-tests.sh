#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Wrapper script for running the OIDC E2E integration tests locally.
# Mirrors the interface of resource-managers/kubernetes/integration-tests/dev/dev-run-integration-tests.sh
#
# Prerequisites:
#   - Minikube running  (minikube start --cpus 4 --memory 6144)
#   - Python 3 + moto installed  (pip install "moto[server,s3,sts]>=5.0.0,<6.0.0")
#   - Docker available and pointing at Minikube's daemon (eval $(minikube docker-env))
#
# Usage (run from anywhere; the script resolves the repository root from its own
# location and builds from there):
#   connector/credential-aws-integration-tests/dev/dev-run-integration-tests.sh [options]
#
# Options:
#   --image-tag <tag>         Use a pre-built Spark image with this tag instead of building one.
#   --image-repo <repo>       Docker image repository (default: docker.io/kubespark).
#   --spark-image <image>     Full image name (overrides --image-repo + --image-tag).
#   --deploy-mode <mode>      Kubernetes backend: minikube (default), docker-desktop, rancher-desktop, cloud.
#   --namespace <ns>          Kubernetes namespace (created if absent; deleted on exit unless pre-existing).
#   --service-account <sa>    Kubernetes service account (default: default).
#   --moto-port <port>        Port for the moto server (default: 5000).
#   --role-arn <arn>          IAM role ARN for AssumeRoleWithWebIdentity
#                             (default: arn:aws:iam::123456789012:role/oidc-e2e-test-role).
#   --s3-bucket <bucket>      S3 bucket name in moto (default: oidc-e2e-test-bucket).
#   --token-file <path>       Path to the OIDC token file inside the driver pod
#                             (default: /var/run/secrets/kubernetes.io/serviceaccount/token).
#   --skip-build              Skip rebuilding Spark and the Docker image.
#   --hadoop-profile <prof>   Hadoop Maven profile (default: hadoop-3).

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
IMAGE_REPO="docker.io/kubespark"
IMAGE_TAG=""
SPARK_IMAGE=""
DEPLOY_MODE="minikube"
NAMESPACE=""
SERVICE_ACCOUNT="default"
MOTO_PORT="5000"
ROLE_ARN="arn:aws:iam::123456789012:role/oidc-e2e-test-role"
S3_BUCKET="oidc-e2e-test-bucket"
TOKEN_FILE="/var/run/secrets/kubernetes.io/serviceaccount/token"
SKIP_BUILD=false
HADOOP_PROFILE="hadoop-3"

MOTO_PID=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while (( "$#" )); do
  case "$1" in
    --image-tag)       IMAGE_TAG="$2";         shift 2 ;;
    --image-repo)      IMAGE_REPO="$2";        shift 2 ;;
    --spark-image)     SPARK_IMAGE="$2";       shift 2 ;;
    --deploy-mode)     DEPLOY_MODE="$2";       shift 2 ;;
    --namespace)       NAMESPACE="$2";         shift 2 ;;
    --service-account) SERVICE_ACCOUNT="$2";   shift 2 ;;
    --moto-port)       MOTO_PORT="$2";         shift 2 ;;
    --role-arn)        ROLE_ARN="$2";          shift 2 ;;
    --s3-bucket)       S3_BUCKET="$2";         shift 2 ;;
    --token-file)      TOKEN_FILE="$2";        shift 2 ;;
    --skip-build)      SKIP_BUILD=true;        shift   ;;
    --hadoop-profile)  HADOOP_PROFILE="$2";    shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[dev-run-integration-tests] $*"; }

cleanup() {
  if [[ -n "${MOTO_PID}" ]]; then
    log "Stopping moto server (PID ${MOTO_PID})..."
    kill "${MOTO_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

check_prereqs() {
  log "Checking prerequisites..."

  if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install Python 3 and run: pip install 'moto[server,s3,sts]>=5.0.0,<6.0.0'" >&2
    exit 1
  fi

  if ! python3 -c "import moto" &>/dev/null; then
    echo "ERROR: moto not found. Run: pip install 'moto[server,s3,sts]>=5.0.0,<6.0.0'" >&2
    exit 1
  fi

  if [[ "${DEPLOY_MODE}" == "minikube" ]]; then
    if ! command -v minikube &>/dev/null; then
      echo "ERROR: minikube not found. Install minikube and run: minikube start --cpus 4 --memory 6144" >&2
      exit 1
    fi
    if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
      echo "ERROR: minikube is not running. Run: minikube start --cpus 4 --memory 6144" >&2
      exit 1
    fi
  fi
}

start_moto() {
  log "Starting moto server on port ${MOTO_PORT}..."
  # Listen on all interfaces so Minikube pods can reach the host
  python3 -m moto.server -H 0.0.0.0 -p "${MOTO_PORT}" &
  MOTO_PID=$!
  # Wait for moto to be ready
  local attempts=0
  until curl -sf "http://localhost:${MOTO_PORT}/" &>/dev/null; do
    attempts=$((attempts + 1))
    if [[ ${attempts} -ge 30 ]]; then
      echo "ERROR: moto server did not start within 30 seconds." >&2
      exit 1
    fi
    sleep 1
  done
  log "moto server is ready (PID ${MOTO_PID})."
}

resolve_moto_host() {
  # Determine the IP address of the host as seen from inside Minikube pods.
  # For minikube, this is the default gateway inside the VM.
  if [[ "${DEPLOY_MODE}" == "minikube" ]]; then
    MOTO_HOST=$(minikube ssh "ip route | grep default | awk '{print \$3}'" 2>/dev/null | tr -d '[:space:]')
    if [[ -z "${MOTO_HOST}" ]]; then
      log "WARNING: Could not detect Minikube gateway IP; falling back to localhost."
      MOTO_HOST="localhost"
    fi
  else
    MOTO_HOST="localhost"
  fi
  MOTO_ENDPOINT="http://${MOTO_HOST}:${MOTO_PORT}"
  log "moto endpoint reachable from pods: ${MOTO_ENDPOINT}"
  # The test process runs on the host and reaches moto on loopback, which
  # generally differs from the pod-facing gateway IP above.
  MOTO_CLIENT_ENDPOINT="http://127.0.0.1:${MOTO_PORT}"
  log "moto endpoint reachable from host (test process): ${MOTO_CLIENT_ENDPOINT}"
}

build_spark_image() {
  if [[ "${SKIP_BUILD}" == "true" ]]; then
    log "Skipping build (--skip-build)."
    # The build path tags the runnable image (with the job jar baked in) as
    # "<repo>/spark:<tag>-job". When skipping the build, derive the same name from
    # --image-tag so the tests use the job-jar image rather than falling back to the
    # plain "<repo>/spark:<tag>" (which lacks the job classes and fails with
    # ClassNotFoundException). An explicit --spark-image still wins.
    if [[ -z "${SPARK_IMAGE}" && -n "${IMAGE_TAG}" ]]; then
      SPARK_IMAGE="${IMAGE_REPO}/spark:${IMAGE_TAG}-job"
      log "Using pre-built job image: ${SPARK_IMAGE}"
    fi
    return
  fi

  log "Building Spark and Docker image..."

  cd "${REPO_ROOT}"

  if [[ "${DEPLOY_MODE}" == "minikube" ]]; then
    eval "$(minikube docker-env)"
  fi

  # Build the modules. -Phadoop-cloud pulls in hadoop-aws and the AWS SDK so the
  # image has S3A support; -Poidc-e2e builds this module (which contains the job).
  build/sbt \
    "-P${HADOOP_PROFILE}" -Phadoop-cloud -Pkubernetes -Pcredential-aws -Poidc-e2e \
    package

  # Generate an image tag from timestamp if not provided
  if [[ -z "${IMAGE_TAG}" ]]; then
    IMAGE_TAG="oidc-e2e-$(date +%Y%m%d%H%M%S)"
  fi

  # Build the base Spark Docker image (includes hadoop-aws/AWS SDK via -Phadoop-cloud).
  ./bin/docker-image-tool.sh \
    -r "${IMAGE_REPO}" \
    -t "${IMAGE_TAG}" \
    build

  # Bake the job jar (OidcS3ReadWriteJob) into the image so its classes are available to
  # the driver. SparkSubmit already puts a local:// primary resource on the driver
  # classpath (and into spark.jars for executors), so the reason for baking it in is not
  # a classpath gap -- it is that docker-image-tool.sh only copies examples/jars into the
  # image, so this module's jar would otherwise be absent from the container entirely.
  # We install it under /opt/spark/jars (already on the classpath) to keep the local://
  # reference simple.
  local job_jar
  job_jar=$(ls connector/credential-aws-integration-tests/target/scala-*/spark-credential-aws-integration-tests_*.jar \
    | grep -v -- '-tests.jar' | head -1)
  local job_jar_name
  job_jar_name=$(basename "${job_jar}")
  local build_ctx
  build_ctx=$(mktemp -d)
  cp "${job_jar}" "${build_ctx}/"
  cat > "${build_ctx}/Dockerfile" <<EOF
FROM ${IMAGE_REPO}/spark:${IMAGE_TAG}
COPY ${job_jar_name} /opt/spark/jars/${job_jar_name}
EOF
  docker build -t "${IMAGE_REPO}/spark:${IMAGE_TAG}-job" "${build_ctx}"
  rm -rf "${build_ctx}"

  SPARK_IMAGE="${IMAGE_REPO}/spark:${IMAGE_TAG}-job"
  log "Built Spark image with job jar: ${SPARK_IMAGE}"
}

run_tests() {
  log "Running OIDC E2E integration tests..."

  cd "${REPO_ROOT}"

  local mvn_args=(
    integration-test
    -am
    -pl "connector/credential-aws-integration-tests"
    "-P${HADOOP_PROFILE}"
    -Pkubernetes
    -Pcredential-aws
    -Poidc-e2e
    "-Dspark.kubernetes.test.deployMode=${DEPLOY_MODE}"
    "-Dspark.oidc.test.stsEndpoint=${MOTO_ENDPOINT}"
    "-Dspark.oidc.test.s3Endpoint=${MOTO_ENDPOINT}"
    "-Dspark.oidc.test.s3ClientEndpoint=${MOTO_CLIENT_ENDPOINT}"
    "-Dspark.oidc.test.roleArn=${ROLE_ARN}"
    "-Dspark.oidc.test.s3Bucket=${S3_BUCKET}"
    "-Dspark.oidc.test.tokenFile=${TOKEN_FILE}"
  )

  if [[ -n "${SPARK_IMAGE}" ]]; then
    mvn_args+=("-Dspark.oidc.test.sparkImage=${SPARK_IMAGE}")
  fi
  if [[ -n "${IMAGE_REPO}" ]]; then
    mvn_args+=("-Dspark.kubernetes.test.imageRepo=${IMAGE_REPO}")
  fi
  if [[ -n "${IMAGE_TAG}" ]]; then
    mvn_args+=("-Dspark.kubernetes.test.imageTag=${IMAGE_TAG}")
  fi
  if [[ -n "${NAMESPACE}" ]]; then
    mvn_args+=("-Dspark.kubernetes.test.namespace=${NAMESPACE}")
  fi
  if [[ -n "${SERVICE_ACCOUNT}" ]]; then
    mvn_args+=("-Dspark.kubernetes.test.serviceAccountName=${SERVICE_ACCOUNT}")
  fi

  build/mvn "${mvn_args[@]}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
check_prereqs
start_moto
resolve_moto_host
build_spark_image
run_tests

log "OIDC E2E integration tests completed successfully."

#!/usr/bin/env bash
# docker-image-build-and-push.sh
#
# Build + push Spark (JVM) + PySpark images to ACR.
#
# IMPORTANT:
#   This script does NOT compile Spark.
#   It assumes Spark jars already exist.
#
# Usage:
#   ./bin/docker-image-build-and-push.sh -t <tag>
#
set -euo pipefail

TAG=""
REPO="databricksregistry.azurecr.io/base/test"
PY_DOCKERFILE="resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile"

usage() {
  echo "Usage: $0 -t <tag> [--repo <repo>] [--py-dockerfile <path>] [--] [extra docker-image-tool args]"
  echo "This script does NOT compile Spark. It assumes jars already exist."
  exit 1
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t) TAG="${2:-}"; shift 2 ;;
    --repo) REPO="${2:-}"; shift 2 ;;
    --py-dockerfile) PY_DOCKERFILE="${2:-}"; shift 2 ;;
    --) shift; EXTRA_ARGS+=("$@"); break ;;
    -h|--help) usage ;;
    *) EXTRA_ARGS+=("$1"); shift ;;
  esac
done

[[ -z "${TAG}" ]] && usage
[[ -x "./bin/docker-image-tool.sh" ]] || { echo "Error: run from Spark repo root"; exit 2; }

# Verify jars are present (RELEASE or assembly)
if [[ -f "./RELEASE" ]]; then
  compgen -G "./jars/spark-*.jar" >/dev/null || {
    echo "ERROR: No jars in ./jars/. Build Spark first."; exit 3; }
else
  compgen -G "assembly/target/scala-*/jars/spark-*.jar" >/dev/null || {
    echo "ERROR: No assembly jars found. Build Spark first."; exit 3; }
fi

echo "==> Logging in to ACR"
az acr login -n databricksregistry

echo "==> Building images"
ARGS=( -r "${REPO}" -t "${TAG}" -p "${PY_DOCKERFILE}" )

run() {
  local subcmd="$1"; shift
  if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    ./bin/docker-image-tool.sh "${ARGS[@]}" -b java_image_tag=17-noble -X "$@" "${EXTRA_ARGS[@]}" "${subcmd}"
  else
    ./bin/docker-image-tool.sh "${ARGS[@]}" -b java_image_tag=17-noble -X "$@" "${subcmd}"
  fi
}

run build
echo "==> Build complete"

echo "==> Pushing images"
run push
echo "==> Push complete"

echo
echo "✅ DONE"
echo "Images:"
echo "  ${REPO}/spark:${TAG}"
echo "  ${REPO}/spark-py:${TAG}"

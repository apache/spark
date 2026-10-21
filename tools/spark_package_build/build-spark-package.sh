#!/usr/bin/env bash
# Spark binary package builder -- clone -> check out Affirm commit -> make-distribution --tgz ->
# (optional) upload to Artifactory.
# Runs inside the builder image (Buildkite agent or any x86_64 Linux Docker host); use --upload only when ready to publish.
#
# Published names are permanent (no delete, probably no overwrite), so the target is checked before the
# long build and the script refuses to upload over an existing file.

set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/build/dist}"
UPLOAD="${UPLOAD:-false}"
COMMIT=""
MVN_FLAGS="${MVN_FLAGS:-}"
ARTIFACTORY_BASE="${ARTIFACTORY_BASE:-}"      # set by the pipeline, required with --upload
TARGET_REPO_PATH="${TARGET_REPO_PATH:-}"      # set by the pipeline, required with --upload
VERSIONS_FILE="/build/VERSIONS.md"

usage() {
    cat <<EOF
Usage: $0 --spark-version VER --affirm-version VER --commit SHA [options]

Required:
  --spark-version VER    Upstream Spark version (e.g. 3.5.1); the commit must descend from tag v<VER>
  --affirm-version VER   Affirm suffix (e.g. dev1, 2) -> spark-<VER>-bin-hadoop3-affirm.<suffix>.tgz
  --commit SHA           Full SHA of the Affirm/spark commit to build, checked out as-is

Optional:
  --mvn-flags "FLAGS"    Maven profiles/flags (default depends on the Spark major version)
  --output-dir DIR       Where to write the .tgz (default: /build/dist)
  --upload               Upload to Artifactory (needs ARTIFACTORY_TOKEN, optionally ARTIFACTORY_USER)

Environment:
  GIT_USER_EMAIL, GIT_USER_NAME   required (git identity inside the container)
  MAVEN_MIRROR_URL                optional; Maven mirror for all downloads (without it Maven uses Maven Central directly)
  ARTIFACTORY_BASE                with --upload: Artifactory base URL (set by the pipeline)
  TARGET_REPO_PATH                with --upload: repository path the package is published under (set by the pipeline)
  GITHUB_TOKEN                    optional, GitHub access via GIT_ASKPASS (never in URLs or arguments)
  ARTIFACTORY_TOKEN               only with --upload; sent through a curl config on stdin, never as an argument
  ARTIFACTORY_USER                optional; if set the token is used as the password (basic auth), else as a bearer token
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --spark-version)   SPARK_VERSION="$2"; shift 2 ;;
        --affirm-version)  AFFIRM_VERSION="$2"; shift 2 ;;
        --commit)          COMMIT="$2"; shift 2 ;;
        --mvn-flags)       MVN_FLAGS="$2"; shift 2 ;;
        --output-dir)      OUTPUT_DIR="$2"; shift 2 ;;
        --upload)          UPLOAD="true"; shift ;;
        -h|--help)         usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "${SPARK_VERSION:-}" || -z "${AFFIRM_VERSION:-}" || -z "${COMMIT}" ]]; then
    echo "ERROR: --spark-version, --affirm-version and --commit are all required"
    usage
fi
if [[ ! "${COMMIT}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "ERROR: --commit must be a full 40-character SHA"
    exit 1
fi

NAME="hadoop3-affirm.${AFFIRM_VERSION}"
PACKAGE="spark-${SPARK_VERSION}-bin-${NAME}.tgz"
TARGET_URL="${ARTIFACTORY_BASE}/${TARGET_REPO_PATH}/${PACKAGE}"

# Default flags per Spark major version (3.5.1: flags of the existing affirm.2 build).
if [[ -z "${MVN_FLAGS}" ]]; then
    case "${SPARK_VERSION}" in
        3.*) MVN_FLAGS="-Denforcer.skip -Pmesos -Pkubernetes -Pscala-2.12 -Phadoop-3 -Phive -Phive-thriftserver" ;;
        4.*) MVN_FLAGS="-Pkubernetes -Phadoop-3 -Phive-thriftserver" ;;
        *) echo "ERROR: no default Maven flags for Spark ${SPARK_VERSION}; pass --mvn-flags"; exit 1 ;;
    esac
fi

echo "Package:   ${PACKAGE}"
echo "Commit:    ${COMMIT}"
echo "Mvn flags: ${MVN_FLAGS}"
java -version 2>&1 | head -1
mvn -version 2>&1 | head -1

# --- Validation: productive versions must be documented with a matching commit SHA ---
if [[ "${AFFIRM_VERSION}" =~ ^[0-9]+$ ]]; then
    SECTION="${SPARK_VERSION}-affirm.${AFFIRM_VERSION}"
    if ! grep -qE "^## ${SECTION//./\\.}\$" "${VERSIONS_FILE}"; then
        echo "ERROR: productive version needs a section '## ${SECTION}' in VERSIONS.md with a 'Commit SHA:' line."
        exit 1
    fi
    RECORDED_SHA=$(awk -v sec="## ${SECTION}" '
        $0 == sec { in_section=1; next }
        in_section && /^## / { exit }
        in_section && /[Cc]ommit [Ss][Hh][Aa]:/ { l=$0; sub(/.*[Cc]ommit [Ss][Hh][Aa]:\**[ \t]*/, "", l); gsub(/[^a-fA-F0-9]/, "", l); if (l != "") { print l; exit } }
    ' "${VERSIONS_FILE}")
    if [[ "${RECORDED_SHA,,}" != "${COMMIT,,}" ]]; then
        echo "ERROR: Commit SHA mismatch for ${SECTION}: VERSIONS.md has '${RECORDED_SHA:-none}', --commit is '${COMMIT}'."
        exit 1
    fi
    echo "OK: ${SECTION} is documented with a matching commit SHA."
else
    echo "Development version '${AFFIRM_VERSION}': skipping VERSIONS.md check."
fi

# --- Validation: the target must not exist (checked before the long build) ---
if [[ "${UPLOAD}" == "true" ]]; then
    for v in ARTIFACTORY_TOKEN ARTIFACTORY_BASE TARGET_REPO_PATH; do
        if [[ -z "${!v:-}" ]]; then
            echo "ERROR: --upload requires ${v}"
            exit 4
        fi
    done
    CODE=$(curl -sS -o /dev/null -I -w '%{http_code}' "${TARGET_URL}" || true)
    if [[ "${CODE}" == "200" ]]; then
        echo "ERROR: ${TARGET_URL} already exists. Published names are permanent; use a new --affirm-version."
        exit 5
    fi
    echo "OK: ${PACKAGE} does not exist yet (HTTP ${CODE})."
fi

# --- Git identity and optional GitHub auth ---
: "${GIT_USER_EMAIL:?ERROR: GIT_USER_EMAIL is not set}"
: "${GIT_USER_NAME:?ERROR: GIT_USER_NAME is not set}"
git config --global user.email "${GIT_USER_EMAIL}"
git config --global user.name "${GIT_USER_NAME}"
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    ASKPASS_SCRIPT=$(mktemp)
    cat > "${ASKPASS_SCRIPT}" <<'ASKPASS'
#!/usr/bin/env bash
if [[ "$1" == *"Username"* ]]; then echo "x-access-token"; else echo "$GITHUB_TOKEN"; fi
ASKPASS
    chmod +x "${ASKPASS_SCRIPT}"
    export GIT_ASKPASS="${ASKPASS_SCRIPT}" GIT_TERMINAL_PROMPT=0
    echo "OK: GITHUB_TOKEN detected, GIT_ASKPASS configured."
fi

# --- Phase 1: clone and check out ---
echo "=== Phase 1: clone and check out ==="
rm -rf /build/affirm-spark
git clone --filter=blob:none https://github.com/Affirm/spark.git /build/affirm-spark
cd /build/affirm-spark
git remote add upstream https://github.com/apache/spark.git
git fetch --quiet upstream tag "v${SPARK_VERSION}"
git fetch --quiet origin "${COMMIT}"
git checkout --quiet --detach "${COMMIT}"
if ! git merge-base --is-ancestor "v${SPARK_VERSION}" HEAD; then
    echo "ERROR: ${COMMIT} does not descend from upstream tag v${SPARK_VERSION}."
    exit 1
fi
echo "OK: ${COMMIT} descends from v${SPARK_VERSION}."

# --- Phase 2: build the distribution ---
echo "=== Phase 2: make-distribution ($(date -u +%FT%TZ)) ==="
SETTINGS_ARGS=()
if [[ -n "${MAVEN_MIRROR_URL:-}" ]]; then
    SETTINGS_ARGS=(-s /settings.xml)
    echo "Using Maven mirror from MAVEN_MIRROR_URL."
else
    echo "NOTICE: MAVEN_MIRROR_URL not set; Maven uses Maven Central directly (may be rate limited)."
fi
# shellcheck disable=SC2086
if ! ./dev/make-distribution.sh --mvn mvn --name "${NAME}" --tgz -B "${SETTINGS_ARGS[@]}" ${MVN_FLAGS}; then
    echo "ERROR: Maven build failed"
    exit 2
fi

# --- Phase 3: collect the package ---
mkdir -p "${OUTPUT_DIR}"
if [[ ! -f "${PACKAGE}" ]]; then
    echo "ERROR: expected ${PACKAGE} was not produced. Found:"
    ls -l spark-*.tgz 2>/dev/null || true
    exit 3
fi
cp "${PACKAGE}" "${OUTPUT_DIR}/"
LOCAL_SHA=$(sha256sum "${OUTPUT_DIR}/${PACKAGE}" | cut -d' ' -f1)
echo "Built ${OUTPUT_DIR}/${PACKAGE}"
echo "  size:   $(stat -c %s "${OUTPUT_DIR}/${PACKAGE}") bytes"
echo "  sha256: ${LOCAL_SHA}"
echo "  jars:   $(tar -tzf "${OUTPUT_DIR}/${PACKAGE}" | grep -c '/jars/.*\.jar$')"

# --- Phase 4: upload and verify (only with --upload) ---
if [[ "${UPLOAD}" != "true" ]]; then
    echo "=== DRY RUN: package built but NOT uploaded. Re-run with --upload to publish. ==="
    exit 0
fi

echo "=== Phase 4: upload to ${TARGET_URL} ==="
# The credential goes through a curl config on stdin (printf is a builtin), so it never appears in ps or logs.
if [[ -n "${ARTIFACTORY_USER:-}" ]]; then
    AUTH_LINE=$(printf 'user = "%s:%s"' "${ARTIFACTORY_USER}" "${ARTIFACTORY_TOKEN}")
else
    AUTH_LINE=$(printf 'header = "Authorization: Bearer %s"' "${ARTIFACTORY_TOKEN}")
fi
if ! printf '%s\n' "${AUTH_LINE}" | curl -K - -sS --fail-with-body \
        -H "X-Checksum-Sha256: ${LOCAL_SHA}" \
        -T "${OUTPUT_DIR}/${PACKAGE}" "${TARGET_URL}"; then
    echo "ERROR: upload failed"
    exit 4
fi
unset AUTH_LINE

echo "=== Verify: download back and compare SHA-256 ==="
REMOTE_SHA=$(curl -sS --fail "${TARGET_URL}" | sha256sum | cut -d' ' -f1)
if [[ "${REMOTE_SHA}" != "${LOCAL_SHA}" ]]; then
    echo "ERROR: checksum mismatch after upload: local ${LOCAL_SHA}, remote ${REMOTE_SHA}"
    exit 4
fi
echo "OK: uploaded and verified ${TARGET_URL}"
echo "Use in a DT: spark_package: ${PACKAGE}"
exit 0

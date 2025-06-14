#!/usr/bin/env bash

#==============================================================================
# Setup
#==============================================================================
set -e -x
trap 'handle_error' ERR

handle_error() {
    set +e +x
    echo "An error occurred. Cleaning up and exiting..."
    exit 1
}

function banner {
    set +x
    echo "+----------------------------------------------------"
    echo "| $1"
    echo "+----------------------------------------------------"
    set -x
}

#==============================================================================
# Configuration Variables (SET THESE!)
#==============================================================================
: "${VERSION:?ERROR: VERSION is not set (e.g., 3.5.5)}"
: "${IMG_NAME:?ERROR: IMG_NAME is not set (e.g., my-spark-image)}"
: "${DOCKER_IMG_ROOT:?ERROR: DOCKER_IMG_ROOT is not set (e.g., docker.io/myuser/...)}"
PROJ_HOME="/Users/david.english/workspace/spark" # Your Spark source directory

# Define the base Dockerfile to fetch from apache/spark-docker (for entrypoint.sh etc.)
SPARK_DOCKER_ORG="https://github.com/apache/spark-docker"
SPARK_DOCKER_SUBDIR="master/${VERSION}/scala2.12-java17-ubuntu" # Common subdirectory for Dockerfile and entrypoint

# Location of your Dockerfile template (create this file as described above)
DOCKERFILE_TEMPLATE_PATH="${PROJ_HOME}/docker_templates/Dockerfile.k8sOnly"

# Temporary directory for Docker build context
DOCKER_BUILD_DIR="${PROJ_HOME}/docker_build_temp"


#==============================================================================
# Body
#==============================================================================
banner "Starting Spark Custom Image Build for $VERSION"

if [[ "$(pwd)" != "$PROJ_HOME" ]]; then
  echo "Error: You must run this script from $PROJ_HOME"
  exit 1
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" = "master" ]; then
  echo "Error: Cannot run this script on the 'master' branch. Please switch to a different branch."
  exit 1
fi

# 1. Build Spark distribution LOCALLY
banner "Building custom Spark distribution..."
./dev/make-distribution.sh --name "$IMG_NAME-$VERSION" \
--tgz \
-Pkubernetes

SPARK_DIST_TGZ="$PROJ_HOME/spark-$VERSION-bin-$IMG_NAME-$VERSION.tgz"
SPARK_TGZ_FILENAME=$(basename "$SPARK_DIST_TGZ") # Get just the filename

banner "Setting up temporary Docker build directory: $DOCKER_BUILD_DIR"
rm -rf "$DOCKER_BUILD_DIR" # Clean up previous build dir
mkdir -p "$DOCKER_BUILD_DIR"

# 2. Copy your pre-modified Dockerfile template to the build directory
banner "Copying pre-modified Dockerfile template..."
cp "$DOCKERFILE_TEMPLATE_PATH" "${DOCKER_BUILD_DIR}/Dockerfile"

# 3. Fetch necessary supporting files (like entrypoint.sh) from apache/spark-docker
banner "Fetching official Spark entrypoint.sh and other supporting files..."
curl -sSL "${SPARK_DOCKER_ORG}/raw/${SPARK_DOCKER_SUBDIR}/entrypoint.sh" -o "${DOCKER_BUILD_DIR}/entrypoint.sh"


# 4. Copy your custom-built TGZ into the Docker build context
banner "Copying custom Spark TGZ into Docker build context..."
cp "$SPARK_DIST_TGZ" "$DOCKER_BUILD_DIR/"

# 5. Build the Docker image
banner "Building Docker image..."
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg spark_uid=185 \
  --build-arg java_image_tag=17-jammy \
  --build-arg SPARK_TGZ_FILENAME="$SPARK_TGZ_FILENAME" \
  -t "$DOCKER_IMG_ROOT/$IMG_NAME:$VERSION" \
  --push "$DOCKER_BUILD_DIR" # Set the build context to your new directory

#==============================================================================
# Cleanup
#==============================================================================
banner "Cleaning up temporary Docker build directory..."
rm -rf "$DOCKER_BUILD_DIR" # Clean up the temporary directory

set +e +x
banner 'Script completed successfully'

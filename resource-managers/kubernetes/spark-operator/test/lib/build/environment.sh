#!/bin/bash

# This script holds library functions for setting up the Docker container build environment

# os::build::environment::create creates a docker container with the default variables.
# arguments are passed directly to the container, OS_BUILD_ENV_GOLANG, OS_BUILD_ENV_IMAGE,
# and OS_RELEASE_DOCKER_ARGS can be used to customize the container. The docker socket
# is mounted by default and the output of the command is the container id.
function os::build::environment::create() {
  set -o errexit
  local release_image="${OS_BUILD_ENV_IMAGE}"
  local additional_context="${OS_BUILD_ENV_DOCKER_ARGS:-}"
  if [[ "${OS_BUILD_ENV_USE_DOCKER:-y}" == "y" ]]; then
    additional_context+=" --privileged -v /var/run/docker.sock:/var/run/docker.sock"

    if [[ "${OS_BUILD_ENV_LOCAL_DOCKER:-n}" == "y" ]]; then
      # if OS_BUILD_ENV_LOCAL_DOCKER==y, add the local OS_ROOT as the bind mount to the working dir
      # and set the running user to the current user
      local workingdir
      workingdir=$( os::build::environment::release::workingdir )
      additional_context+=" -v ${OS_ROOT}:${workingdir} -u $(id -u)"
    elif [[ -n "${OS_BUILD_ENV_VOLUME:-}" ]]; then
      if docker volume inspect "${OS_BUILD_ENV_VOLUME}" >/dev/null 2>&1; then
        os::log::debug "Re-using volume ${OS_BUILD_ENV_VOLUME}"
      else
        # if OS_BUILD_ENV_VOLUME is set and no volume already exists, create a docker volume to
        # store the working output so successive iterations can reuse shared code.
        os::log::debug "Creating volume ${OS_BUILD_ENV_VOLUME}"
        docker volume create --name "${OS_BUILD_ENV_VOLUME}" > /dev/null
      fi
      local workingdir
      workingdir=$( os::build::environment::release::workingdir )
      additional_context+=" -v ${OS_BUILD_ENV_VOLUME}:${workingdir}"
    fi
  fi

  if [[ -n "${OS_BUILD_ENV_FROM_ARCHIVE-}" ]]; then
    additional_context+=" -e OS_VERSION_FILE=/tmp/os-version-defs"
  else
    additional_context+=" -e OS_VERSION_FILE="
  fi

  declare -a cmd=( )
  declare -a env=( )
  local prefix=1
  for arg in "${@:1}"; do
    if [[ "${arg}" != *"="* ]]; then
      prefix=0
    fi
    if [[ "${prefix}" -eq 1 ]]; then
      env+=( "-e" "${arg}" )
    else
      cmd+=( "${arg}" )
    fi
  done
  if [[ -t 0 ]]; then
    if [[ "${#cmd[@]}" -eq 0 ]]; then
      cmd=( "/bin/sh" )
    fi
    if [[ "${cmd[0]}" == "/bin/sh" || "${cmd[0]}" == "/bin/bash" ]]; then
      additional_context+=" -it"
    fi
  fi

  # Create a new container from the release environment
  os::log::debug "Creating container: \`docker create ${additional_context} ${env[@]+"${env[@]}"} ${release_image} ${cmd[@]+"${cmd[@]}"}"
  docker create ${additional_context} "${env[@]+"${env[@]}"}" "${release_image}" "${cmd[@]+"${cmd[@]}"}"
}
readonly -f os::build::environment::create

# os::build::environment::release::workingdir calculates the working directory for the current
# release image.
function os::build::environment::release::workingdir() {
  set -o errexit
  # get working directory
  local container
  container="$(docker create "${release_image}")"
  local workingdir
  workingdir="$(docker inspect -f '{{ index . "Config" "WorkingDir" }}' "${container}")"
  docker rm "${container}" > /dev/null
  echo "${workingdir}"
}
readonly -f os::build::environment::release::workingdir

# os::build::environment::cleanup stops and removes the container named in the argument
# (unless OS_BUILD_ENV_LEAVE_CONTAINER is set, in which case it will only stop the container).
function os::build::environment::cleanup() {
  local container=$1
  os::log::debug "Stopping container ${container}"
  docker stop --time=0 "${container}" > /dev/null || true
  if [[ -z "${OS_BUILD_ENV_LEAVE_CONTAINER:-}" ]]; then
    os::log::debug "Removing container ${container}"
    docker rm "${container}" > /dev/null
  fi
}
readonly -f os::build::environment::cleanup

# os::build::environment::start starts the container provided as the first argument
# using whatever content exists in the container already.
function os::build::environment::start() {
  local container=$1

  os::log::debug "Starting container ${container}"
  if [[ "$( docker inspect --type container -f '{{ .Config.OpenStdin }}' "${container}" )" == "true" ]]; then
    docker start -ia "${container}"
  else
    docker start "${container}" > /dev/null
    os::log::debug "Following container logs"
    docker logs -f "${container}"
  fi

  local exitcode
  exitcode="$( docker inspect --type container -f '{{ .State.ExitCode }}' "${container}" )"

  os::log::debug "Container exited with ${exitcode}"

  # extract content from the image
  if [[ -n "${OS_BUILD_ENV_PRESERVE-}" ]]; then
    local workingdir
    workingdir="$(docker inspect -f '{{ index . "Config" "WorkingDir" }}' "${container}")"
    local oldIFS="${IFS}"
    IFS=:
    for path in ${OS_BUILD_ENV_PRESERVE}; do
      local parent=.
      if [[ "${path}" != "." ]]; then
        parent="$( dirname "${path}" )"
        mkdir -p "${parent}"
      fi
      os::log::debug "Copying from ${container}:${workingdir}/${path} to ${parent}"
      if ! output="$( docker cp "${container}:${workingdir}/${path}" "${parent}" 2>&1 )"; then
        os::log::warn "Copying ${path} from the container failed!"
        os::log::warn "${output}"
      fi
    done
    IFS="${oldIFS}"
  fi
  return "${exitcode}"
}
readonly -f os::build::environment::start

# os::build::environment::withsource starts the container provided as the first argument
# after copying in the contents of the current Git repository at HEAD (or, if specified,
# the ref specified in the second argument).
function os::build::environment::withsource() {
  local container=$1
  local commit=${2:-HEAD}

  if [[ -n "${OS_BUILD_ENV_LOCAL_DOCKER-}" ]]; then
    os::build::environment::start "${container}"
    return
  fi

  local workingdir
  workingdir="$(docker inspect -f '{{ index . "Config" "WorkingDir" }}' "${container}")"

  if [[ -n "${OS_BUILD_ENV_FROM_ARCHIVE-}" ]]; then
    # Generate version definitions. Tree state is clean because we are pulling from git directly.
    OS_GIT_TREE_STATE=clean os::build::get_version_vars
    os::build::save_version_vars "/tmp/os-version-defs"

    os::log::debug "Generating source code archive"
    tar -cf - -C /tmp/ os-version-defs | docker cp - "${container}:/tmp"
    git archive --format=tar "${commit}" | docker cp - "${container}:${workingdir}"
    os::build::environment::start "${container}"
    return
  fi

  local excluded=()
  local oldIFS="${IFS}"
  IFS=:
  for exclude in ${OS_BUILD_ENV_EXCLUDE:-_output}; do
    excluded+=("--exclude=${exclude}")
  done
  IFS="${oldIFS}"
  if which rsync &>/dev/null && [[ -n "${OS_BUILD_ENV_VOLUME-}" ]]; then
    os::log::debug "Syncing source using \`rsync\`"
    if ! rsync -a --blocking-io "${excluded[@]}" --delete --omit-dir-times --numeric-ids -e "docker run --rm -i -v \"${OS_BUILD_ENV_VOLUME}:${workingdir}\" --entrypoint=/bin/bash \"${OS_BUILD_ENV_IMAGE}\" -c '\$@'" . remote:"${workingdir}"; then
      os::log::debug "Falling back to \`tar\` and \`docker cp\` as \`rsync\` is not in container"
      tar -cf - "${excluded[@]}" . | docker cp - "${container}:${workingdir}"
    fi
  else
    os::log::debug "Syncing source using \`tar\` and \`docker cp\`"
    tar -cf - "${excluded[@]}" . | docker cp - "${container}:${workingdir}"
  fi

  os::build::environment::start "${container}"
}
readonly -f os::build::environment::withsource

# os::build::environment::run launches the container with the provided arguments and
# the current commit (defaults to HEAD). The container is automatically cleaned up.
function os::build::environment::run() {
  local commit="${OS_GIT_COMMIT:-HEAD}"
  local volume="${OS_BUILD_ENV_REUSE_VOLUME:-}"
  if [[ -z "${volume}" ]]; then
    volume="origin-build-$( git rev-parse "${commit}" )"
  fi
  volume="$( echo "${volume}" | tr '[:upper:]' '[:lower:]' )"
  export OS_BUILD_ENV_VOLUME="${volume}"

  if [[ -n "${OS_BUILD_ENV_VOLUME_FORCE_NEW:-}" ]]; then
    if docker volume inspect "${volume}" >/dev/null 2>&1; then
      os::log::debug "Removing volume ${volume}"
      docker volume rm "${volume}"
    fi
  fi

  os::log::debug "Using commit ${commit}"
  os::log::debug "Using volume ${volume}"

  local container
  container="$( os::build::environment::create "$@" )"
  trap "os::build::environment::cleanup ${container}" EXIT

  os::log::debug "Using container ${container}"

  os::build::environment::withsource "${container}" "${commit}"
}
readonly -f os::build::environment::run
#!/usr/bin/env bash

install_scala_for_linux() {
  # Determine the current working directory
  local dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  local scala_url="http://downloads.typesafe.com/scala/2.11.4/scala-2.11.4.tgz"
  local scala_loc="${dir}/../scala-2.11.4.tgz"
  local scala_bin="${dir}/../scala-2.11.4/bin/scala"

  if [ ! -f "${scala_bin}" ]; then
    # check if we already have the tarball; check if we have curl installed; download `scala`
    [ ! -f "${scala_loc}" ] && [ -n "`which curl 2>/dev/null`" ] && curl "${scala_url}" > "${scala_loc}"
    # if the `scala` file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${scala_loc}" ] && [ -n "`which wget 2>/dev/null`" ] && wget -O "${scala_loc}" "${scala_url}"
    # if both weren't successful, exit
    [ ! -f "${scala_loc}" ] && \
      echo "ERROR: Cannot find or download a version of Maven, please install manually and try again." && \
      exit 2
    cd "${dir}/.." && tar -xzf "${scala_loc}"
    rm -rf "${scala_loc}"
  fi
  export SCALA_HOME="$(dirname ${scala_bin})/.."
}

install_scala_for_osx() {
  brew install scala
  export SCALA_HOME="$(dirname "`brew --prefix`/Cellar/$(readlink `which scala`)")/../libexec"
}

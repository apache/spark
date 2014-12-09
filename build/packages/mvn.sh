#!/usr/bin/env bash

install_mvn_for_linux() {
  # Determine the current working directory
  local dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  local mvn_url="http://apache.claz.org/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz"
  local mvn_loc="${dir}/../apache-maven-3.2.3-bin.tar.gz"
  local mvn_bin="${dir}/../apache-maven-3.2.3/bin/mvn"

  if [ ! -f "${mvn_bin}" ]; then
    # check if we already have the tarball; check if we have curl installed; download `mvn`
    [ ! -f "${mvn_loc}" ] && [ -n "`which curl 2>/dev/null`" ] && curl "${mvn_url}" > "${mvn_loc}"
    # if the `mvn` file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${mvn_loc}" ] && [ -n "`which wget 2>/dev/null`" ] && wget -O "${mvn_loc}" "${mvn_url}"
    # if both weren't successful, exit
    [ ! -f "${mvn_loc}" ] && \
      echo "ERROR: Cannot find or download a version of Maven, please install manually and try again." && \
      exit 2
    cd "${dir}/.." && tar -xzf "${mvn_loc}"
    rm -rf "${mvn_loc}"
  fi
  export MVN_BIN="${mvn_bin}"
}

install_mvn_for_osx() {
  brew install maven
  export MVN_BIN=`which mvn`
}

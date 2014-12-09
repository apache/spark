#!/usr/bin/env bash

# Determine the current working directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MVN_URL="http://apache.claz.org/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz"
MVN_LOC="${DIR}/../apache-maven-3.2.3-bin.tar.gz"

install_mvn_for_linux() {
  local mvn_bin="${DIR}/../apache-maven-3.2.3/bin/mvn"

  if [ ! -f "${mvn_bin}" ]; then
    # first check if we have curl installed and, if so, download Leiningen
    [ -n "`which curl 2>/dev/null`" ] && curl "${MVN_URL}" > "${MVN_LOC}"
    # if the `lein` file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${MVN_LOC}" ] && [ -n "`which wget 2>/dev/null`" ] && wget -O "${MVN_LOC}" "${MVN_URL}"
    # if both weren't successful, exit
    [ ! -f "${MVN_LOC}" ] && \
      echo "ERROR: Cannot find or download a version of Maven, please install manually and try again." && \
      exit 2
    cd "${DIR}/.." && tar -xzf "${MVN_LOC}"
  fi
  export MVN_BIN="${mvn_bin}"
}

install_mvn_for_osx() {
  brew install maven
  export MVN_BIN=`which mvn`
}


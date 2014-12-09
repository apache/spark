#!/usr/bin/env bash

# Determine the current working directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SCALA_URL="http://downloads.typesafe.com/scala/2.11.4/scala-2.11.4.tgz"
SCALA_LOC="${DIR}/../scala/scala-2.11.4.tgz"

install_scala_for_linux() {
  # first check if we have curl installed and, if so, download Leiningen
  [ -n "`which curl 2>/dev/null`" ] && curl "${SCALA_URL}" > "${SCALA_LOC}"
  # if the `lein` file still doesn't exist, lets try `wget` and cross our fingers
  [ ! -f "${SCALA_LOC}" ] && [ -n "`which wget 2>/dev/null`" ] && wget -O "${SCALA_LOC}" "${SCALA_URL}"
  # if both weren't successful, exit
  [ ! -f "${SCALA_LOC}" ] && \
    echo "ERROR: Cannot find or download a version of Scala, please install manually and try again." && \
    exit 2
}

install_scala_for_osx() {
  brew install scala
  export SCALA_BIN=`which scala`
}

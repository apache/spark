#!/usr/bin/env bash

install_zinc_for_linux() {
  # Determine the current working directory
  local dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  local zinc_url="http://downloads.typesafe.com/zinc/0.3.5.3/zinc-0.3.5.3.tgz"
  local zinc_loc="${dir}/../zinc-0.3.5.3.tgz"
  local zinc_bin="${dir}/../zinc-0.3.5.3/bin/zinc"

  if [ ! -f "${zinc_bin}" ]; then
    # check if we already have the tarball; check if we have curl installed; download `zinc`
    [ ! -f "${zinc_loc}" ] && [ -n "`which curl 2>/dev/null`" ] && curl "${zinc_url}" > "${zinc_loc}"
    # if the `zinc` file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${zinc_loc}" ] && [ -n "`which wget 2>/dev/null`" ] && wget -O "${zinc_loc}" "${zinc_url}"
    # if both weren't successful, exit
    [ ! -f "${zinc_loc}" ] && \
      echo "ERROR: Cannot find or download a version of Maven, please install manually and try again." && \
      exit 2
    cd "${dir}/.." && tar -xzf "${zinc_loc}"
    rm -rf "${zinc_loc}"
  fi
  export ZINC_BIN="${zinc_bin}"
}

install_zinc_for_osx() {
  brew install zinc
  export ZINC_BIN=`which zinc`
}

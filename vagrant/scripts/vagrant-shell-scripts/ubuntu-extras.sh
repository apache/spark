#!/usr/bin/env bash

set -e

# {{{ Archives

archive-file-unpack() {
  log-operation "$FUNCNAME" "$@"
  if [ $# -lt 1 ]; then
    echo 'Usage: '"$0"' FILE [DIRECTORY]' >/dev/stderr
    return 1
  fi
  local file
  local directory
  local command
  local dependency
  file="$( readlink -f "$1" )"
  directory="${2:-.}"
  case "$file" in
    *.tar.gz  | *.tgz  ) command='tar -zxf'   ;;
    *.tar.bz2 | *.tbz2 ) command='tar -jxf'   ;;
    *.tar.xz           ) command='tar -Jxf'   ;;
    *.tar              ) command='tar -xf'    ;;
    *.zip              ) command='unzip -q'   ; dependency='unzip' ;;
    *.7z               ) command='7za x'      ;;
    *.gzip  | *.gz     ) command='gunzip -q'  ;;
    *.bzip2 | *.bz2    ) command='bunzip2 -q' ; dependency='bzip2' ;;
    * )
      echo "$0: Unsupported file format: $file" >/dev/stderr ;
      return 2
      ;;
  esac
  if [ ! -d "$directory" ]; then
    mkdir -p "$directory"
  fi
  if [ -n "$dependency" ]; then
    dependency-install "$dependency"
  fi
  ( cd "$directory" && eval $command \"\$file\" )
}

# }}}

# {{{ Packages (e.g., ZIP distributions)

package-ignore-preserve() {
  local package_name="$1" package_path="$2" preserved_path="$3"
  if [ -f "${package_path}/.gitignore" ]; then
    for preserve in `cat "${package_path}/.gitignore" | grep '^!' | grep -v '/$' | sed -e 's#^!##g'`; do
      eval "rm -f \"${preserved_path}/${package_name}-\"*\"/\"${preserve}"
    done
  fi
}

package-uri-download() {
  local uri
  local target
  dependency-install 'curl'
  uri="$1"
  shift
  if [ -z "$1" ]; then
    target="-s"
  else
    target="-o $1"
    shift
  fi
  curl '-#' --insecure -L "$@" $target "$uri"
}

package-uri-install() {
  local package_name
  local package_uri
  local package_path
  local package_version
  local package_index
  package_name="$1"
  package_uri="$2"
  package_index="$3"
  package_path="$4"
  package_version="$5"
  if [ -z "$package_path" ]; then
    package_path="$( echo "$package_name" | tr '-' '_' | tr 'a-z' 'A-Z' )_PATH"
    package_path="${!package_path}"
  fi
  if [ -z "$package_version" ]; then
    package_version="$( echo "$package_name" | tr '-' '_' | tr 'a-z' 'A-Z' )_VERSION"
    package_version="${!package_version}"
  fi
  if [ -z "$package_index" ]; then
    package_index="${package_path}/bin/${package_name}"
  fi
  for variable_name in 'package_uri' 'package_index'; do
    eval "$variable_name"=\""$( \
        echo "${!variable_name}" | \
        sed -e 's#%name#'"$package_name"'#g' | \
        sed -e 's#%version#'"$package_version"'#g' |
        sed -e 's#%path#'"$package_path"'#g' \
      )"\"
  done
  log-operation "$FUNCNAME" \
    "$package_name" \
    "$package_uri" \
    "$package_index" \
    "$package_path" \
    "$package_version"
  if [ ! -f "$package_index" ]; then
    dependency-install 'rsync'
    TMP_PATH="$( mktemp -d -t "${package_name}-XXXXXXXX" )"
    TMP_FILE="$TMP_PATH/$( basename "$package_uri" )"
    package-uri-download "$package_uri" "$TMP_FILE" -f || return $?
    archive-file-unpack "$TMP_FILE" "$TMP_PATH"
    rm -f "$TMP_FILE"
    package-ignore-preserve "$package_name" "$package_path" "$TMP_PATH"
    rsync -rlptDP "$TMP_PATH/${package_name}-"*'/' "${package_path}/"
    rm -Rf "$TMP_PATH"
  fi
}

# }}}

# {{{ Temporary Files

temporary-cleanup() {
  local exit_code=$? tmp_path
  for tmp_path in "$@"; do
    if [ -d "$tmp_path" ]; then
      rm -Rf "$tmp_path"
    fi
  done
  exit $exit_code
}

# }}}

# {{{ Dependency Management

# Create associations for packages we are going to install.
dependency-package-associate 'unzip' 'unzip'
dependency-package-associate 'bzip2' 'bzip2'
dependency-package-associate 'rsync' 'rsync'

# }}}

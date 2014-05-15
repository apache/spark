#!/usr/bin/env bash

## Glob for a pattern.  Return value is array in $_RET.
function glob() {
  local pat="$1"
  shopt -s nullglob
  unset _RET
  _RET=( $pat )
}

## Glob for a pattern, and die if there's more than one match.  Return
## value is array in $_RET.
function one_glob() {
  glob "$1"

  if (( ${#_RET[@]} > 1 )); then
    echo "Found multiple files matching $1" >&2
    echo "Please remove all but one." >&2
    exit 1
  fi
}

## Glob for a pattern, and die if there's not exactly one match.
## Return value is string (not array) in $_RET.
function need_one_glob() {
  local pat="$1"
  local errtext="$2"
  one_glob "$pat"
  local files=(${_RET[@]})
  unset _RET

  if (( ${#files[@]} == 0 )); then
    echo "No files found matching $pat" >&2
    echo $errtext >&2
    exit 1
  fi

  _RET=${files[0]}
}

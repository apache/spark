#!/usr/bin/env bash

function glob() {
  local pat="$1"
  shopt -s nullglob
  _RET=( $pat )
}

function one_glob() {
  glob "$1"
  local files=$_RET

  if (( ${#files[@]} > 1 )); then
    echo "Found multiple files matching $1" >&2
    echo "Please remove all but one." >&2
    exit 1
  fi

  _RET=$files
}

function need_one_glob() {
  local pat="$1"
  local errtext="$2"
  one_glob "$pat"
  local files=$_RET

  if (( ${#files[@]} == 0 )); then
    echo "No files found matching $pat" >&2
    echo $errtext >&2
    exit 1
  fi

  _RET=${files[0]}
}

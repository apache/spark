#!/usr/bin/env bash
#
##############################################################################
# Copyright 2002-2013 LAMP/EPFL
#
# This is free software; see the distribution for copying conditions.
# There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE.
##############################################################################

findScalaHome () {
  # see SI-2092 and SI-5792
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ] ; do
    local linked="$(readlink "$source")"
    local dir="$( cd -P $(dirname "$source") && cd -P $(dirname "$linked") && pwd )"
    source="$dir/$(basename "$linked")"
  done
  ( cd -P "$(dirname "$source")/.." && pwd )
}
execCommand () {
  [[ -n $SCALA_RUNNER_DEBUG ]] && echo "" && for arg in "$@"; do echo "$arg"; done && echo "";
  "$@"
}

# Not sure what the right default is here: trying nonzero.
scala_exit_status=127
saved_stty=""

# restore stty settings (echo in particular)
function restoreSttySettings() {
  if [[ -n $SCALA_RUNNER_DEBUG ]]; then
    echo "restoring stty:"
    echo "$saved_stty"
  fi
    
  stty $saved_stty
  saved_stty=""
}

function onExit() {
  [[ "$saved_stty" != "" ]] && restoreSttySettings
  exit $scala_exit_status
}

# to reenable echo if we are interrupted before completing.
trap onExit INT

# save terminal settings
saved_stty=$(stty -g 2>/dev/null)
# clear on error so we don't later try to restore them
if [[ ! $? ]]; then  
  saved_stty=""
fi
if [[ -n $SCALA_RUNNER_DEBUG ]]; then
  echo "saved stty:"
  echo "$saved_stty"
fi

unset cygwin
if uname | grep -q ^CYGWIN; then
  cygwin="$(uname)"
fi

unset mingw
if uname | grep -q ^MINGW; then
  mingw="$(uname)"
fi

# Finding the root folder for this Scala distribution
SCALA_HOME="$(findScalaHome)"
SEP=":"

# Possible additional command line options
WINDOWS_OPT=""
EMACS_OPT=""
[[ -n "$EMACS" ]] && EMACS_OPT="-Denv.emacs=$EMACS"

# Remove spaces from SCALA_HOME on windows
if [[ -n "$cygwin" ]]; then
  SCALA_HOME="$(shome="$(cygpath --windows --short-name "$SCALA_HOME")" ; cygpath --unix "$shome")"
# elif uname |grep -q ^MINGW; then
#   SEP=";"
fi

# Constructing the extension classpath
TOOL_CLASSPATH=""
if [[ -z "$TOOL_CLASSPATH" ]]; then
    for ext in "$SCALA_HOME"/lib/* ; do
        if [[ -z "$TOOL_CLASSPATH" ]]; then
            TOOL_CLASSPATH="$ext"
        else
            TOOL_CLASSPATH="${TOOL_CLASSPATH}${SEP}${ext}"
        fi
    done
fi

if [[ -n "$cygwin" ]]; then
    if [[ "$OS" = "Windows_NT" ]] && cygpath -m .>/dev/null 2>/dev/null ; then
        format=mixed
    else
        format=windows
    fi
    SCALA_HOME="$(cygpath --$format "$SCALA_HOME")"
    TOOL_CLASSPATH="$(cygpath --path --$format "$TOOL_CLASSPATH")"
elif [[ -n "$mingw" ]]; then
    SCALA_HOME="$(cmd //c echo "$SCALA_HOME")"
    TOOL_CLASSPATH="$(cmd //c echo "$TOOL_CLASSPATH")"
fi

if [[ -n "$cygwin$mingw" ]]; then
    case "$TERM" in
        rxvt* | xterm*)
            stty -icanon min 1 -echo
            WINDOWS_OPT="-Djline.terminal=scala.tools.jline.UnixTerminal"
        ;;
    esac
fi

[[ -n "$JAVA_OPTS" ]] || JAVA_OPTS="-Xmx256M -Xms32M"

# break out -D and -J options and add them to JAVA_OPTS as well
# so they reach the underlying JVM in time to do some good.  The
# -D options will be available as system properties.
declare -a java_args
declare -a scala_args

# default to the boot classpath for speed, except on cygwin/mingw because
# JLine on Windows requires a custom DLL to be loaded.
unset usebootcp
if [[ -z "$cygwin$mingw" ]]; then
  usebootcp="true"
fi

# If using the boot classpath, also pass an empty classpath
# to java to suppress "." from materializing.
classpathArgs () {
  if [[ -n $usebootcp ]]; then
    echo "-Xbootclasspath/a:$TOOL_CLASSPATH -classpath \"\""
  else
    echo "-classpath $TOOL_CLASSPATH"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -D*)
      # pass to scala as well: otherwise we lose it sometimes when we
      # need it, e.g. communicating with a server compiler.
      java_args=("${java_args[@]}" "$1")
      scala_args=("${scala_args[@]}" "$1")
      shift
      ;;
    -J*)
      # as with -D, pass to scala even though it will almost
      # never be used.
      java_args=("${java_args[@]}" "${1:2}")
      scala_args=("${scala_args[@]}" "$1")
      shift
      ;;
    -toolcp)
      TOOL_CLASSPATH="${TOOL_CLASSPATH}${SEP}${2}"
      shift 2
      ;;
    -nobootcp)
      unset usebootcp
      shift
      ;;
    -usebootcp)
      usebootcp="true"
      shift
      ;;
    -debug)
      SCALA_RUNNER_DEBUG=1
      shift
      ;;
    *)
      scala_args=("${scala_args[@]}" "$1")
      shift
      ;;
  esac
done

# reset "$@" to the remaining args
set -- "${scala_args[@]}"

if [[ -z "$JAVACMD" && -n "$JAVA_HOME" && -x "$JAVA_HOME/bin/java" ]]; then
    JAVACMD="$JAVA_HOME/bin/java"
fi

# note that variables which may intentionally be empty must not
# be quoted: otherwise an empty string will appear as a command line
# argument, and java will think that is the program to run.
execCommand \
  "${JAVACMD:=java}" \
  $JAVA_OPTS \
  "${java_args[@]}" \
  $(classpathArgs) \
  -Dscala.home="$SCALA_HOME" \
  -Dscala.usejavacp=true \
  $EMACS_OPT \
  $WINDOWS_OPT \
   scala.tools.nsc.Main  "$@"

# record the exit status lest it be overwritten:
# then reenable echo and propagate the code.
scala_exit_status=$?
onExit

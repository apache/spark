#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Provides tab completion for the main hadoop script.
#
# On debian-based systems, place in /etc/bash_completion.d/ and either restart
# Bash or source the script manually (. /etc/bash_completion.d/hadoop.sh).

_hadoop() {
  local script cur prev temp

  COMPREPLY=()
  cur=${COMP_WORDS[COMP_CWORD]}
  prev=${COMP_WORDS[COMP_CWORD-1]}  
  script=${COMP_WORDS[0]}  
  
  # Bash lets you tab complete things even if the script doesn't
  # exist (or isn't executable). Check to make sure it is, as we
  # need to execute it to get options/info
  if [ -f "$script" -a -x "$script" ]; then
    case $COMP_CWORD in
    1)
      # Completing the first argument (the command).

      temp=`$script | grep -n "^\s*or"`;
      temp=`$script | head -n $((${temp%%:*} - 1)) | awk '/^ / {print $1}' | sort | uniq`;
      COMPREPLY=(`compgen -W "${temp}" -- ${cur}`);
      return 0;;

    2)
      # Completing the second arg (first arg to the command)

      # The output of commands isn't hugely consistent, so certain
      # names are hardcoded and parsed differently. Some aren't
      # handled at all (mostly ones without args).
      case ${COMP_WORDS[1]} in
      dfs | dfsadmin | fs | job | pipes)
        # One option per line, enclosed in square brackets

        temp=`$script ${COMP_WORDS[1]} 2>&1 | awk '/^[ \t]*\[/ {gsub("[[\\]]", ""); print $1}'`;
        COMPREPLY=(`compgen -W "${temp}" -- ${cur}`);
        return 0;;

      jar)
        # Any (jar) file

        COMPREPLY=(`compgen -A file -- ${cur}`);
        return 0;;

      namenode)
        # All options specified in one line,
        # enclosed in [] and separated with |
        temp=`$script ${COMP_WORDS[1]} -help 2>&1 | grep Usage: | cut -d '[' -f 2- | awk '{gsub("] \\| \\[|]", " "); print $0}'`;
        COMPREPLY=(`compgen -W "${temp}" -- ${cur}`);
        return 0;;

      *)
        # Other commands - no idea

        return 1;;
      esac;;

    *)
      # Additional args
      
      case ${COMP_WORDS[1]} in
      dfs | fs)
        # DFS/FS subcommand completion
        # Pull the list of options, grep for the one the user is trying to use,
        # and then select the description of the relevant argument
        temp=$((${COMP_CWORD} - 1));
        temp=`$script ${COMP_WORDS[1]} 2>&1 | grep -- "${COMP_WORDS[2]} " | awk '{gsub("[[ \\]]", ""); print $0}' | cut -d '<' -f ${temp}`;

        if [ ${#temp} -lt 1 ]; then
          # No match
          return 1;
        fi;

        temp=${temp:0:$((${#temp} - 1))};

        # Now do completion based on the argument
        case $temp in
        path | src | dst)
          # DFS path completion
          temp=`$script ${COMP_WORDS[1]} -ls "${cur}*" 2>&1 | grep -vE '^Found ' | cut -f 1 | awk '{gsub("^.* ", ""); print $0;}'`
          COMPREPLY=(`compgen -W "${temp}" -- ${cur}`);
          return 0;;

        localsrc | localdst)
          # Local path completion
          COMPREPLY=(`compgen -A file -- ${cur}`);
          return 0;;

        *)
          # Other arguments - no idea
          return 1;;
        esac;;

      *)
        # Other subcommands - no idea
        return 1;;
      esac;
    esac;
  fi;
}

complete -F _hadoop hadoop

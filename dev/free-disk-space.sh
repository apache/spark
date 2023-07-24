#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# ======
# MACROS
# ======

# macro to print a line of equals
# (silly but works)
printSeparationLine() {
  str=${1:=}
  num=${2:-80}
  counter=1
  output=""
  while [ $counter -le $num ]
  do
     output="${output}${str}"
     counter=$((counter+1))
  done
  echo "${output}"
}

# macro to compute available space
# REF: https://unix.stackexchange.com/a/42049/60849
# REF: https://stackoverflow.com/a/450821/408734
getAvailableSpace() { echo $(df -a $1 | awk 'NR > 1 {avail+=$4} END {print avail}'); }

# macro to make Kb human readable (assume the input is Kb)
# REF: https://unix.stackexchange.com/a/44087/60849
formatByteCount() { echo $(numfmt --to=iec-i --suffix=B --padding=7 $1'000'); }

# macro to output saved space
printSavedSpace() {
  saved=${1}
  title=${2:-}

  echo ""
  printSeparationLine '*' 80
  if [ ! -z "${title}" ]; then
    echo "=> ${title}: Saved $(formatByteCount $saved)"
  else
    echo "=> Saved $(formatByteCount $saved)"
  fi
  printSeparationLine '*' 80
  echo ""
}

# macro to print output of dh with caption
printDH() {
  caption=${1:-}

  printSeparationLine '=' 80
  echo "${caption}"
  echo ""
  echo "$ dh -h /"
  echo ""
  df -h /
  echo "$ dh -a /"
  echo ""
  df -a /
  echo "$ dh -a"
  echo ""
  df -a
  printSeparationLine '=' 80
}



# ======
# SCRIPT
# ======

# Display initial disk space stats

AVAILABLE_INITIAL=$(getAvailableSpace)
AVAILABLE_ROOT_INITIAL=$(getAvailableSpace '/')

printDH "BEFORE CLEAN-UP:"
echo ""


# Option: Remove Android library
BEFORE=$(getAvailableSpace)

rm -rf /usr/local/lib/android

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Android library"

# Option: Remove .NET runtime
BEFORE=$(getAvailableSpace)
# https://github.community/t/bigger-github-hosted-runners-disk-space/17267/11

rm -rf /usr/share/dotnet

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED ".NET runtime"

# Option: Remove Haskell runtime
BEFORE=$(getAvailableSpace)

rm -rf /opt/ghc

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Haskell runtime"

# Option: Remove large packages
# REF: https://github.com/apache/flink/blob/master/tools/azure-pipelines/free_disk_space.sh
BEFORE=$(getAvailableSpace)

apt-get remove -y '^dotnet-.*'
apt-get remove -y '^llvm-.*'
apt-get remove -y 'php.*'
apt-get remove -y '^mongodb-.*'
apt-get remove -y '^mysql-.*'
apt-get remove -y azure-cli google-cloud-sdk google-chrome-stable firefox powershell mono-devel libgl1-mesa-dri
apt-get autoremove -y
apt-get clean

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Large misc. packages"

# Option: Remove Docker images
BEFORE=$(getAvailableSpace)

docker image prune --all --force

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Docker images"

# Option: Remove tool cache
# REF: https://github.com/actions/virtual-environments/issues/2875#issuecomment-1163392159

BEFORE=$(getAvailableSpace)

rm -rf "$AGENT_TOOLSDIRECTORY"

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Tool cache"

# Option: Remove Swap storage
BEFORE=$(getAvailableSpace)

swapoff -a
rm -f /mnt/swapfile
free -h

AFTER=$(getAvailableSpace)
SAVED=$((AFTER-BEFORE))
printSavedSpace $SAVED "Swap storage"

# Output saved space statistic

AVAILABLE_END=$(getAvailableSpace)
AVAILABLE_ROOT_END=$(getAvailableSpace '/')

echo ""
printDH "AFTER CLEAN-UP:"

echo ""
echo ""

echo "/dev/root:"
printSavedSpace $((AVAILABLE_ROOT_END - AVAILABLE_ROOT_INITIAL))
echo "overall:"
printSavedSpace $((AVAILABLE_END - AVAILABLE_INITIAL))
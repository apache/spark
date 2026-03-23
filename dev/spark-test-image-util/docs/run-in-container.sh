#!/bin/bash
#
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
#

# 1.Set env variable.
set -ex
_arch="$(uname -m)"
case "$_arch" in
  "aarch64") export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 ;;
  "x86_64") export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ;;
  *) echo "Unexpected arch $_arch picking first java-17-openjdk in /usr/lib/jvm";
     export JAVA_HOME=$(ls /usr/lib/jvm/java-17-openjdk-* | head -n 1);;
esac
export PATH="$HOME/.bin:$JAVA_HOME/bin:$PATH"
export SPARK_DOCS_IS_BUILT_ON_HOST=1
# We expect to compile the R document on the host.
export SKIP_RDOC=1
mkdir -p ~/.bin
mkdir -p ~/.gem

# 2.Install bundler.
gem install bundler -v 2.4.22 --install-dir ~/.gem --bindir ~/.bin
cd /__w/spark/spark/docs
bundle install

# 3.Build docs, includes: `error docs`, `scala doc`, `python doc`, `sql doc`, excludes: `r doc`.
# We need this link to make sure `python3` points to `python3.12` which contains the prerequisite packages.
ln -s "$(which python3.12)" ~/.bin/python3

# Build docs first with SKIP_API to ensure they are buildable without requiring any
# language docs to be built beforehand.
cd /__w/spark/spark/docs
bundle exec jekyll build

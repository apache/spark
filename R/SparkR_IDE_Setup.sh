#!/bin/sh

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

# Download RStudio
if [ ! -f rstudio-0.98.1091-x86_64.rpm ]; then
  wget  http://download1.rstudio.org/rstudio-0.98.1091-x86_64.rpm
fi

# Install using the rpm via yum

sudo yum install rstudio-0.98.1091-x86_64.rpm

rm rstudio-0.98.1091-x86_64.rpm

# Add SparkR directory to .libPaths() in order to import SparkR into an Rstudio session

cat >> $HOME/.Rprofile <<EOT
lib_path <- .libPaths()

lib_path <- c(lib_path,"/home/cloudera/SparkR-pkg/lib")

.libPaths(lib_path)

rm(lib_path)
EOT


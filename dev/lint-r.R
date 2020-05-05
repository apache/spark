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

argv <- commandArgs(TRUE)
SPARK_ROOT_DIR <- as.character(argv[1])
LOCAL_LIB_LOC <- file.path(SPARK_ROOT_DIR, "R", "lib")

# Checks if SparkR is installed in a local directory.
if (!requireNamespace("SparkR", lib.loc = LOCAL_LIB_LOC)) {
  stop("You should install SparkR in a local directory with `R/install-dev.sh`.")
}

# Installs lintr if needed
if (!requireNamespace("lintr")) {
  install.packages('lintr')
}

library(lintr)
library(methods)
library(testthat)
path.to.package <- file.path(SPARK_ROOT_DIR, "R", "pkg")
lint_package(path.to.package, cache = FALSE)

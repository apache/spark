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
if (! library(SparkR, lib.loc = LOCAL_LIB_LOC, logical.return = TRUE)) {
  stop("You should install SparkR in a local directory with `R/install-dev.sh`.")
}

# Installs lintr from Github in a local directory if lintr is not installed already or
# the existing lintr is not the specified version.
#
# Note that, the CRAN's version is too old to adapt to our rules. Therefore, we try to
# install lintr from the latest commit in Github, rather than a specific tag or release.
# The current latest is jimhester/lintr@5431140 (see SPARK-22063), the dev version,
# '1.0.1.9000'.
if ("lintr" %in% row.names(installed.packages()) == FALSE ||
    packageVersion("lintr") != "1.0.1.9000") {
  devtools::install_github("jimhester/lintr@5431140")
}

library(lintr)
library(methods)
library(testthat)
path.to.package <- file.path(SPARK_ROOT_DIR, "R", "pkg")
lint_package(path.to.package, cache = FALSE)

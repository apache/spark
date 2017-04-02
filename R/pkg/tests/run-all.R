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

library(testthat)
library(SparkR)

# Turn all warnings into errors
options("warn" = 2)

install.spark()

# Setup global test environment
sparkRDir <- file.path(Sys.getenv("SPARK_HOME"), "R")
sparkRFilesBefore <- list.files(path = sparkRDir, all.files = TRUE)
sparkRWhitelistSQLDirs <- c("spark-warehouse", "metastore_db")
invisible(lapply(sparkRWhitelistSQLDirs,
                 function(x) { unlink(file.path(sparkRDir, x), recursive = TRUE, force = TRUE)}))

test_package("SparkR")

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

.First <- function() {
  home <- Sys.getenv("SPARK_HOME")
  .libPaths(c(file.path(home, "R", "lib"), .libPaths()))
  Sys.setenv(NOAWT=1)

  library(utils)
  library(SparkR)
  sc <- sparkR.init(Sys.getenv("MASTER", unset = ""))
  assign("sc", sc, envir=.GlobalEnv)
  sqlCtx <- sparkRSQL.init(sc)
  assign("sqlCtx", sqlCtx, envir=.GlobalEnv)
  cat("\n Welcome to SparkR!")
  cat("\n Spark context is available as sc, SQL context is available as sqlCtx\n")
}

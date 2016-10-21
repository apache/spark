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
  Sys.setenv(NOAWT = 1)

  # Make sure SparkR package is the last loaded one
  old <- getOption("defaultPackages")
  options(defaultPackages = c(old, "SparkR"))

  spark <- SparkR::sparkR.session()
  assign("spark", spark, envir = .GlobalEnv)
  sc <- SparkR:::callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", spark)
  assign("sc", sc, envir = .GlobalEnv)
  sparkVer <- SparkR:::callJMethod(sc, "version")
  cat("\n Welcome to")
  cat("\n")
  cat("    ____              __", "\n")
  cat("   / __/__  ___ _____/ /__", "\n")
  cat("  _\\ \\/ _ \\/ _ `/ __/  '_/", "\n")
  cat(" /___/ .__/\\_,_/_/ /_/\\_\\")
  if (nchar(sparkVer) == 0) {
    cat("\n")
  } else {
    cat("   version ", sparkVer, "\n")
  }
  cat("    /_/", "\n")
  cat("\n")

  cat("\n SparkSession available as 'spark'.\n")
}

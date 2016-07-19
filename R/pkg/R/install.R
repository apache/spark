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

# Functions to install Spark in case the user directly downloads SparkR
# from CRAN.

#' Download and Install Spark to Local Directory
#' 
#' \code{install_spark} downloads and installs Spark to local directory if
#' it is not found. The Spark version we use is 2.0.0 (preview). Users can
#' specify a desired Hadoop version, the remote site, and the directory where
#' the package is installed locally.
#'
#' @param hadoop_version Version of Hadoop to install. 2.3, 2.4, 2.6,
#'        and 2.7 (default)
#' @param url the base URL of the repositories to use
#' @param local_dir local directory that Spark is installed to
#' @rdname install_spark
#' @name install_spark
#' @export
#' @examples
#'\dontrun{
#' install_spark()
#'}
#' @note install_spark since 2.1.0
install_spark <- function(hadoop_version = NULL, url = NULL, local_dir = NULL) {
  version <- paste0("spark-", spark_version_default())
  hadoop_version <- hadoop_version_default()
  packageName <- paste0(version, "-bin-hadoop", hadoop_version)
  if (is.null(local_dir)) {
    local_dir <- getOption("spark.install.dir", 
                           rappdirs::app_dir("spark"))$cache()
  }
  packageLocalDir <- file.path(local_dir, packageName)
  if (dir.exists(packageLocalDir)) {
    fmt <- "Spark %s for Hadoop %s has been installed."
    msg <- sprintf(fmt, version, hadoop_version)
    message(msg)
  } else {
    dir.create(packageLocalDir, recursive = TRUE)
    if (is.null(url)) {
      mirror_sites <- read.csv(mirror_csv_url())
      url <- mirror_sites$url[1]
    }
    packageRemotePath <- paste0(file.path(url, "spark", version, packageName),
                                ".tgz")
    fmt <- paste("Installing Spark %s for Hadoop %s.",
                 "Downloading from:\n %s",
                 "Installing to:\n %s", sep = "\n")
    msg <- sprintf(fmt, version, hadoop_version, packageRemotePath, 
                   packageLocalDir)
    message(msg)
    packageLocalPath <- paste0(packageLocalDir, ".tgz")
    download.file(packageRemotePath, packageLocalPath)
    untar(tarfile = packageLocalPath, exdir = local_dir)
    unlink(packageLocalPath)
  }
}

mirror_csv_url <- function() {
  system.file("extdata", "spark_download.csv", package = "SparkR")
}

spark_version_default <- function() {
  "2.0.0-preview"
}

hadoop_version_default <- function() {
  "2.7"
}

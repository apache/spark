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

#' Download and Install Spark Core to Local Directory
#' 
#' \code{install_spark} downloads and installs Spark to local directory if
#' it is not found. The Spark version we use is 2.0.0 (preview). Users can
#' specify a desired Hadoop version, the remote site, and the directory where
#' the package is installed locally.
#'
#' @param hadoop_version Version of Hadoop to install. 2.3, 2.4, 2.6,
#'        and 2.7 (default)
#' @param mirror_url the base URL of the repositories to use
#' @param local_dir local directory that Spark is installed to
#' @return \code{install_spark} returns the local directory 
#'         where Spark is found or installed
#' @rdname install_spark
#' @name install_spark
#' @export
#' @examples
#'\dontrun{
#' install_spark()
#'}
#' @note install_spark since 2.1.0
install_spark <- function(hadoop_version = NULL, mirror_url = NULL,
                          local_dir = NULL) {
  version <- paste0("spark-", spark_version_default())
  hadoop_version <- match.arg(hadoop_version, supported_versions_hadoop())
  packageName <- ifelse(hadoop_version == "without",
                        paste0(version, "-bin-without-hadoop"),
                        paste0(version, "-bin-hadoop", hadoop_version))
  if (is.null(local_dir)) {
    local_dir <- getOption("spark.install.dir", spark_cache_path())
  } else {
    local_dir <- normalizePath(local_dir)
  }

  packageLocalDir <- file.path(local_dir, packageName)

  if (dir.exists(packageLocalDir)) {
    fmt <- "Spark %s for Hadoop %s has been installed."
    msg <- sprintf(fmt, version, hadoop_version)
    message(msg)
    return(invisible(packageLocalDir))
  }

  packageLocalPath <- paste0(packageLocalDir, ".tgz")
  tarExists <- file.exists(packageLocalPath)

  if (tarExists) {
    message("Tar file found. Installing...")
  } else {
    dir.create(packageLocalDir, recursive = TRUE)
    if (is.null(mirror_url)) {
      message("Remote URL not provided. Use Apache default.")
      mirror_url <- mirror_url_default()
    }
    # This is temporary, should be removed when released
    version <- "spark-releases/spark-2.0.0-rc4-bin"
    packageRemotePath <- paste0(file.path(mirror_url, version, packageName),
                                ".tgz")
    fmt <- paste("Installing Spark %s for Hadoop %s.",
                 "Downloading from:\n- %s",
                 "Installing to:\n- %s", sep = "\n")
    msg <- sprintf(fmt, version, hadoop_version, packageRemotePath,
                   packageLocalDir)
    message(msg)

    fetchFail <- tryCatch(download.file(packageRemotePath, packageLocalPath),
                          error = function(e) {
                            msg <- paste0("Fetch failed from ", mirror_url, ".")
                            message(msg)
                            TRUE
                          })
    if (fetchFail) {
      message("Try the backup option.")
      mirror_sites <- tryCatch(read.csv(mirror_url_csv()),
                               error = function(e) stop("No csv file found."))
      mirror_url <- mirror_sites$url[1]
      packageRemotePath <- paste0(file.path(mirror_url, version, packageName),
                                  ".tgz")
      message(sprintf("Downloading from:\n- %s", packageRemotePath))
      tryCatch(download.file(packageRemotePath, packageLocalPath),
               error = function(e) {
                 stop("Download failed. Please provide a valid mirror_url.")
               })
    }
  }

  untar(tarfile = packageLocalPath, exdir = local_dir)
  if (!tarExists) {
    unlink(packageLocalPath)
  }
  message("Installation done.")
  invisible(packageLocalDir)
}

mirror_url_default <- function() {
  # change to http://www.apache.org/dyn/closer.lua
  # when released

  "http://people.apache.org/~pwendell"
}

supported_versions_hadoop <- function() {
  c("2.7", "2.6", "2.4", "without")
}

spark_cache_path <- function() {
  if (.Platform$OS.type == "windows") {
    winAppPath <- Sys.getenv("%LOCALAPPDATA%", unset = NA)
    if (is.null(winAppPath)) {
      msg <- paste("%LOCALAPPDATA% not found.",
                    "Please define or enter an installation path in loc_dir.")
      stop(msg)
    } else {
      path <- file.path(winAppPath, "spark", "spark", "Cache")
    }
  } else if (.Platform$OS.type == "unix") {
    if (Sys.info()["sysname"] == "Darwin") {
      path <- file.path("~/Library/Caches", "spark")
    } else {
      path <- file.path(Sys.getenv("XDG_CACHE_HOME", "~/.cache"), "spark")
    }
  } else {
    stop("Unknown OS")
  }
  normalizePath(path, mustWork = TRUE)
}

mirror_url_csv <- function() {
  system.file("extdata", "spark_download.csv", package = "SparkR")
}

spark_version_default <- function() {
  "2.0.0"
}

hadoop_version_default <- function() {
  "2.7"
}

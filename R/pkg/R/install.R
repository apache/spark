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

#' Download and Install Apache Spark to a Local Directory
#' 
#' \code{install.spark} downloads and installs Spark to local directory if
#' it is not found. The Spark version we use is the same as the SparkR version.
#' Users can specify a desired Hadoop version, the remote site, and
#' the directory where the package is installed locally.
#'
#' @param hadoopVersion Version of Hadoop to install. Default is without,
#'        Spark's "Hadoop free" build. See
#'        \href{http://spark.apache.org/docs/latest/hadoop-provided.html}{
#'        "Hadoop Free" Build} for more information.
#' @param mirrorUrl base URL of the repositories to use. The directory
#'        layout should follow
#'        \href{http://www.apache.org/dyn/closer.lua/spark/}{Apache mirrors}.
#' @param localDir a local directory where Spark is installed. Default path to
#'        the cache directory:
#'        \itemize{
#'          \item Mac OS X: \file{~/Library/Caches/spark}
#'          \item Unix: \env{$XDG_CACHE_HOME} if defined,
#'                otherwise \file{~/.cache/spark}
#'          \item Win XP: \file{C:\\Documents and Settings\\< username
#'                >\\Local Settings\\Application Data\\spark\\spark\\Cache}
#'          \item Win Vista: \file{C:\\Users\\< username
#'                >\\AppData\\Local\\spark\\spark\\Cache}
#'        }
#' @return \code{install.spark} returns the local directory
#'         where Spark is found or installed
#' @rdname install.spark
#' @name install.spark
#' @export
#' @examples
#'\dontrun{
#' install.spark()
#'}
#' @note install.spark since 2.1.0
#' @seealso See available Hadoop versions:
#' \href{http://spark.apache.org/downloads.html}{Apache Spark}
install.spark <- function(hadoopVersion = NULL, mirrorUrl = NULL,
                          localDir = NULL) {
  version <- paste0("spark-", packageVersion("SparkR"))
  hadoopVersion <- match.arg(hadoopVersion, supported_versions_hadoop())
  packageName <- ifelse(hadoopVersion == "without",
                        paste0(version, "-bin-without-hadoop"),
                        paste0(version, "-bin-hadoop", hadoopVersion))
  if (is.null(localDir)) {
    localDir <- getOption("spark.install.dir", spark_cache_path())
  } else {
    localDir <- normalizePath(localDir)
  }

  packageLocalDir <- file.path(localDir, packageName)


  # can use dir.exists(packageLocalDir) under R 3.2.0 or later
  if (!is.na(file.info(packageLocalDir)$isdir)) {
    fmt <- "Spark %s for Hadoop %s has been installed."
    msg <- sprintf(fmt, version, hadoopVersion)
    message(msg)
    return(invisible(packageLocalDir))
  }

  packageLocalPath <- paste0(packageLocalDir, ".tgz")
  tarExists <- file.exists(packageLocalPath)

  if (tarExists) {
    message("Tar file found. Installing...")
  } else {
    if (is.null(mirrorUrl)) {
      message("Remote URL not provided. Use Apache default.")
      mirrorUrl <- mirror_url_default()
    }

    version <- "spark-2.0.0-rc4-bin"
    # When 2.0 released, remove the above line and
    # change spark-releases to spark in the statement below
    packageRemotePath <- paste0(
      file.path(mirrorUrl, "spark-releases", version, packageName), ".tgz")
    fmt <- paste("Installing Spark %s for Hadoop %s.",
                 "Downloading from:\n- %s",
                 "Installing to:\n- %s", sep = "\n")
    msg <- sprintf(fmt, version, hadoopVersion, packageRemotePath,
                   packageLocalDir)
    message(msg)

    fetchFail <- tryCatch(download.file(packageRemotePath, packageLocalPath),
                          error = function(e) {
                            msg <- paste0("Fetch failed from ", mirrorUrl, ".")
                            message(msg)
                            TRUE
                          })
    if (fetchFail) {
      message("Try the backup option.")
      mirror_sites <- tryCatch(read.csv(mirror_url_csv()),
                               error = function(e) stop("No csv file found."))
      mirrorUrl <- mirror_sites$url[1]
      packageRemotePath <- paste0(file.path(mirrorUrl, version, packageName),
                                  ".tgz")
      message(sprintf("Downloading from:\n- %s", packageRemotePath))
      tryCatch(download.file(packageRemotePath, packageLocalPath),
               error = function(e) {
                 stop("Download failed. Please provide a valid mirrorUrl.")
               })
    }
  }

  untar(tarfile = packageLocalPath, exdir = localDir)
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
      path <- file.path(Sys.getenv("HOME"), "Library/Caches", "spark")
    } else {
      path <- file.path(
        Sys.getenv("XDG_CACHE_HOME", file.path(Sys.getenv("HOME"), ".cache")),
        "spark")
    }
  } else {
    stop("Unknown OS")
  }
  normalizePath(path, mustWork = FALSE)
}

mirror_url_csv <- function() {
  system.file("extdata", "spark_download.csv", package = "SparkR")
}

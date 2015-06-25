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

.sparkREnv <- new.env()

sparkR.onLoad <- function(libname, pkgname) {
  .sparkREnv$libname <- libname
}

# Utility function that returns TRUE if we have an active connection to the
# backend and FALSE otherwise
connExists <- function(env) {
  tryCatch({
    exists(".sparkRCon", envir = env) && isOpen(env[[".sparkRCon"]])
  }, error = function(err) {
    return(FALSE)
  })
}

#' Stop the Spark context.
#'
#' Also terminates the backend this R session is connected to
sparkR.stop <- function() {
  env <- .sparkREnv
  if (exists(".sparkRCon", envir = env)) {
    # cat("Stopping SparkR\n")
    if (exists(".sparkRjsc", envir = env)) {
      sc <- get(".sparkRjsc", envir = env)
      callJMethod(sc, "stop")
      rm(".sparkRjsc", envir = env)
    }

    if (exists(".backendLaunched", envir = env)) {
      callJStatic("SparkRHandler", "stopBackend")
    }

    # Also close the connection and remove it from our env
    conn <- get(".sparkRCon", envir = env)
    close(conn)

    rm(".sparkRCon", envir = env)
    rm(".scStartTime", envir = env)
  }

  if (exists(".monitorConn", envir = env)) {
    conn <- get(".monitorConn", envir = env)
    close(conn)
    rm(".monitorConn", envir = env)
  }

  # Clear all broadcast variables we have
  # as the jobj will not be valid if we restart the JVM
  clearBroadcastVariables()

  # Clear jobj maps
  clearJobjs()
}

#' Initialize a new Spark Context.
#'
#' This function initializes a new SparkContext.
#'
#' @param master The Spark master URL.
#' @param appName Application name to register with cluster manager
#' @param sparkHome Spark Home directory
#' @param sparkEnvir Named list of environment variables to set on worker nodes.
#' @param sparkExecutorEnv Named list of environment variables to be used when launching executors.
#' @param sparkJars Character string vector of jar files to pass to the worker nodes.
#' @param sparkRLibDir The path where R is installed on the worker nodes.
#' @param sparkPackages Character string vector of packages from spark-packages.org
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark")
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"))
#' sc <- sparkR.init("yarn-client", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"),
#'                  list(LD_LIBRARY_PATH="/directory of JVM libraries (libjvm.so) on workers/"),
#'                  c("jarfile1.jar","jarfile2.jar"))
#'}

sparkR.init <- function(
  master = "",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list(),
  sparkExecutorEnv = list(),
  sparkJars = "",
  sparkRLibDir = "",
  sparkPackages = "") {

  if (exists(".sparkRjsc", envir = .sparkREnv)) {
    cat("Re-using existing Spark Context. Please stop SparkR with sparkR.stop() or restart R to create a new Spark Context\n")
    return(get(".sparkRjsc", envir = .sparkREnv))
  }

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  jars <- suppressWarnings(normalizePath(as.character(sparkJars)))

  # Classpath separator is ";" on Windows
  # URI needs four /// as from http://stackoverflow.com/a/18522792
  if (.Platform$OS.type == "unix") {
    collapseChar <- ":"
    uriSep <- "//"
  } else {
    collapseChar <- ";"
    uriSep <- "////"
  }

  existingPort <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")
  if (existingPort != "") {
    backendPort <- existingPort
  } else {
    path <- tempfile(pattern = "backend_port")
    launchBackend(
        args = path,
        sparkHome = sparkHome,
        jars = jars,
        sparkSubmitOpts = Sys.getenv("SPARKR_SUBMIT_ARGS", "sparkr-shell"),
        packages = sparkPackages)
    # wait atmost 100 seconds for JVM to launch
    wait <- 0.1
    for (i in 1:25) {
      Sys.sleep(wait)
      if (file.exists(path)) {
        break
      }
      wait <- wait * 1.25
    }
    if (!file.exists(path)) {
      stop("JVM is not ready after 10 seconds")
    }
    f <- file(path, open='rb')
    backendPort <- readInt(f)
    monitorPort <- readInt(f)
    close(f)
    file.remove(path)
    if (length(backendPort) == 0 || backendPort == 0 ||
        length(monitorPort) == 0 || monitorPort == 0) {
      stop("JVM failed to launch")
    }
    assign(".monitorConn", socketConnection(port = monitorPort), envir = .sparkREnv)
    assign(".backendLaunched", 1, envir = .sparkREnv)
  }

  .sparkREnv$backendPort <- backendPort
  tryCatch({
    connectBackend("localhost", backendPort)
  }, error = function(err) {
    stop("Failed to connect JVM\n")
  })

  if (nchar(sparkHome) != 0) {
    sparkHome <- normalizePath(sparkHome)
  }

  if (nchar(sparkRLibDir) != 0) {
    .sparkREnv$libname <- sparkRLibDir
  }

  sparkEnvirMap <- new.env()
  for (varname in names(sparkEnvir)) {
    sparkEnvirMap[[varname]] <- sparkEnvir[[varname]]
  }

  sparkExecutorEnvMap <- new.env()
  if (!any(names(sparkExecutorEnv) == "LD_LIBRARY_PATH")) {
    sparkExecutorEnvMap[["LD_LIBRARY_PATH"]] <- paste0("$LD_LIBRARY_PATH:",Sys.getenv("LD_LIBRARY_PATH"))
  }
  for (varname in names(sparkExecutorEnv)) {
    sparkExecutorEnvMap[[varname]] <- sparkExecutorEnv[[varname]]
  }

  nonEmptyJars <- Filter(function(x) { x != "" }, jars)
  localJarPaths <- sapply(nonEmptyJars, function(j) { utils::URLencode(paste("file:", uriSep, j, sep = "")) })

  # Set the start time to identify jobjs
  # Seconds resolution is good enough for this purpose, so use ints
  assign(".scStartTime", as.integer(Sys.time()), envir = .sparkREnv)

  assign(
    ".sparkRjsc",
    callJStatic(
      "org.apache.spark.api.r.RRDD",
      "createSparkContext",
      master,
      appName,
      as.character(sparkHome),
      as.list(localJarPaths),
      sparkEnvirMap,
      sparkExecutorEnvMap),
    envir = .sparkREnv
  )

  sc <- get(".sparkRjsc", envir = .sparkREnv)

  # Register a finalizer to sleep 1 seconds on R exit to make RStudio happy
  reg.finalizer(.sparkREnv, function(x) { Sys.sleep(1) }, onexit = TRUE)

  sc
}

#' Initialize a new SQLContext.
#'
#' This function creates a SparkContext from an existing JavaSparkContext and
#' then uses it to initialize a new SQLContext
#'
#' @param jsc The existing JavaSparkContext created with SparkR.init()
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#'}

sparkRSQL.init <- function(jsc = NULL) {
  if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
    return(get(".sparkRSQLsc", envir = .sparkREnv))
  }

  # If jsc is NULL, create a Spark Context
  sc <- if (is.null(jsc)) {
    sparkR.init()
  } else {
    jsc
  }

  sqlContext <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                            "createSQLContext",
                            sc)
  assign(".sparkRSQLsc", sqlContext, envir = .sparkREnv)
  sqlContext
}

#' Initialize a new HiveContext.
#'
#' This function creates a HiveContext from an existing JavaSparkContext
#'
#' @param jsc The existing JavaSparkContext created with SparkR.init()
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRHive.init(sc)
#'}

sparkRHive.init <- function(jsc = NULL) {
  if (exists(".sparkRHivesc", envir = .sparkREnv)) {
    return(get(".sparkRHivesc", envir = .sparkREnv))
  }

  # If jsc is NULL, create a Spark Context
  sc <- if (is.null(jsc)) {
    sparkR.init()
  } else {
    jsc
  }

  ssc <- callJMethod(sc, "sc")
  hiveCtx <- tryCatch({
    newJObject("org.apache.spark.sql.hive.HiveContext", ssc)
  }, error = function(err) {
    stop("Spark SQL is not built with Hive support")
  })

  assign(".sparkRHivesc", hiveCtx, envir = .sparkREnv)
  hiveCtx
}

#' Assigns a group ID to all the jobs started by this thread until the group ID is set to a
#' different value or cleared.
#'
#' @param sc existing spark context
#' @param groupid the ID to be assigned to job groups
#' @param description description for the the job group ID
#' @param interruptOnCancel flag to indicate if the job is interrupted on job cancellation
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' setJobGroup(sc, "myJobGroup", "My job group description", TRUE)
#'}

setJobGroup <- function(sc, groupId, description, interruptOnCancel) {
  callJMethod(sc, "setJobGroup", groupId, description, interruptOnCancel)
}

#' Clear current job group ID and its description
#'
#' @param sc existing spark context
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' clearJobGroup(sc)
#'}

clearJobGroup <- function(sc) {
  callJMethod(sc, "clearJobGroup")
}

#' Cancel active jobs for the specified group
#'
#' @param sc existing spark context
#' @param groupId the ID of job group to be cancelled
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' cancelJobGroup(sc, "myJobGroup")
#'}

cancelJobGroup <- function(sc, groupId) {
  callJMethod(sc, "cancelJobGroup", groupId)
}

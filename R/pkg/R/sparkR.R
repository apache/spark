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

# Utility function that returns TRUE if we have an active connection to the
# backend and FALSE otherwise
connExists <- function(env) {
  tryCatch({
    exists(".sparkRCon", envir = env) && isOpen(env[[".sparkRCon"]])
  },
  error = function(err) {
    return(FALSE)
  })
}

#' Stop the Spark Session and Spark Context
#'
#' Stop the Spark Session and Spark Context.
#'
#' Also terminates the backend this R session is connected to.
#' @rdname sparkR.session.stop
#' @name sparkR.session.stop
#' @export
#' @note sparkR.session.stop since 2.0.0
sparkR.session.stop <- function() {
  env <- .sparkREnv
  if (exists(".sparkRCon", envir = env)) {
    if (exists(".sparkRjsc", envir = env)) {
      sc <- get(".sparkRjsc", envir = env)
      callJMethod(sc, "stop")
      rm(".sparkRjsc", envir = env)

      if (exists(".sparkRsession", envir = env)) {
        rm(".sparkRsession", envir = env)
      }
    }

    # Remove the R package lib path from .libPaths()
    if (exists(".libPath", envir = env)) {
      libPath <- get(".libPath", envir = env)
      .libPaths(.libPaths()[.libPaths() != libPath])
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

#' @rdname sparkR.session.stop
#' @name sparkR.stop
#' @export
#' @note sparkR.stop since 1.4.0
sparkR.stop <- function() {
  sparkR.session.stop()
}

#' (Deprecated) Initialize a new Spark Context
#'
#' This function initializes a new SparkContext.
#'
#' @param master The Spark master URL
#' @param appName Application name to register with cluster manager
#' @param sparkHome Spark Home directory
#' @param sparkEnvir Named list of environment variables to set on worker nodes
#' @param sparkExecutorEnv Named list of environment variables to be used when launching executors
#' @param sparkJars Character vector of jar files to pass to the worker nodes
#' @param sparkPackages Character vector of package coordinates
#' @seealso \link{sparkR.session}
#' @rdname sparkR.init-deprecated
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark")
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"))
#' sc <- sparkR.init("yarn-client", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="4g"),
#'                  list(LD_LIBRARY_PATH="/directory of JVM libraries (libjvm.so) on workers/"),
#'                  c("one.jar", "two.jar", "three.jar"),
#'                  c("com.databricks:spark-avro_2.10:2.0.1"))
#'}
#' @note sparkR.init since 1.4.0
sparkR.init <- function(
  master = "",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list(),
  sparkExecutorEnv = list(),
  sparkJars = "",
  sparkPackages = "") {
  .Deprecated("sparkR.session")
  sparkR.sparkContext(master,
     appName,
     sparkHome,
     convertNamedListToEnv(sparkEnvir),
     convertNamedListToEnv(sparkExecutorEnv),
     sparkJars,
     sparkPackages)
}

# Internal function to handle creating the SparkContext.
sparkR.sparkContext <- function(
  master = "",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvirMap = new.env(),
  sparkExecutorEnvMap = new.env(),
  sparkJars = "",
  sparkPackages = "") {

  if (exists(".sparkRjsc", envir = .sparkREnv)) {
    cat(paste("Re-using existing Spark Context.",
              "Call sparkR.session.stop() or restart R to create a new Spark Context\n"))
    return(get(".sparkRjsc", envir = .sparkREnv))
  }

  jars <- processSparkJars(sparkJars)
  packages <- processSparkPackages(sparkPackages)

  existingPort <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")
  if (existingPort != "") {
    if (length(packages) != 0) {
      warning(paste("sparkPackages has no effect when using spark-submit or sparkR shell",
                    " please use the --packages commandline instead", sep = ","))
    }
    backendPort <- existingPort
  } else {
    path <- tempfile(pattern = "backend_port")
    submitOps <- getClientModeSparkSubmitOpts(
        Sys.getenv("SPARKR_SUBMIT_ARGS", "sparkr-shell"),
        sparkEnvirMap)
    launchBackend(
        args = path,
        sparkHome = sparkHome,
        jars = jars,
        sparkSubmitOpts = submitOps,
        packages = packages)
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
    f <- file(path, open = "rb")
    backendPort <- readInt(f)
    monitorPort <- readInt(f)
    rLibPath <- readString(f)
    close(f)
    file.remove(path)
    if (length(backendPort) == 0 || backendPort == 0 ||
        length(monitorPort) == 0 || monitorPort == 0 ||
        length(rLibPath) != 1) {
      stop("JVM failed to launch")
    }
    assign(".monitorConn", socketConnection(port = monitorPort), envir = .sparkREnv)
    assign(".backendLaunched", 1, envir = .sparkREnv)
    if (rLibPath != "") {
      assign(".libPath", rLibPath, envir = .sparkREnv)
      .libPaths(c(rLibPath, .libPaths()))
    }
  }

  .sparkREnv$backendPort <- backendPort
  tryCatch({
    connectBackend("localhost", backendPort)
  },
  error = function(err) {
    stop("Failed to connect JVM\n")
  })

  if (nchar(sparkHome) != 0) {
    sparkHome <- suppressWarnings(normalizePath(sparkHome))
  }

  if (is.null(sparkExecutorEnvMap$LD_LIBRARY_PATH)) {
    sparkExecutorEnvMap[["LD_LIBRARY_PATH"]] <-
      paste0("$LD_LIBRARY_PATH:", Sys.getenv("LD_LIBRARY_PATH"))
  }

  # Classpath separator is ";" on Windows
  # URI needs four /// as from http://stackoverflow.com/a/18522792
  if (.Platform$OS.type == "unix") {
    uriSep <- "//"
  } else {
    uriSep <- "////"
  }
  localJarPaths <- lapply(jars,
                          function(j) { utils::URLencode(paste("file:", uriSep, j, sep = "")) })

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
      localJarPaths,
      sparkEnvirMap,
      sparkExecutorEnvMap),
    envir = .sparkREnv
  )

  sc <- get(".sparkRjsc", envir = .sparkREnv)

  # Register a finalizer to sleep 1 seconds on R exit to make RStudio happy
  reg.finalizer(.sparkREnv, function(x) { Sys.sleep(1) }, onexit = TRUE)

  sc
}

#' (Deprecated) Initialize a new SQLContext
#'
#' This function creates a SparkContext from an existing JavaSparkContext and
#' then uses it to initialize a new SQLContext
#'
#' Starting SparkR 2.0, a SparkSession is initialized and returned instead.
#' This API is deprecated and kept for backward compatibility only.
#'
#' @param jsc The existing JavaSparkContext created with SparkR.init()
#' @seealso \link{sparkR.session}
#' @rdname sparkRSQL.init-deprecated
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#'}
#' @note sparkRSQL.init since 1.4.0
sparkRSQL.init <- function(jsc = NULL) {
  .Deprecated("sparkR.session")

  if (exists(".sparkRsession", envir = .sparkREnv)) {
    return(get(".sparkRsession", envir = .sparkREnv))
  }

  # Default to without Hive support for backward compatibility.
  sparkR.session(enableHiveSupport = FALSE)
}

#' (Deprecated) Initialize a new HiveContext
#'
#' This function creates a HiveContext from an existing JavaSparkContext
#'
#' Starting SparkR 2.0, a SparkSession is initialized and returned instead.
#' This API is deprecated and kept for backward compatibility only.
#'
#' @param jsc The existing JavaSparkContext created with SparkR.init()
#' @seealso \link{sparkR.session}
#' @rdname sparkRHive.init-deprecated
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRHive.init(sc)
#'}
#' @note sparkRHive.init since 1.4.0
sparkRHive.init <- function(jsc = NULL) {
  .Deprecated("sparkR.session")

  if (exists(".sparkRsession", envir = .sparkREnv)) {
    return(get(".sparkRsession", envir = .sparkREnv))
  }

  # Default to without Hive support for backward compatibility.
  sparkR.session(enableHiveSupport = TRUE)
}

#' Get the existing SparkSession or initialize a new SparkSession.
#'
#' SparkSession is the entry point into SparkR. \code{sparkR.session} gets the existing
#' SparkSession or initializes a new SparkSession.
#' Additional Spark properties can be set in \code{...}, and these named parameters take priority
#' over values in \code{master}, \code{appName}, named lists of \code{sparkConfig}.
#'
#' For details on how to initialize and use SparkR, refer to SparkR programming guide at
#' \url{http://spark.apache.org/docs/latest/sparkr.html#starting-up-sparksession}.
#'
#' @param master the Spark master URL.
#' @param appName application name to register with cluster manager.
#' @param sparkHome Spark Home directory.
#' @param sparkConfig named list of Spark configuration to set on worker nodes.
#' @param sparkJars character vector of jar files to pass to the worker nodes.
#' @param sparkPackages character vector of package coordinates
#' @param enableHiveSupport enable support for Hive, fallback if not built with Hive support; once
#'        set, this cannot be turned off on an existing session
#' @param ... named Spark properties passed to the method.
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.json(path)
#'
#' sparkR.session("local[2]", "SparkR", "/home/spark")
#' sparkR.session("yarn-client", "SparkR", "/home/spark",
#'                list(spark.executor.memory="4g"),
#'                c("one.jar", "two.jar", "three.jar"),
#'                c("com.databricks:spark-avro_2.10:2.0.1"))
#' sparkR.session(spark.master = "yarn-client", spark.executor.memory = "4g")
#'}
#' @note sparkR.session since 2.0.0
sparkR.session <- function(
  master = "",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkConfig = list(),
  sparkJars = "",
  sparkPackages = "",
  enableHiveSupport = TRUE,
  ...) {

  sparkConfigMap <- convertNamedListToEnv(sparkConfig)
  namedParams <- list(...)
  if (length(namedParams) > 0) {
    paramMap <- convertNamedListToEnv(namedParams)
    # Override for certain named parameters
    if (exists("spark.master", envir = paramMap)) {
      master <- paramMap[["spark.master"]]
    }
    if (exists("spark.app.name", envir = paramMap)) {
      appName <- paramMap[["spark.app.name"]]
    }
    overrideEnvs(sparkConfigMap, paramMap)
  }

  if (!exists(".sparkRjsc", envir = .sparkREnv)) {
    retHome <- sparkCheckInstall(sparkHome, master)
    if (!is.null(retHome)) sparkHome <- retHome
    sparkExecutorEnvMap <- new.env()
    sparkR.sparkContext(master, appName, sparkHome, sparkConfigMap, sparkExecutorEnvMap,
       sparkJars, sparkPackages)
    stopifnot(exists(".sparkRjsc", envir = .sparkREnv))
  }

  if (exists(".sparkRsession", envir = .sparkREnv)) {
    sparkSession <- get(".sparkRsession", envir = .sparkREnv)
    # Apply config to Spark Context and Spark Session if already there
    # Cannot change enableHiveSupport
    callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                "setSparkContextSessionConf",
                sparkSession,
                sparkConfigMap)
  } else {
    jsc <- get(".sparkRjsc", envir = .sparkREnv)
    sparkSession <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                                "getOrCreateSparkSession",
                                jsc,
                                sparkConfigMap,
                                enableHiveSupport)
    assign(".sparkRsession", sparkSession, envir = .sparkREnv)
  }
  sparkSession
}

#' Assigns a group ID to all the jobs started by this thread until the group ID is set to a
#' different value or cleared.
#'
#' @param groupId the ID to be assigned to job groups.
#' @param description description for the job group ID.
#' @param interruptOnCancel flag to indicate if the job is interrupted on job cancellation.
#' @rdname setJobGroup
#' @name setJobGroup
#' @examples
#'\dontrun{
#' sparkR.session()
#' setJobGroup("myJobGroup", "My job group description", TRUE)
#'}
#' @note setJobGroup since 1.5.0
#' @method setJobGroup default
setJobGroup.default <- function(groupId, description, interruptOnCancel) {
  sc <- getSparkContext()
  callJMethod(sc, "setJobGroup", groupId, description, interruptOnCancel)
}

setJobGroup <- function(sc, groupId, description, interruptOnCancel) {
  if (class(sc) == "jobj" && any(grepl("JavaSparkContext", getClassName.jobj(sc)))) {
    .Deprecated("setJobGroup(groupId, description, interruptOnCancel)",
                old = "setJobGroup(sc, groupId, description, interruptOnCancel)")
    setJobGroup.default(groupId, description, interruptOnCancel)
  } else {
    # Parameter order is shifted
    groupIdToUse <- sc
    descriptionToUse <- groupId
    interruptOnCancelToUse <- description
    setJobGroup.default(groupIdToUse, descriptionToUse, interruptOnCancelToUse)
  }
}

#' Clear current job group ID and its description
#'
#' @rdname clearJobGroup
#' @name clearJobGroup
#' @examples
#'\dontrun{
#' sparkR.session()
#' clearJobGroup()
#'}
#' @note clearJobGroup since 1.5.0
#' @method clearJobGroup default
clearJobGroup.default <- function() {
  sc <- getSparkContext()
  callJMethod(sc, "clearJobGroup")
}

clearJobGroup <- function(sc) {
  if (!missing(sc) &&
      class(sc) == "jobj" &&
      any(grepl("JavaSparkContext", getClassName.jobj(sc)))) {
    .Deprecated("clearJobGroup()", old = "clearJobGroup(sc)")
  }
  clearJobGroup.default()
}


#' Cancel active jobs for the specified group
#'
#' @param groupId the ID of job group to be cancelled
#' @rdname cancelJobGroup
#' @name cancelJobGroup
#' @examples
#'\dontrun{
#' sparkR.session()
#' cancelJobGroup("myJobGroup")
#'}
#' @note cancelJobGroup since 1.5.0
#' @method cancelJobGroup default
cancelJobGroup.default <- function(groupId) {
  sc <- getSparkContext()
  callJMethod(sc, "cancelJobGroup", groupId)
}

cancelJobGroup <- function(sc, groupId) {
  if (class(sc) == "jobj" && any(grepl("JavaSparkContext", getClassName.jobj(sc)))) {
    .Deprecated("cancelJobGroup(groupId)", old = "cancelJobGroup(sc, groupId)")
    cancelJobGroup.default(groupId)
  } else {
    # Parameter order is shifted
    groupIdToUse <- sc
    cancelJobGroup.default(groupIdToUse)
  }
}

sparkConfToSubmitOps <- new.env()
sparkConfToSubmitOps[["spark.driver.memory"]]           <- "--driver-memory"
sparkConfToSubmitOps[["spark.driver.extraClassPath"]]   <- "--driver-class-path"
sparkConfToSubmitOps[["spark.driver.extraJavaOptions"]] <- "--driver-java-options"
sparkConfToSubmitOps[["spark.driver.extraLibraryPath"]] <- "--driver-library-path"
sparkConfToSubmitOps[["spark.master"]] <- "--master"
sparkConfToSubmitOps[["spark.yarn.keytab"]] <- "--keytab"
sparkConfToSubmitOps[["spark.yarn.principal"]] <- "--principal"


# Utility function that returns Spark Submit arguments as a string
#
# A few Spark Application and Runtime environment properties cannot take effect after driver
# JVM has started, as documented in:
# http://spark.apache.org/docs/latest/configuration.html#application-properties
# When starting SparkR without using spark-submit, for example, from Rstudio, add them to
# spark-submit commandline if not already set in SPARKR_SUBMIT_ARGS so that they can be effective.
getClientModeSparkSubmitOpts <- function(submitOps, sparkEnvirMap) {
  envirToOps <- lapply(ls(sparkConfToSubmitOps), function(conf) {
    opsValue <- sparkEnvirMap[[conf]]
    # process only if --option is not already specified
    if (!is.null(opsValue) &&
        nchar(opsValue) > 1 &&
        !grepl(sparkConfToSubmitOps[[conf]], submitOps)) {
      # put "" around value in case it has spaces
      paste0(sparkConfToSubmitOps[[conf]], " \"", opsValue, "\" ")
    } else {
      ""
    }
  })
  # --option must be before the application class "sparkr-shell" in submitOps
  paste0(paste0(envirToOps, collapse = ""), submitOps)
}

# Utility function that handles sparkJars argument, and normalize paths
processSparkJars <- function(jars) {
  splittedJars <- splitString(jars)
  if (length(splittedJars) > length(jars)) {
    warning("sparkJars as a comma-separated string is deprecated, use character vector instead")
  }
  normalized <- suppressWarnings(normalizePath(splittedJars))
  normalized
}

# Utility function that handles sparkPackages argument
processSparkPackages <- function(packages) {
  splittedPackages <- splitString(packages)
  if (length(splittedPackages) > length(packages)) {
    warning("sparkPackages as a comma-separated string is deprecated, use character vector instead")
  }
  splittedPackages
}

# Utility function that checks and install Spark to local folder if not found
#
# Installation will not be triggered if it's called from sparkR shell
# or if the master url is not local
#
# @param sparkHome directory to find Spark package.
# @param master the Spark master URL, used to check local or remote mode.
# @return NULL if no need to update sparkHome, and new sparkHome otherwise.
sparkCheckInstall <- function(sparkHome, master) {
  if (!isSparkRShell()) {
    if (!is.na(file.info(sparkHome)$isdir)) {
      msg <- paste0("Spark package found in SPARK_HOME: ", sparkHome)
      message(msg)
      NULL
    } else {
      if (!nzchar(master) || isMasterLocal(master)) {
        msg <- paste0("Spark not found in SPARK_HOME: ",
                      sparkHome)
        message(msg)
        packageLocalDir <- install.spark()
        packageLocalDir
      } else {
        msg <- paste0("Spark not found in SPARK_HOME: ",
                      sparkHome, "\n", installInstruction("remote"))
        stop(msg)
      }
    }
  } else {
    NULL
  }
}

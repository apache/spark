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
#' @examples
#'\dontrun{
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark")
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"))
#' sc <- sparkR.init("yarn-client", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="4g"),
#'                  list(LD_LIBRARY_PATH="/directory of JVM libraries (libjvm.so) on workers/"),
#'                  c("one.jar", "two.jar", "three.jar"),
#'                  c("com.databricks:spark-avro_2.11:2.0.1"))
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
  connectionTimeout <- as.numeric(Sys.getenv("SPARKR_BACKEND_CONNECTION_TIMEOUT", "6000"))
  if (existingPort != "") {
    if (length(packages) != 0) {
      warning("sparkPackages has no effect when using spark-submit or sparkR shell, ",
              "please use the --packages commandline instead")
    }
    backendPort <- existingPort
    authSecret <- Sys.getenv("SPARKR_BACKEND_AUTH_SECRET")
    if (nchar(authSecret) == 0) {
      stop("Auth secret not provided in environment.")
    }
  } else {
    path <- tempfile(pattern = "backend_port")
    submitOps <- getClientModeSparkSubmitOpts(
        Sys.getenv("SPARKR_SUBMIT_ARGS", "sparkr-shell"),
        sparkEnvirMap)
    invisible(checkJavaVersion())
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
    connectionTimeout <- readInt(f)

    # Don't use readString() so that we can provide a useful
    # error message if the R and Java versions are mismatched.
    authSecretLen <- readInt(f)
    if (length(authSecretLen) == 0 || authSecretLen == 0) {
      stop("Unexpected EOF in JVM connection data. Mismatched versions?")
    }
    authSecret <- readStringData(f, authSecretLen)
    close(f)
    file.remove(path)
    if (length(backendPort) == 0 || backendPort == 0 ||
        length(monitorPort) == 0 || monitorPort == 0 ||
        length(rLibPath) != 1 || length(authSecret) == 0) {
      stop("JVM failed to launch")
    }

    monitorConn <- socketConnection(port = monitorPort, blocking = TRUE,
                                    timeout = connectionTimeout, open = "wb")
    doServerAuth(monitorConn, authSecret)

    assign(".monitorConn", monitorConn, envir = .sparkREnv)
    assign(".backendLaunched", 1, envir = .sparkREnv)
    if (rLibPath != "") {
      assign(".libPath", rLibPath, envir = .sparkREnv)
      .libPaths(c(rLibPath, .libPaths()))
    }
  }

  .sparkREnv$backendPort <- backendPort
  tryCatch({
    connectBackend("localhost", backendPort, timeout = connectionTimeout, authSecret = authSecret)
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
                          function(j) { utils::URLencode(paste0("file:", uriSep, j)) })

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
#' When called in an interactive session, this method checks for the Spark installation, and, if not
#' found, it will be downloaded and cached automatically. Alternatively, \code{install.spark} can
#' be called manually.
#'
#' A default warehouse is created automatically in the current directory when a managed table is
#' created via \code{sql} statement \code{CREATE TABLE}, for example. To change the location of the
#' warehouse, set the named parameter \code{spark.sql.warehouse.dir} to the SparkSession. Along with
#' the warehouse, an accompanied metastore may also be automatically created in the current
#' directory when a new SparkSession is initialized with \code{enableHiveSupport} set to
#' \code{TRUE}, which is the default. For more details, refer to Hive configuration at
#' \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables}.
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
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.json(path)
#'
#' sparkR.session("local[2]", "SparkR", "/home/spark")
#' sparkR.session("yarn", "SparkR", "/home/spark",
#'                list(spark.executor.memory="4g", spark.submit.deployMode="client"),
#'                c("one.jar", "two.jar", "three.jar"),
#'                c("com.databricks:spark-avro_2.12:2.0.1"))
#' sparkR.session(spark.master = "yarn", spark.submit.deployMode = "client",
#                 spark.executor.memory = "4g")
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

  deployMode <- ""
  if (exists("spark.submit.deployMode", envir = sparkConfigMap)) {
    deployMode <- sparkConfigMap[["spark.submit.deployMode"]]
  }

  if (!exists("spark.r.sql.derby.temp.dir", envir = sparkConfigMap)) {
    sparkConfigMap[["spark.r.sql.derby.temp.dir"]] <- tempdir()
  }

  if (!exists(".sparkRjsc", envir = .sparkREnv)) {
    retHome <- sparkCheckInstall(sparkHome, master, deployMode)
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

  # Check if version number of SparkSession matches version number of SparkR package
  jvmVersion <- callJMethod(sparkSession, "version")
  # Remove -SNAPSHOT from jvm versions
  jvmVersionStrip <- gsub("-SNAPSHOT", "", jvmVersion, fixed = TRUE)
  rPackageVersion <- paste0(packageVersion("SparkR"))

  if (jvmVersionStrip != rPackageVersion) {
    warning("Version mismatch between Spark JVM and SparkR package. ",
            "JVM version was ", jvmVersion,
            ", while R package version was ", rPackageVersion)
  }

  sparkSession
}

#' Get the URL of the SparkUI instance for the current active SparkSession
#'
#' Get the URL of the SparkUI instance for the current active SparkSession.
#'
#' @return the SparkUI URL, or NA if it is disabled, or not started.
#' @rdname sparkR.uiWebUrl
#' @name sparkR.uiWebUrl
#' @examples
#'\dontrun{
#' sparkR.session()
#' url <- sparkR.uiWebUrl()
#' }
#' @note sparkR.uiWebUrl since 2.1.1
sparkR.uiWebUrl <- function() {
  sc <- sparkR.callJMethod(getSparkContext(), "sc")
  u <- callJMethod(sc, "uiWebUrl")
  if (callJMethod(u, "isDefined")) {
    callJMethod(u, "get")
  } else {
    NA
  }
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
setJobGroup <- function(groupId, description, interruptOnCancel) {
  sc <- getSparkContext()
  invisible(callJMethod(sc, "setJobGroup", groupId, description, interruptOnCancel))
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
clearJobGroup <- function() {
  sc <- getSparkContext()
  invisible(callJMethod(sc, "clearJobGroup"))
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
cancelJobGroup <- function(groupId) {
  sc <- getSparkContext()
  invisible(callJMethod(sc, "cancelJobGroup", groupId))
}

#' Set a human readable description of the current job.
#'
#' Set a description that is shown as a job description in UI.
#'
#' @param value The job description of the current job.
#' @rdname setJobDescription
#' @name setJobDescription
#' @examples
#'\dontrun{
#' setJobDescription("This is an example job.")
#'}
#' @note setJobDescription since 2.3.0
setJobDescription <- function(value) {
  if (!is.null(value)) {
    value <- as.character(value)
  }
  sc <- getSparkContext()
  invisible(callJMethod(sc, "setJobDescription", value))
}

#' Set a local property that affects jobs submitted from this thread, such as the
#' Spark fair scheduler pool.
#'
#' @param key The key for a local property.
#' @param value The value for a local property.
#' @rdname setLocalProperty
#' @name setLocalProperty
#' @examples
#'\dontrun{
#' setLocalProperty("spark.scheduler.pool", "poolA")
#'}
#' @note setLocalProperty since 2.3.0
setLocalProperty <- function(key, value) {
  if (is.null(key) || is.na(key)) {
    stop("key should not be NULL or NA.")
  }
  if (!is.null(value)) {
    value <- as.character(value)
  }
  sc <- getSparkContext()
  invisible(callJMethod(sc, "setLocalProperty", as.character(key), value))
}

#' Get a local property set in this thread, or \code{NULL} if it is missing. See
#' \code{setLocalProperty}.
#'
#' @param key The key for a local property.
#' @rdname getLocalProperty
#' @name getLocalProperty
#' @examples
#'\dontrun{
#' getLocalProperty("spark.scheduler.pool")
#'}
#' @note getLocalProperty since 2.3.0
getLocalProperty <- function(key) {
  if (is.null(key) || is.na(key)) {
    stop("key should not be NULL or NA.")
  }
  sc <- getSparkContext()
  callJMethod(sc, "getLocalProperty", as.character(key))
}

sparkConfToSubmitOps <- new.env()
sparkConfToSubmitOps[["spark.driver.memory"]]           <- "--driver-memory"
sparkConfToSubmitOps[["spark.driver.extraClassPath"]]   <- "--driver-class-path"
sparkConfToSubmitOps[["spark.driver.extraJavaOptions"]] <- "--driver-java-options"
sparkConfToSubmitOps[["spark.driver.extraLibraryPath"]] <- "--driver-library-path"
sparkConfToSubmitOps[["spark.master"]] <- "--master"
sparkConfToSubmitOps[["spark.yarn.keytab"]] <- "--keytab"
sparkConfToSubmitOps[["spark.yarn.principal"]] <- "--principal"
sparkConfToSubmitOps[["spark.kerberos.keytab"]] <- "--keytab"
sparkConfToSubmitOps[["spark.kerberos.principal"]] <- "--principal"


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
        !grepl(sparkConfToSubmitOps[[conf]], submitOps, fixed = TRUE)) {
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
# @param deployMode whether to deploy your driver on the worker nodes (cluster)
#        or locally as an external client (client).
# @return NULL if no need to update sparkHome, and new sparkHome otherwise.
sparkCheckInstall <- function(sparkHome, master, deployMode) {
  if (!isSparkRShell()) {
    if (!is.na(file.info(sparkHome)$isdir)) {
      message("Spark package found in SPARK_HOME: ", sparkHome)
      NULL
    } else {
      if (interactive() || isMasterLocal(master)) {
        message("Spark not found in SPARK_HOME: ", sparkHome)
        # If EXISTING_SPARKR_BACKEND_PORT environment variable is set, assume
        # that we're in Spark submit. spark-submit always sets Spark home
        # so this case should not happen. This is just a safeguard.
        isSparkRSubmit <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "") != ""

        # SPARKR_ASK_INSTALLATION is an internal environment variable in case
        # users want to disable this behavior. This environment variable should
        # be removed if no user complains. This environment variable was added
        # in case other notebook projects are affected.
        if (!isSparkRSubmit && Sys.getenv("SPARKR_ASK_INSTALLATION", "TRUE") == "TRUE") {
          # Finally, we're either plain R shell or Rscript.
          msg <- paste0(
            "Will you download and install (or reuse if it exists) Spark package ",
            "under the cache [", sparkCachePath(), "]? (y/n): ")

          answer <- NA
          while (is.na(answer) || (answer != "y" && answer != "n")) {
            # Dispatch on R shell in case readLines does not work in RStudio
            # See https://stackoverflow.com/questions/30191232/use-stdin-from-within-r-studio
            if (interactive()) {
              answer <- readline(prompt = msg)
            } else {
              cat(msg)
              answer <- readLines("stdin", n = 1)
            }
          }
          if (answer == "n") {
            stop(paste0(
             "Please make sure Spark package is installed in this machine.\n",
             "  - If there is one, set the path in sparkHome parameter or ",
             "environment variable SPARK_HOME.\n",
             "  - If not, you may run install.spark function to do the job."))
          }
        }
        packageLocalDir <- install.spark()
        packageLocalDir
      } else if (isClientMode(master) || deployMode == "client") {
        msg <- paste0("Spark not found in SPARK_HOME: ",
                      sparkHome, "\n", installInstruction("remote"))
        stop(msg)
      } else {
        NULL
      }
    }
  } else {
    NULL
  }
}

# Utility function for sending auth data over a socket and checking the server's reply.
doServerAuth <- function(con, authSecret) {
  if (nchar(authSecret) == 0) {
    stop("Auth secret not provided.")
  }
  writeString(con, authSecret)
  flush(con)
  reply <- readString(con)
  if (reply != "ok") {
    close(con)
    stop("Unexpected reply from server.")
  }
}

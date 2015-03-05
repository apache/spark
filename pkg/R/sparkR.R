.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep = "")
  packageStartupMessage("[SparkR] Initializing with classpath ", assemblyJarPath, "\n")
 
  .sparkREnv$libname <- libname
  .sparkREnv$assemblyJarPath <- assemblyJarPath
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

# Stop the Spark context.
# Also terminates the backend this R session is connected to
sparkR.stop <- function(env = .sparkREnv) {

  if (!connExists(env)) {
    # When the workspace is saved in R, the connections are closed
    # *before* the finalizer is run. In these cases, we reconnect
    # to the backend, so we can shut it down.
    tryCatch({
      connectBackend("localhost", .sparkREnv$sparkRBackendPort)
    }, error = function(err) {
      cat("Error in Connection: Use sparkR.init() to restart SparkR\n")
    }, warning = function(war) {
      cat("No Connection Found: Use sparkR.init() to restart SparkR\n")
    })
  } 

  if (exists(".sparkRCon", envir = env)) {
    cat("Stopping SparkR\n")
    if (exists(".sparkRjsc", envir = env)) {
      sc <- get(".sparkRjsc", envir = env)
      callJMethod(sc, "stop")
      rm(".sparkRjsc", envir = env)
    }
  
    callJStatic("SparkRHandler", "stopBackend")
    # Also close the connection and remove it from our env
    conn <- get(".sparkRCon", envir = env)
    close(conn)
    rm(".sparkRCon", envir = env)

    if (exists(".monitorConn", envir = env)) {
      conn <- get(".monitorConn", envir = env)
      close(conn)
      rm(".monitorConn", envir = env)
    }

    # Finally, sleep for 1 sec to let backend finish exiting.
    # Without this we get port conflicts in RStudio when we try to 'Restart R'.
    Sys.sleep(1)
  }

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
#' @param sparkRBackendPort The port to use for SparkR JVM Backend.
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
  sparkRLibDir = "") {

  if (exists(".sparkRjsc", envir = .sparkREnv)) {
    cat("Re-using existing Spark Context. Please stop SparkR with sparkR.stop() or restart R to create a new Spark Context\n")
    return(get(".sparkRjsc", envir = .sparkREnv))
  }

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  jars <- suppressWarnings(
    normalizePath(c(as.character(.sparkREnv$assemblyJarPath), as.character(sparkJars))))

  # Classpath separator is ";" on Windows
  # URI needs four /// as from http://stackoverflow.com/a/18522792
  if (.Platform$OS.type == "unix") {
    collapseChar <- ":"
    uriSep <- "//"
  } else {
    collapseChar <- ";"
    uriSep <- "////"
  }
  cp <- paste0(jars, collapse = collapseChar)

  yarn_conf_dir <- Sys.getenv("YARN_CONF_DIR", "")
  if (yarn_conf_dir != "") {
    cp <- paste(cp, yarn_conf_dir, sep = ":")
  }

  sparkRExistingPort <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")
  if (sparkRExistingPort != "") {
    sparkRBackendPort <- sparkRExistingPort
  } else {
    path <- tempfile(pattern = "backend_port")
    if (Sys.getenv("SPARKR_USE_SPARK_SUBMIT", "") == "") {
      launchBackend(classPath = cp,
                    mainClass = "edu.berkeley.cs.amplab.sparkr.SparkRBackend",
                    args = path,
                    javaOpts = paste("-Xmx", sparkMem, sep = ""))
    } else {
      # TODO: We should deprecate sparkJars and ask users to add it to the
      # command line (using --jars) which is picked up by SparkSubmit
      launchBackendSparkSubmit(
          mainClass = "edu.berkeley.cs.amplab.sparkr.SparkRBackend",
          args = path,
          appJar = .sparkREnv$assemblyJarPath,
          sparkHome = sparkHome,
          sparkSubmitOpts = Sys.getenv("SPARKR_SUBMIT_ARGS", ""))
    }
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
    sparkRBackendPort <- readInt(f)
    monitorPort <- readInt(f)
    close(f)
    file.remove(path)
    if (length(sparkRBackendPort) == 0 || sparkRBackendPort == 0 ||
        length(monitorPort) == 0 || monitorPort == 0) {
      stop("JVM failed to launch")
    }
    assign(".monitorConn", socketConnection(port = monitorPort), envir = .sparkREnv)
  }

  .sparkREnv$sparkRBackendPort <- sparkRBackendPort
  tryCatch({
    connectBackend("localhost", sparkRBackendPort)
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

  assign(
    ".sparkRjsc",
    callJStatic(
      "edu.berkeley.cs.amplab.sparkr.RRDD",
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

  # Register a finalizer to stop backend on R exit
  reg.finalizer(.sparkREnv, sparkR.stop, onexit = TRUE)

  sc
}

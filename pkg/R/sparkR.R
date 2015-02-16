.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep = "")
  assemblyJarPath <- gsub(" ", "\\ ", assemblyJarPath, fixed = T)
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
    conn <- get(".sparkRCon", env)
    close(conn)
    rm(".sparkRCon", envir = env)
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
  master = "local",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list(),
  sparkExecutorEnv = list(),
  sparkJars = "",
  sparkRLibDir = "",
  sparkRBackendPort = 12345) {

  if (exists(".sparkRjsc", envir = .sparkREnv)) {
    cat("Re-using existing Spark Context. Please stop SparkR with sparkR.stop() or restart R to create a new Spark Context\n")
    return(get(".sparkRjsc", envir = .sparkREnv))
  }

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  jars <- c(as.character(.sparkREnv$assemblyJarPath), as.character(sparkJars))

  cp <- paste0(jars, collapse = ":")

  yarn_conf_dir <- Sys.getenv("YARN_CONF_DIR", "")
  if (yarn_conf_dir != "") {
    cp <- paste(cp, yarn_conf_dir, sep = ":")
  }
  launchBackend(classPath = cp,
                mainClass = "edu.berkeley.cs.amplab.sparkr.SparkRBackend",
                args = as.character(sparkRBackendPort),
                javaOpts = paste("-Xmx", sparkMem, sep = ""))
  Sys.sleep(2) # Wait for backend to come up
  .sparkREnv$sparkRBackendPort <- sparkRBackendPort
  connectBackend("localhost", sparkRBackendPort) # Connect to it

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
  localJarPaths <- sapply(nonEmptyJars, function(j) { paste("file://", j, sep = "") })

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
#' sqlCtx <- sparkRSQL.init(sc)
#'}

sparkRSQL.init <- function(jsc) {
  sparkContext = callJMethod(jsc, "sc")
  
  if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
    cat("Re-using existing SparkSQL Context. Please restart R to create a new SparkSQL Context\n")
    return(get(".sparkRSQLsc", envir = .sparkREnv))
  }
  
  assign(
    ".sparkRSQLsc",
    callJStatic(
      "edu.berkeley.cs.amplab.sparkr.SQLUtils",
      "createSQLContext",
      sparkContext),
    envir = .sparkREnv
  )
  sqlCtx <- get(".sparkRSQLsc", envir = .sparkREnv)
  
  sqlCtx
}

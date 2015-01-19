.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep="")
  assemblyJarPath <- gsub(" ", "\\ ", assemblyJarPath, fixed=T)
  packageStartupMessage("[SparkR] Initializing with classpath ", assemblyJarPath, "\n")
 
  .sparkREnv$libname <- libname
  .sparkREnv$assemblyJarPath <- assemblyJarPath
}

#' Stop this Spark context
#' Also terminates the backend this R session is connected to
sparkR.stop <- function(sparkREnv) {
  sc <- get(".sparkRjsc", envir=.sparkREnv)
  callJMethod(sc, "stop")
  stopBackend()
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
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark")
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"))
#' sc <- sparkR.init("yarn-client", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"),
#'                  list(LD_LIBRARY_PATH="/directory of Java VM Library Files (libjvm.so) on worker nodes/"),
#'                  c("jarfile1.jar","jarfile2.jar"))
#'}

sparkR.init <- function(
  master = "local",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list(),
  sparkExecutorEnv = list(),
  sparkJars = "",
  sparkRLibDir = "") {

  if (exists(".sparkRjsc", envir=.sparkREnv)) {
    cat("Re-using existing Spark Context. Please restart R to create a new Spark Context\n")
    return(get(".sparkRjsc", envir=.sparkREnv))
  }

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  cp = .sparkREnv$assemblyJarPath
  yarn_conf_dir <- Sys.getenv("YARN_CONF_DIR", "")
  if (yarn_conf_dir != "") {
    cp = paste(cp, yarn_conf_dir, sep=":")
  }
  launchBackend(classPath=cp, mainClass="edu.berkeley.cs.amplab.sparkr.SparkRBackend",
                args="12345", javaOpts=paste("-Xmx", sparkMem, sep=""))
  Sys.sleep(2) # Wait for backend to come up
  init("localhost", 12345) # Connect to it

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
  
  #.jaddClassPath(sparkJars)
  jars <- c(as.character(.sparkREnv$assemblyJarPath), as.character(sparkJars))

  nonEmptyJars <- Filter(function(x) { x != "" }, jars)
  localJarPaths <- sapply(nonEmptyJars, function(j) { paste("file://", j, sep="") })

  assign(
    ".sparkRjsc",
    createSparkContext(
      master,
      appName,
      as.character(sparkHome),
      as.list(localJarPaths),
      sparkEnvirMap,
      sparkExecutorEnvMap),
    envir=.sparkREnv
  )

  sc <- get(".sparkRjsc", envir=.sparkREnv)

  # Register a finalizer to stop backend on R exit
  reg.finalizer(.sparkREnv, sparkR.stop, onexit=TRUE)

  sc
}

.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep="")
  packageStartupMessage("[SparkR] Initializing with classpath ", assemblyJarPath, "\n")

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  yarn_conf_dir <- Sys.getenv("YARN_CONF_DIR", "")
  
  .sparkREnv$libname <- libname
  .sparkREnv$assemblyJarPath <- assemblyJarPath
  .jinit(classpath=assemblyJarPath, parameters=paste("-Xmx", sparkMem, sep=""))
  .jaddClassPath(yarn_conf_dir)
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
#'                  List(LD_LIBRARY_PATH="/directory of Java VM Library Files (libjvm.so) on worker nodes/"),
#'                  c("jarfile1.jar","jarfile2.jar"))
#'}

sparkR.init <- function(
  master = "local",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list(),
  sparkExecutorEnv = list(),
  sparkJars = "") {

  if (exists(".sparkRjsc", envir=.sparkREnv)) {
    return(get(".sparkRjsc", envir=.sparkREnv))
  }

  if (nchar(sparkHome) != 0) {
    sparkHome <- normalizePath(sparkHome)
  }

  sparkEnvirMap <- .jnew("java/util/HashMap")
  for (varname in names(sparkEnvir)) {
    sparkEnvirMap$put(varname, sparkEnvir[[varname]])
  }
  
  sparkExecutorEnvMap <- .jnew("java/util/HashMap")
  if (!any(names(sparkExecutorEnv) == "LD_LIBRARY_PATH")) {
    sparkExecutorEnvMap$put("LD_LIBRARY_PATH", paste0("$LD_LIBRARY_PATH:",Sys.getenv("LD_LIBRARY_PATH")))
  }
  for (varname in names(sparkExecutorEnv)) {
    sparkExecutorEnvMap$put(varname, sparkExecutorEnv[[varname]])
  }
  
  .jaddClassPath(sparkJars)
  jars=c(as.character(.sparkREnv$assemblyJarPath), as.character(sparkJars))
  
  assign(
    ".sparkRjsc",
    J("edu.berkeley.cs.amplab.sparkr.RRDD",
      "createSparkContext",
      master,
      appName,
      as.character(sparkHome),
      .jarray(jars, "java/lang/String"),
      sparkEnvirMap,
      sparkExecutorEnvMap),
    envir=.sparkREnv
  )

  get(".sparkRjsc", envir=.sparkREnv)
}

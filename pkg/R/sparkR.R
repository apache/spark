.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep="")
  packageStartupMessage("[SparkR] Initializing with classpath ", assemblyJarPath, "\n")

  sparkMem <- Sys.getenv("SPARK_MEM", "512m")
  .sparkREnv$libname <- libname
  .sparkREnv$assemblyJarPath <- assemblyJarPath
  .jinit(classpath=assemblyJarPath, parameters=paste("-Xmx", sparkMem, sep=""))
}

#' Initialize a new Spark Context.
#'
#' This function initializes a new SparkContext.
#'
#' @param master The Spark master URL.
#' @param appName Application name to register with cluster manager
#' @param sparkHome Spark Home directory
#' @param sparkEnvir Named list of environment variables to set on worker nodes.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark")
#' sc <- sparkR.init("local[2]", "SparkR", "/home/spark",
#'                  list(spark.executor.memory="1g"))
#'}

sparkR.init <- function(
  master = "local",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  sparkEnvir = list() ) {

  if (exists(".sparkRjsc", envir=.sparkREnv)) {
    return(get(".sparkRjsc", envir=.sparkREnv))
  }

  if (nchar(sparkHome) != 0) {
    sparkHome <- normalizePath(sparkHome)
  }

  hm <- .jnew("java/util/HashMap")
  for ( varname in names(sparkEnvir)) {
    .jrcall(hm, "put", varname, sparkEnvir[[varname]])
  }

  assign(
    ".sparkRjsc",
    J("edu.berkeley.cs.amplab.sparkr.RRDD",
      "createSparkContext",
      master,
      appName,
      as.character(sparkHome),
      .jarray(as.character(.sparkREnv$assemblyJarPath),
               "java/lang/String"),
      hm),
    envir=.sparkREnv
  )

  get(".sparkRjsc", envir=.sparkREnv)
}

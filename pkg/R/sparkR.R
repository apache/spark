.sparkREnv <- new.env()

assemblyJarName <- "sparkr-assembly-0.1.jar"

sparkR.onLoad <- function(libname, pkgname) {
  assemblyJarPath <- paste(libname, "/SparkR/", assemblyJarName, sep="")
  packageStartupMessage("[SparkR] Initializing with classpath ", assemblyJarPath, "\n")
  .sparkREnv[["libname"]] <- libname
  .sparkREnv[["assemblyJarPath"]] <- assemblyJarPath
  .jinit(classpath=assemblyJarPath)
}

#' Initialize a new Spark Context.
#'
#' This function initializes a new SparkContext.
#'
#' @param master The Spark master URL.
#' @param appName Application name to register with cluster manager
#' @param sparkHome Spark Home directory
#' @export
#' @examples
#'\dontrun{
#' sparkR.init("local[2]", "SparkR", "/home/spark")
#'}

sparkR.init <- function(
  master = "local",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME")) {

  if (exists(".sparkRjsc", envir=.sparkREnv)) {
    return(get(".sparkRjsc", envir=.sparkREnv))
  }

  sparkHomeNormalized <- normalizePath(sparkHome)

  # TODO: support other constructors
  assign(
    ".sparkRjsc",
     .jnew("org/apache/spark/api/java/JavaSparkContext", master, appName,
           as.character(sparkHomeNormalized),
           as.character(.sparkREnv[["assemblyJarPath"]])),
     envir=.sparkREnv
  )

  get(".sparkRjsc", envir=.sparkREnv)
}

# Hardcoded spark jar
sparkJarPath <- "assembly/target/scala-2.9.3/spark-assembly-0.8.0-SNAPSHOT-hadoop1.0.4.jar"


.sparkREnv <- new.env()

sparkR.onLoad <- function(libname, pkgname) {
  sparkDir <- strsplit(libname, "/")
  sparkJarAbsPath <- c(sparkDir[[1]][1:(length(sparkDir[[1]]) - 2)], sparkJarPath)
  classPath <- paste(sparkJarAbsPath, collapse = "/")

  assign("sparkJar", classPath, env=.sparkREnv)

  packageStartupMessage("[SparkR] Initializing with classpath ", classPath, "\n")

  .jinit(classpath=classPath)
}

# Initializes and returns a JavaSparkContext
sparkR.init <- function(
  master = "local[2]",
  appName = "SparkR",
  sparkHome = Sys.getenv("SPARK_HOME"),
  jars = NULL,
  jarFile = NULL,
  environment = NULL) {

  if (exists(".sparkRjsc", env=.sparkREnv)) {
    return(get(".sparkRjsc", env=.sparkREnv))
  }

  # TODO: support other constructors
  assign(
    ".sparkRjsc",
     .jnew("org/apache/spark/api/java/JavaSparkContext", master, appName),
     env=.sparkREnv
  )

  get(".sparkRjsc", env=.sparkREnv)
}


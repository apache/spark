#
# Definitions of methods implemented by SparkRBackend
# Corresponding Java definition found in SparkRBackendInterface.scala
#

createSparkContext <- function(
  master,
  appName,
  sparkHome,
  jars,
  sparkEnvirMap=list(),
  sparkExecutorEnvMap=list()) {

  # Find a way to pass hash map?
  invokeJava("createSparkContext", master, appName, sparkHome,
             jars, sparkEnvirMap, sparkExecutorEnvMap)
}

stopBackend <- function() {
  invokeJava("stopBackend")
  invisible(NULL)

  # Also close the connection and remove it from our env
  close(conn)
  rm(".sparkRCon", envir=.sparkREnv)
}

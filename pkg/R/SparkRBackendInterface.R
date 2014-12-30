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

  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }

  conn <- get(".sparkRCon", .sparkREnv)

  rc <- rawConnection(raw(0), "r+")
  writeString(rc, "createSparkContext")
  writeString(rc, master)
  writeString(rc, appName)
  writeString(rc, sparkHome)
  writeVector(rc, jars)
  writeNamedList(rc, sparkEnvirMap)
  writeNamedList(rc, sparkExecutorEnvMap)

  bytesToSend <- rawConnectionValue(rc)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)

  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)

  appId <- readString(conn)
  appId
}

stopBackend <- function() {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }
  conn <- get(".sparkRCon", .sparkREnv)

  rc <- rawConnection(raw(0), "r+")
  writeString(rc, "stopBackend")

  bytesToSend <- rawConnectionValue(rc)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)

  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)
  invisible(NULL)

  # Also close the connection and remove it from our env
  close(conn)
  rm(".sparkRCon", envir=.sparkREnv)
}

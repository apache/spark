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

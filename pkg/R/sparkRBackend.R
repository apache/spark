# Methods to call into SparkRBackend. 
#

createSparkContext <- function(
  master,
  appName,
  sparkHome,
  jars=list(),
  sparkEnvirMap=new.env(),
  sparkExecutorEnvMap=new.env()) {

  invokeJava(
    isStatic=TRUE,
    objId="edu.berkeley.cs.amplab.sparkr.RRDD",
    methodName="createSparkContext",
    master, appName, sparkHome, jars, sparkEnvirMap, sparkExecutorEnvMap)
}

stopBackend <- function() {
  invokeJava(TRUE, "SparkRHandler", "stopBackend")

  # Also close the connection and remove it from our env
  conn <- get(".sparkRCon", .sparkREnv)
  close(conn)
  rm(".sparkRCon", envir=.sparkREnv)
}

# if isStatic is true, objId contains className
invokeJava <- function(isStatic, objId, methodName, ...) {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }

  rc <- rawConnection(raw(0), "r+")

  writeBoolean(rc, isStatic)
  # Write object id as string if it is static
  # else check if its a jobj and write its id
  if (isStatic) {
    writeString(rc, objId)
  } else {
    stopifnot(class(objId) == "jobj")
    writeString(rc, objId$id)
  }
  writeString(rc, methodName)

  #writeString(rpcName)
  writeArgs(rc, list(...))

  bytesToSend <- rawConnectionValue(rc)
  conn <- get(".sparkRCon", .sparkREnv)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)

  # TODO: check the status code to output error information
  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)
  readObject(conn)
}

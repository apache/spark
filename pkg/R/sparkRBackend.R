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

callJMethod <- function(objId, methodName, ...) {
  stopifnot(class(objId) == "jobj")
  invokeJava(isStatic=FALSE, objId, methodName, ...)
}

callJStatic <- function(className, methodName, ...) {
  invokeJava(isStatic=TRUE, className, methodName, ...)
}

newJObject <- function(className, ...) {
  invokeJava(isStatic=TRUE, className, methodName="new", ...)
}

removeJObject <- function(objId) {
  invokeJava(isStatic=TRUE, "SparkRHandler", "rm", objId)
}

isRemoveMethod <- function(isStatic, objId, methodName) {
  isStatic == TRUE && objId == "SparkRHandler" && methodName == "rm"
}

# If isStatic is true, objId contains className otherwise
# it should contain a jobj returned previously by the backend
invokeJava <- function(isStatic, objId, methodName, ...) {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }

  # If this is already isn't a removeJObject call
  if (!isRemoveMethod(isStatic, objId, methodName)) {
    objsToRemove <- ls(.toRemoveJobjs)
    if (length(objsToRemove) > 0) {
      #cat("Clearing up objsToRemove\n")
      sapply(objsToRemove,
            function(e) {
              removeJObject(e)
            })
      rm(list=objsToRemove, envir=.toRemoveJobjs)
    }
  }

  rc <- rawConnection(raw(0), "r+")

  writeBoolean(rc, isStatic)
  # Write object id as string if it is static
  # else check if its a jobj and write its id
  if (isStatic) {
    writeString(rc, objId)
  } else {
    writeString(rc, objId$id)
  }
  writeString(rc, methodName)

  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)

  bytesToSend <- rawConnectionValue(rc)

  conn <- get(".sparkRCon", .sparkREnv)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)

  # TODO: check the status code to output error information
  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)
  ret <- readObject(conn)

  close(rc) # TODO: Can we close this before ?

  ret
}

# Methods to call into SparkRBackend. 

# Call a Java method named methodName on the object
# specified by objId. objId should be a "jobj" returned
# from the SparkRBackend.
callJMethod <- function(objId, methodName, ...) {
  stopifnot(class(objId) == "jobj")
  invokeJava(isStatic=FALSE, objId, methodName, ...)
}

# Call a static method on a specified className
callJStatic <- function(className, methodName, ...) {
  invokeJava(isStatic=TRUE, className, methodName, ...)
}

# Create a new object of the specified class name
newJObject <- function(className, ...) {
  invokeJava(isStatic=TRUE, className, methodName="<init>", ...)
}

# Remove an object from the SparkR backend. This is done
# automatically when a jobj is garbage collected.
removeJObject <- function(objId) {
  invokeJava(isStatic=TRUE, "SparkRHandler", "rm", objId)
}

isRemoveMethod <- function(isStatic, objId, methodName) {
  isStatic == TRUE && objId == "SparkRHandler" && methodName == "rm"
}

# Invoke a Java method on the SparkR backend. Users
# should typically use one of the higher level methods like
# callJMethod, callJStatic etc. instead of using this.
#
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

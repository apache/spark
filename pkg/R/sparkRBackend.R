# Methods to call into SparkRBackend. 


# Returns TRUE if object is an instance of given class
isInstanceOf <- function(jobj, className) {
  stopifnot(class(jobj) == "jobj")
  cls <- SparkR:::callJStatic("java.lang.Class", "forName", className)
  SparkR:::callJMethod(cls, "isInstance", jobj)
}

# Call a Java method named methodName on the object
# specified by objId. objId should be a "jobj" returned
# from the SparkRBackend.
callJMethod <- function(objId, methodName, ...) {
  stopifnot(class(objId) == "jobj")
  invokeJava(isStatic = FALSE, objId$id, methodName, ...)
}

# Call a static method on a specified className
callJStatic <- function(className, methodName, ...) {
  invokeJava(isStatic = TRUE, className, methodName, ...)
}

# Create a new object of the specified class name
newJObject <- function(className, ...) {
  invokeJava(isStatic = TRUE, className, methodName = "<init>", ...)
}

# Remove an object from the SparkR backend. This is done
# automatically when a jobj is garbage collected.
removeJObject <- function(objId) {
  invokeJava(isStatic = TRUE, "SparkRHandler", "rm", objId)
}

isRemoveMethod <- function(isStatic, objId, methodName) {
  isStatic == TRUE && objId == "SparkRHandler" && methodName == "rm"
}

# Invoke a Java method on the SparkR backend. Users
# should typically use one of the higher level methods like
# callJMethod, callJStatic etc. instead of using this.
#
# isStatic - TRUE if the method to be called is static
# objId - String that refers to the object on which method is invoked
#         Should be a jobj id for non-static methods and the classname
#         for static methods
# methodName - name of method to be invoked
invokeJava <- function(isStatic, objId, methodName, ...) {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }

  # If this isn't a removeJObject call
  if (!isRemoveMethod(isStatic, objId, methodName)) {
    objsToRemove <- ls(.toRemoveJobjs)
    if (length(objsToRemove) > 0) {
      sapply(objsToRemove,
            function(e) {
              removeJObject(e)
            })
      rm(list = objsToRemove, envir = .toRemoveJobjs)
    }
  }

  rc <- rawConnection(raw(0), "r+")

  writeBoolean(rc, isStatic)
  writeString(rc, objId)
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

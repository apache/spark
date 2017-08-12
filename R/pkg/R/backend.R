#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Methods to call into SparkRBackend.


# Returns TRUE if object is an instance of given class
isInstanceOf <- function(jobj, className) {
  stopifnot(class(jobj) == "jobj")
  cls <- callJStatic("java.lang.Class", "forName", className)
  callJMethod(cls, "isInstance", jobj)
}

# Call a Java method named methodName on the object
# specified by objId. objId should be a "jobj" returned
# from the SparkRBackend.
callJMethod <- function(objId, methodName, ...) {
  stopifnot(class(objId) == "jobj")
  if (!isValidJobj(objId)) {
    stop("Invalid jobj ", objId$id,
         ". If SparkR was restarted, Spark operations need to be re-executed.")
  }
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
    stop("No connection to backend found. Please re-run sparkR.session()")
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

  # Construct the whole request message to send it once,
  # avoiding write-write-read pattern in case of Nagle's algorithm.
  # Refer to http://en.wikipedia.org/wiki/Nagle%27s_algorithm for the details.
  bytesToSend <- rawConnectionValue(rc)
  close(rc)
  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytesToSend))
  writeBin(bytesToSend, rc)
  requestMessage <- rawConnectionValue(rc)
  close(rc)

  conn <- get(".sparkRCon", .sparkREnv)
  writeBin(requestMessage, conn)

  returnStatus <- readInt(conn)
  handleErrors(returnStatus, conn)

  # Backend will send +1 as keep alive value to prevent various connection timeouts
  # on very long running jobs. See spark.r.heartBeatInterval
  while (returnStatus == 1) {
    returnStatus <- readInt(conn)
    handleErrors(returnStatus, conn)
  }

  readObject(conn)
}

# Helper function to check for returned errors and print appropriate error message to user
handleErrors <- function(returnStatus, conn) {
  if (length(returnStatus) == 0) {
    stop("No status is returned. Java SparkR backend might have failed.")
  }

  # 0 is success and +1 is reserved for heartbeats. Other negative values indicate errors.
  if (returnStatus < 0) {
    stop(readString(conn))
  }
}

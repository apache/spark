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

# References to objects that exist on the JVM backend
# are maintained using the jobj. 

#' @include generics.R
NULL

# Maintain a reference count of Java object references
# This allows us to GC the java object when it is safe
.validJobjs <- new.env(parent = emptyenv())

# List of object ids to be removed
.toRemoveJobjs <- new.env(parent = emptyenv())

# Check if jobj was created with the current SparkContext
isValidJobj <- function(jobj) {
  if (exists(".scStartTime", envir = .sparkREnv)) {
    jobj$appId == get(".scStartTime", envir = .sparkREnv)
  } else {
    FALSE
  }
}

getJobj <- function(objId) {
  newObj <- jobj(objId)
  if (exists(objId, .validJobjs)) {
    .validJobjs[[objId]] <- .validJobjs[[objId]] + 1
  } else {
    .validJobjs[[objId]] <- 1
  }
  newObj
}

# Handler for a java object that exists on the backend.
jobj <- function(objId) {
  if (!is.character(objId)) {
    stop("object id must be a character")
  }
  # NOTE: We need a new env for a jobj as we can only register
  # finalizers for environments or external references pointers.
  obj <- structure(new.env(parent = emptyenv()), class = "jobj")
  obj$id <- objId
  obj$appId <- get(".scStartTime", envir = .sparkREnv)

  # Register a finalizer to remove the Java object when this reference
  # is garbage collected in R
  reg.finalizer(obj, cleanup.jobj)
  obj
}

#' Print a JVM object reference.
#'
#' This function prints the type and id for an object stored
#' in the SparkR JVM backend.
#'
#' @param x The JVM object reference
#' @param ... further arguments passed to or from other methods
print.jobj <- function(x, ...) {
  cls <- callJMethod(x, "getClass")
  name <- callJMethod(cls, "getName")
  cat("Java ref type", name, "id", x$id, "\n", sep = " ")
}

cleanup.jobj <- function(jobj) {
  if (isValidJobj(jobj)) {
    objId <- jobj$id
    # If we don't know anything about this jobj, ignore it
    if (exists(objId, envir = .validJobjs)) {
      .validJobjs[[objId]] <- .validJobjs[[objId]] - 1

      if (.validJobjs[[objId]] == 0) {
        rm(list = objId, envir = .validJobjs)
        # NOTE: We cannot call removeJObject here as the finalizer may be run
        # in the middle of another RPC. Thus we queue up this object Id to be removed
        # and then run all the removeJObject when the next RPC is called.
        .toRemoveJobjs[[objId]] <- 1
      }
    }
  }
}

clearJobjs <- function() {
  valid <- ls(.validJobjs)
  rm(list = valid, envir = .validJobjs)

  removeList <- ls(.toRemoveJobjs)
  rm(list = removeList, envir = .toRemoveJobjs)
}

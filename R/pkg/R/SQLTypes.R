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

# Utility functions for handling SparkSQL DataTypes.

# Handler for StructType
structType <- function(st) {
  obj <- structure(new.env(parent = emptyenv()), class = "structType")
  obj$jobj <- st
  obj$fields <- function() { lapply(callJMethod(st, "fields"), structField) }
  obj
}

#' Print a Spark StructType.
#'
#' This function prints the contents of a StructType returned from the
#' SparkR JVM backend.
#'
#' @param x A StructType object
#' @param ... further arguments passed to or from other methods
print.structType <- function(x, ...) {
  fieldsList <- lapply(x$fields(), function(i) { i$print() })
  print(fieldsList)
}

# Handler for StructField
structField <- function(sf) {
  obj <- structure(new.env(parent = emptyenv()), class = "structField")
  obj$jobj <- sf
  obj$name <- function() { callJMethod(sf, "name") }
  obj$dataType <- function() { callJMethod(sf, "dataType") }
  obj$dataType.toString <- function() { callJMethod(obj$dataType(), "toString") }
  obj$dataType.simpleString <- function() { callJMethod(obj$dataType(), "simpleString") }
  obj$nullable <- function() { callJMethod(sf, "nullable") }
  obj$print <- function() { paste("StructField(", 
                     paste(obj$name(), obj$dataType.toString(), obj$nullable(), sep = ", "),
                     ")", sep = "") }
  obj
}

#' Print a Spark StructField.
#'
#' This function prints the contents of a StructField returned from the
#' SparkR JVM backend.
#'
#' @param x A StructField object
#' @param ... further arguments passed to or from other methods
print.structField <- function(x, ...) {
  cat(x$print())
}

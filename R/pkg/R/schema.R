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

# A set of S3 classes and methods that support the SparkSQL `StructType` and `StructField
# datatypes. These are used to create and interact with DataFrame schemas.

#' structType
#'
#' Create a structType object that contains the metadata for a DataFrame. Intended for
#' use with createDataFrame and toDF.
#'
#' @param x a structField object (created with the field() function)
#' @param ... additional structField objects
#' @return a structType object
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
#' schema <- structType(structField("a", "integer"), structField("b", "string"))
#' df <- createDataFrame(sqlCtx, rdd, schema)
#' }
structType <- function(x, ...) {
  UseMethod("structType", x)
}

structType.jobj <- function(x) {
  obj <- structure(list(), class = "structType")
  obj$jobj <- x
  obj$fields <- function() { lapply(callJMethod(obj$jobj, "fields"), structField) }
  obj
}

structType.structField <- function(x, ...) {
  fields <- list(x, ...)
  if (!all(sapply(fields, inherits, "structField"))) {
    stop("All arguments must be structField objects.")
  }
  sfObjList <- lapply(fields, function(field) {
    field$jobj
  })
  stObj <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructType",
                       listToSeq(sfObjList))
  structType(stObj)
}

#' Print a Spark StructType.
#'
#' This function prints the contents of a StructType returned from the
#' SparkR JVM backend.
#'
#' @param x A StructType object
#' @param ... further arguments passed to or from other methods
print.structType <- function(x, ...) {
  cat("StructType\n",
      sapply(x$fields(),
             function(field) {
               paste("|-", "name = \"", field$name(),
                     "\", type = \"", field$dataType.toString(),
                     "\", nullable = ", field$nullable(), "\n",
                     sep = "")
             }),
      sep = "")
}

#' structField
#'
#' Create a structField object that contains the metadata for a single field in a schema.
#'
#' @param x The name of the field
#' @param type The data type of the field
#' @param nullable A logical vector indicating whether or not the field is nullable
#' @return a structField object
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' rdd <- lapply(parallelize(sc, 1:10), function(x) { list(x, as.character(x)) })
#' field1 <- structField("a", "integer", TRUE)
#' field2 <- structField("b", "string", TRUE)
#' schema <- structType(field1, field2)
#' df <- createDataFrame(sqlCtx, rdd, schema)
#' }

structField <- function(x, ...) {
  UseMethod("structField", x)
}

structField.jobj <- function(x) {
  obj <- structure(list(), class = "structField")
  obj$jobj <- x
  obj$name <- function() { callJMethod(x, "name") }
  obj$dataType <- function() { callJMethod(x, "dataType") }
  obj$dataType.toString <- function() { callJMethod(obj$dataType(), "toString") }
  obj$dataType.simpleString <- function() { callJMethod(obj$dataType(), "simpleString") }
  obj$nullable <- function() { callJMethod(x, "nullable") }
  obj
}

structField.character <- function(x, type, nullable = TRUE) {
  if (class(x) != "character") {
    stop("Field name must be a string.")
  }
  if (class(type) != "character") {
    stop("Field type must be a string.")
  }
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }
  options <- c("byte",
               "integer",
               "float",
               "double",
               "numeric",
               "character",
               "string",
               "binary",
               "raw",
               "logical",
               "boolean",
               "timestamp",
               "date")
  dataType <- if (type %in% options) {
    type
  } else {
    stop(paste("Unsupported type for Dataframe:", type))
  }
  sfObj <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructField",
                       x,
                       dataType,
                       nullable)
  structField(sfObj)
}

#' Print a Spark StructField.
#'
#' This function prints the contents of a StructField returned from the
#' SparkR JVM backend.
#'
#' @param x A StructField object
#' @param ... further arguments passed to or from other methods
print.structField <- function(x, ...) {
  cat("StructField(name = \"", x$name(),
      "\", type = \"", x$dataType.toString(),
      "\", nullable = ", x$nullable(),
      ")",
      sep = "")
}

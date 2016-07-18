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
# datatypes. These are used to create and interact with SparkDataFrame schemas.

#' structType
#'
#' Create a structType object that contains the metadata for a SparkDataFrame. Intended for
#' use with createDataFrame and toDF.
#'
#' @param x a structField object (created with the field() function)
#' @param ... additional structField objects
#' @return a structType object
#' @rdname structType
#' @export
#' @examples
#'\dontrun{
#' schema <-  structType(structField("a", "integer"), structField("c", "string"),
#'                       structField("avg", "double"))
#' df1 <- gapply(df, list("a", "c"),
#'               function(key, x) { y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE) },
#'               schema)
#' }
#' @note structType since 1.4.0
structType <- function(x, ...) {
  UseMethod("structType", x)
}

#' @rdname structType
#' @method structType jobj
#' @export
structType.jobj <- function(x, ...) {
  obj <- structure(list(), class = "structType")
  obj$jobj <- x
  obj$fields <- function() { lapply(callJMethod(obj$jobj, "fields"), structField) }
  obj
}

#' @rdname structType
#' @method structType structField
#' @export
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
                       sfObjList)
  structType(stObj)
}

#' Print a Spark StructType.
#'
#' This function prints the contents of a StructType returned from the
#' SparkR JVM backend.
#'
#' @param x A StructType object
#' @param ... further arguments passed to or from other methods
#' @note print.structType since 1.4.0
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
#' @return a structField object
#' @rdname structField
#' @export
#' @examples
#'\dontrun{
#' field1 <- structField("a", "integer")
#' field2 <- structField("c", "string")
#' field3 <- structField("avg", "double")
#' schema <-  structType(field1, field2, field3)
#' df1 <- gapply(df, list("a", "c"),
#'               function(key, x) { y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE) },
#'               schema)
#' }
#' @note structField since 1.4.0
structField <- function(x, ...) {
  UseMethod("structField", x)
}

#' @rdname structField
#' @method structField jobj
#' @export
structField.jobj <- function(x, ...) {
  obj <- structure(list(), class = "structField")
  obj$jobj <- x
  obj$name <- function() { callJMethod(x, "name") }
  obj$dataType <- function() { callJMethod(x, "dataType") }
  obj$dataType.toString <- function() { callJMethod(obj$dataType(), "toString") }
  obj$dataType.simpleString <- function() { callJMethod(obj$dataType(), "simpleString") }
  obj$nullable <- function() { callJMethod(x, "nullable") }
  obj
}

checkType <- function(type) {
  if (!is.null(PRIMITIVE_TYPES[[type]])) {
    return()
  } else {
    # Check complex types
    firstChar <- substr(type, 1, 1)
    switch (firstChar,
            a = {
              # Array type
              m <- regexec("^array<(.+)>$", type)
              matchedStrings <- regmatches(type, m)
              if (length(matchedStrings[[1]]) >= 2) {
                elemType <- matchedStrings[[1]][2]
                checkType(elemType)
                return()
              }
            },
            m = {
              # Map type
              m <- regexec("^map<(.+),(.+)>$", type)
              matchedStrings <- regmatches(type, m)
              if (length(matchedStrings[[1]]) >= 3) {
                keyType <- matchedStrings[[1]][2]
                if (keyType != "string" && keyType != "character") {
                  stop("Key type in a map must be string or character")
                }
                valueType <- matchedStrings[[1]][3]
                checkType(valueType)
                return()
              }
            },
            s = {
              # Struct type
              m <- regexec("^struct<(.+)>$", type)
              matchedStrings <- regmatches(type, m)
              if (length(matchedStrings[[1]]) >= 2) {
                fieldsString <- matchedStrings[[1]][2]
                # strsplit does not return the final empty string, so check if
                # the final char is ","
                if (substr(fieldsString, nchar(fieldsString), nchar(fieldsString)) != ",") {
                  fields <- strsplit(fieldsString, ",")[[1]]
                  for (field in fields) {
                    m <- regexec("^(.+):(.+)$", field)
                    matchedStrings <- regmatches(field, m)
                    if (length(matchedStrings[[1]]) >= 3) {
                      fieldType <- matchedStrings[[1]][3]
                      checkType(fieldType)
                    } else {
                      break
                    }
                  }
                  return()
                }
              }
            })
  }

  stop(paste("Unsupported type for SparkDataframe:", type))
}

#' @param type The data type of the field
#' @param nullable A logical vector indicating whether or not the field is nullable
#' @rdname structField
#' @export
structField.character <- function(x, type, nullable = TRUE, ...) {
  if (class(x) != "character") {
    stop("Field name must be a string.")
  }
  if (class(type) != "character") {
    stop("Field type must be a string.")
  }
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }

  checkType(type)

  sfObj <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructField",
                       x,
                       type,
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
#' @note print.structField since 1.4.0
print.structField <- function(x, ...) {
  cat("StructField(name = \"", x$name(),
      "\", type = \"", x$dataType.toString(),
      "\", nullable = ", x$nullable(),
      ")",
      sep = "")
}

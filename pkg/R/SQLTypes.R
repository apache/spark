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

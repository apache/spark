# Utility functions for handling Spark DataTypes.


structType <- function(st) {
  obj <- structure(new.env(parent = emptyenv()), class = "structType")
  obj$jobj <- st
  obj$fields <- lapply(SparkR:::callJMethod(st, "fields"), structField)
  obj
}

print.structType <- function(x, ...) {
  fieldsList <- lapply(x$fields, function(i) i$print)
  print(fieldsList)
}

structField <- function(sf) {
  obj <- structure(new.env(parent = emptyenv()), class = "structField")
  obj$jobj <- sf
  obj$name <- SparkR:::callJMethod(sf, "name")
  obj$dataType <- SparkR:::callJMethod(sf, "dataType")
  obj$dataType.toString <- SparkR:::callJMethod(obj$dataType, "toString")
  obj$dataType.simpleString <- SparkR:::callJMethod(obj$dataType, "simpleString")
  obj$nullable <- SparkR:::callJMethod(sf, "nullable")
  obj$print <- paste("StructField(", 
                     paste(obj$name, obj$dataType.toString, obj$nullable, sep = ", "),
                     ")", sep = "")
  obj
}

print.structField <- function(x, ...) {
  cat(x$print)
}
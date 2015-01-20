# Utility functions to serialize, deserialize etc.

writeString <- function(con, value) {
  writeInt(con, as.integer(nchar(value) + 1))
  writeBin(value, con, endian="big")
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian="big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian="big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRawSerialize <- function(outputCon, batch) {
  outputSer <- serialize(batch, ascii = FALSE, conn = NULL)
  writeRaw(outputCon, outputSer)
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian="big")
}

writeType <- function(con, class) {
  type <- switch(class,
            integer = "i",
            character = "c",
            logical = "b",
            double = "d",
            numeric = "d",
            raw = "r",
            list = "l",
            jobj = "j",
            environment = "e",
            stop("Unsupported type for serialization"))
  writeBin(charToRaw(type), con)
}

writeObject <- function(con, object, withType = TRUE) {
  # In R vectors have same type as objects. So check if this is a vector
  # and if so just call writeVector
  # TODO: Should we just throw an exception here instead ?
  if (withType) {
    writeType(con, class(object))
  }
  switch(class(object),
    integer = writeInt(con, object),
    character = writeString(con, object),
    logical = writeBoolean(con, object),
    double = writeDouble(con, object),
    numeric = writeDouble(con, object),
    raw = writeRaw(con, object),
    list = writeList(con, object),
    jobj = writeString(con, object$id),
    environment = writeEnv(con, object),
    stop("Unsupported type for serialization"))
}

# Used to pass arrays
writeList <- function(con, arr) {
  # All elements should be of same type
  elemType <- unique(sapply(arr, function(elem) { class(elem) }))
  stopifnot(length(elemType) <= 1)

  # Write empty lists as strings ?
  if (length(elemType) == 0) {
    elemType <- "character"
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass named lists as hash maps and lists as vectors
#
# We don't support pass in heterogenous lists.
# Note that all the elements of the list should be of same type 
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeList(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) { env[[x]] })
    writeList(con, as.list(vals))
  }
}

writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}

writeStrings <- function(con, stringList) {
  writeLines(unlist(stringList), con)
}

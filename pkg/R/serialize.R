# Utility functions to serialize, deserialize etc.

writeString <- function(con, value, withType = FALSE) {
  if (withType) {
    writeString(con, typeof(value))
  }
  writeInt(con, as.integer(nchar(value) + 1))
  writeBin(value, con, endian="big")
}

writeInt <- function(con, value, withType = FALSE) {
  if (withType) {
    writeString(con, typeof(value))
  }
  writeBin(as.integer(value), con, endian="big")
}

writeDouble <- function(con, value, withType = FALSE) {
  if (withType) {
    writeString(con, typeof(value))
  }
  writeBin(value, con, endian="big")
}

writeBoolean <- function(con, value, withType = FALSE) {
  if (withType) {
    writeString(con, typeof(value))
  }
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRaw <- function(con, batch, serialized = FALSE, withType = FALSE) {
  if (serialized) {
    outputSer <- batch
  } else {
    outputSer <- serialize(batch, ascii = FALSE, conn = NULL)
  }
  if (withType) {
    writeString(con, typeof(outputSer))
  }
  writeInt(con, length(outputSer))
  writeBin(outputSer, con, endian="big")
}

writeObject <- function(con, object, withType = FALSE) {
  switch(typeof(object),
    integer = writeInt(con, object, withType),
    character = writeString(con, object, withType),
    logical = writeBoolean(con, object, withType),
    double = writeDouble(con, object, withType),
    raw = writeRaw(con, object, withType),
    stop("Unsupported type for serialization"))
}

# Used to pass arrays
writeVector <- function(con, arr, withType = FALSE) {
  if (!is.vector(arr) || typeof(arr) == "list") {
    stop("writeVector cannot be used for non-vectors or lists")
  }
  if (withType) {
    writeString(con, "vector")
  }
  writeInt(con, length(arr))
  writeString(con, typeof(arr))
  if (length(arr) > 0) { 
    for (a in arr) {
      writeObject(con, a)
    }
  }
}

writeList <- function(con, list, withType = FALSE) {
  if (withType) {
    writeString(con, "list")
  }
  writeInt(con, length(list))
  if (length(list) > 0) { 
    for (a in list) {
      writeObject(con, a, TRUE)
    }
  }
}

# Used to pass named lists as hash maps
# Note that all the elements of the list should be of same type 
writeNamedList <- function(con, namedlist) {
  len <- length(namedlist)
  writeInt(con, len)
  if (len > 0) {
    # All elements should be of same type
    elemType <- unique(sapply(namedlist, function(elem) { typeof(elem) }))
    stopifnot(length(elemType) == 1)
 
    writeString(con, elemType)
    names <- names(namedlist)
 
    writeVector(con, names)
    writeVector(con, unlist(namedlist, use.names=F, recursive=F))
  }
}

# Used for writing out a hashed-by-key pairwise RDD.
writeEnvironment <- function(con, e, keyValPairsSerialized = TRUE) {
  writeInt(con, length(e))
  for (bucketNum in ls(e)) {
    writeInt(con, bucketNum)
    writeInt(con, length(e[[bucketNum]]))
    for (tuple in e[[bucketNum]]) {
      writeRaw(con, tuple[[1]], keyValPairsSerialized)
      writeRaw(con, tuple[[2]], keyValPairsSerialized)
    }
  }
}

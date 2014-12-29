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

writeRaw <- function(con, batch, serialized = FALSE) {
  if (serialized) {
    outputSer <- batch
  } else {
    outputSer <- serialize(batch, ascii = FALSE, conn = NULL)
  }
  writeInt(con, length(outputSer))
  writeBin(outputSer, con, endian="big")
}

# Used to pass arrays
writeVector <- function(con, arr) {
  if (!is.vector(arr) || typeof(arr) == "list") {
    stop("writeVector cannot be used for non-vectors or lists")
  }
  writeInt(con, length(arr))
  writeString(con, typeof(arr))
  if (length(arr) > 0) { 
    if (typeof(arr) == "integer") {
      for (a in arr) {
        writeInt(con, a)
      }
    } else if (typeof(arr) == "character") {
      for (a in arr) {
        writeString(con, a)
      }
    } else if (typeof(arr) == "logical") {
      for (a in arr) {
        writeBoolean(con, a)
      }
    } else if (typeof(arr) == "double") {
      for (a in arr) {
        writeDouble(con, a)
      }
    } else if (typeof(arr) == "raw") {
      writeBin(con, arr, endian="big")
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

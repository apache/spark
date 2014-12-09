# Utility functions to serialize, deserialize etc.

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian="big")
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  data <- readBin(con, raw(), as.integer(dataLen), endian="big")
}

readRawLen <- function(con, dataLen) {
  data <- readBin(con, raw(), as.integer(dataLen), endian="big")
}

readDeserialize <- function(con) {
  # We have two cases that are possible - In one, the entire partition is
  # encoded as a byte array, so we have only one value to read. If so just
  # return firstData
  dataLen <- readInt(con)
  firstData <- unserialize(
      readBin(con, raw(), as.integer(dataLen), endian="big"))

  # Else, read things into a list
  dataLen <- readInt(con)
  if (length(dataLen) > 0 && dataLen > 0) {
    data <- list(firstData)
    while (length(dataLen) > 0 && dataLen > 0) {
      data[[length(data) + 1L]] <- unserialize(
          readBin(con, raw(), as.integer(dataLen), endian="big"))
      dataLen <- readInt(con)
    }
    unlist(data, recursive = FALSE)
  } else {
    firstData
  }
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian="big")
  rawToChar(string)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian="big")
}

writeStrings <- function(con, stringList) {
  writeLines(unlist(stringList), con)
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

# Fast append to list by using an accumulator.
# http://stackoverflow.com/questions/17046336/here-we-go-again-append-an-element-to-a-list-in-r
#
# The accumulator should has three fields size, counter and data.
# This function amortizes the allocation cost by doubling
# the size of the list every time it fills up.
addItemToAccumulator <- function(acc, item) {
  if(acc$counter == acc$size) {
    acc$size <- acc$size * 2
    length(acc$data) <- acc$size
  }
  acc$counter <- acc$counter + 1
  acc$data[[acc$counter]] <- item
}

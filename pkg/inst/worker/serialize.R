# Utility functions to serialize, deserialize etc.

readInt <- function(con) {
  r <- readBin(con, integer(), n = 1, endian="big")
  write("** in readInt: ", stderr())
  write(r, stderr())
  write("**", stderr())
  r
}

readRaw <- function(con) {
  dataLen <- readInt(con)

  write("**in readRaw, dataLen: ", stderr())
  write(dataLen, stderr())
  write("**", stderr())
  write(as.character(mode(dataLen)), stderr())

  data <- readBin(con, raw(), as.integer(dataLen), endian="big")
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian="big")
  rawToChar(string)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian="big")
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

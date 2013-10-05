# Utility functions to serialize, deserialize etc.

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian="big")
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  data <- readBin(con, raw(), dataLen, endian="big")
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian="big")
  rawToChar(string)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian="big")
}

writeRaw <- function(con, batch) {
  outputSer <- serialize(batch, ascii = FALSE, conn = NULL)
  writeInt(con, length(outputSer))
  writeBin(outputSer, con, endian="big")
}

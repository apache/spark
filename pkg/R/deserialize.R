# Utility functions to serialize, deserialize etc.

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian="big")
  rawToChar(string)
}

readInt <- function(con, num = 1) {
  readBin(con, integer(), n = num, endian="big")
}

readDouble <- function(con, num = 1) {
  readBin(con, double(), n = num, endian="big")
}

readBoolean <- function(con, num = 1) {
  as.logical(readInt(con, num))
}

readVector <- function(con) {
  len <- readInt(con)
  type <- readString(con)
  if (length > 0) {
    if (type == "integer") {
      out <- readInt(con, num=len) 
    } else if (type == "character") {
      out <- sapply(1:len, function(x) {
        readString(con)
      })
    } else if (type == "logical") {
      out <- readBoolean(con, num=len)
    } else if (type == "double") {
      out <- readDouble(con, num=len)
    } else if (type == "raw") {
      out <- readRawLen(con, len)
    } else {
    }
    out
  } else {
    vector(mode=type)
  }
}

readNamedList <- function(con) {
  len <- readInt(con)
  if (len > 0) {
    # TODO: This is not used ?
    elemType <- readString(con)
    names <- readVector(con)
    vals <- readVector(con)

    out <- as.list(vals)
    names(out) <- names
    out
  } else {
    list()
  }
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


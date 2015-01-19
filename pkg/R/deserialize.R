# Utility functions to serialize, deserialize etc.

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian="big")
  rawToChar(string)
}

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian="big")
}

readDouble <- function(con) {
  readBin(con, double(), n = 1, endian="big")
}

readBoolean <- function(con) {
  as.logical(readInt(con))
}

readObject <- function(con) {
  # Read type first
  type <- readString(con)
  readObjectType(con, type)
}

readObjectType <- function(con, type) {
  switch (type,
    integer = readInt(con),
    character = readString(con),
    logical = readBoolean(con),
    double = readDouble(con),
    raw = readRaw(con),
    vector = readVector(con),
    list = readList(con),
    void = NULL,
    jobj = jobj(readString(con)),
    stop("Unsupported type for deserialization"))
}

# TODO: We don't use readVector as it is tricky
# to assembly array of raw objects. Delete this ?
readVector <- function(con) {
  type <- readString(con)
  len <- readInt(con)
  if (length > 0) {
    sapply(1:len, readObjectType(con, type))
  } else {
    vector(mode=type)
  }
}

# We only support lists where all elements are of same type
readList <- function(con) {
  type <- readString(con)
  len <- readInt(con)
  if (length > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readObjectType(con, type)
    }
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


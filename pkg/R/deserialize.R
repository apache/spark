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

readType <- function(con) {
  rawToChar(readBin(con, "raw", n = 1L))
}

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch (type,
    "i" = readInt(con),
    "c" = readString(con),
    "b" = readBoolean(con),
    "d" = readDouble(con),
    "r" = readRaw(con),
    "v" = readVector(con),
    "l" = readList(con),
    "n" = NULL,
    "j" = getJobj(readString(con)),
    stop("Unsupported type for deserialization"))
}

# TODO: We don't use readVector as it is tricky
# to assembly array of raw objects. Delete this ?
readVector <- function(con) {
  type <- readType(con)
  len <- readInt(con)
  if (length > 0) {
    sapply(1:len, readTypedObject(con, type))
  } else {
    vector(mode=type)
  }
}

# We only support lists where all elements are of same type
readList <- function(con) {
  type <- readType(con)
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
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


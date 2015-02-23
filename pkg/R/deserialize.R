# Utility functions to deserialize objects from Java.

# Type mapping from Java to R
# 
# void -> NULL
# Int -> integer
# String -> character
# Boolean -> logical
# Double -> double
# Long -> double
# Array[Byte] -> raw
#
# Array[T] -> list()
# Object -> jobj

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
    "l" = readList(con),
    "n" = NULL,
    "j" = getJobj(readString(con)),
    stop("Unsupported type for deserialization"))
}

readString <- function(con) {
  stringLen <- readInt(con)
  string <- readBin(con, raw(), stringLen, endian = "big")
  rawToChar(string)
}

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian = "big")
}

readDouble <- function(con) {
  readBin(con, double(), n = 1, endian = "big")
}

readBoolean <- function(con) {
  as.logical(readInt(con))
}

readType <- function(con) {
  rawToChar(readBin(con, "raw", n = 1L))
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
  data <- readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readRawLen <- function(con, dataLen) {
  data <- readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readDeserialize <- function(con) {
  # We have two cases that are possible - In one, the entire partition is
  # encoded as a byte array, so we have only one value to read. If so just
  # return firstData
  dataLen <- readInt(con)
  firstData <- unserialize(
      readBin(con, raw(), as.integer(dataLen), endian = "big"))

  # Else, read things into a list
  dataLen <- readInt(con)
  if (length(dataLen) > 0 && dataLen > 0) {
    data <- list(firstData)
    while (length(dataLen) > 0 && dataLen > 0) {
      data[[length(data) + 1L]] <- unserialize(
          readBin(con, raw(), as.integer(dataLen), endian = "big"))
      dataLen <- readInt(con)
    }
    unlist(data, recursive = FALSE)
  } else {
    firstData
  }
}

readDeserializeRows <- function(inputCon) {
  # readDeserializeRows will deserialize a DataOutputStream composed of
  # a list of lists. Since the DOS is one continuous stream and
  # the number of rows varies, we put the readRow function in a while loop
  # that termintates when the next row is empty.
  colNames <- readList(inputCon)

  data <- list()
  numCols <- readInt(inputCon)
  # We write a length for each row out
  while(length(numCols) > 0 && numCols > 0) {
    data[[length(data) + 1L]] <- readRow(inputCon, numCols)
    numCols <- readInt(inputCon)
  }
  dataOut <- lapply(data, assignNames, colNames)
  dataOut # this is a list of named lists now
}

readRowList <- function(obj) {
  # readRowList is meant for use inside an lapply. As a result, it is
  # necessary to open a standalone connection for the row and consume
  # the numCols bytes inside the read function in order to correctly
  # deserialize the row.
  rawObj <- rawConnection(obj, "r+")
  numCols <- SparkR:::readInt(rawObj)
  rowOut <- SparkR:::readRow(rawObj, numCols)
  close(rawObj)
  rowOut
}

readRow <- function(inputCon, numCols) {
  lapply(1:numCols, function(x) {
    obj <- readObject(inputCon)
    if (is.null(obj)) {
      NA
    } else {
      obj
    }
  }) # each row is a list now
}

assignNames <- function(row, colNames) {
  names(row) <- colNames
  row
}

# Take a single column as Array[Byte] and deserialize it into an atomic vector
readCol <- function(inputCon, numRows) {
  sapply(1:numRows, function(x) {
    value <- readObject(inputCon)
    # Replace NULL with NA so we can coerce to vectors
    if (is.null(value)) NA else value
  }) # each column is an atomic vector now
}

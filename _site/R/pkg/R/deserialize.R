#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
# Date -> Date
# Time -> POSIXct
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
    "D" = readDate(con),
    "t" = readTime(con),
    "l" = readList(con),
    "n" = NULL,
    "j" = getJobj(readString(con)),
    stop(paste("Unsupported type for deserialization", type)))
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

readDate <- function(con) {
  as.Date(readString(con))
}

readTime <- function(con) {
  t <- readDouble(con)
  as.POSIXct(t, origin = "1970-01-01")
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
  data <- list()
  while(TRUE) {
    row <- readRow(inputCon)
    if (length(row) == 0) {
      break
    }
    data[[length(data) + 1L]] <- row
  }
  data # this is a list of named lists now
}

readRowList <- function(obj) {
  # readRowList is meant for use inside an lapply. As a result, it is
  # necessary to open a standalone connection for the row and consume
  # the numCols bytes inside the read function in order to correctly
  # deserialize the row.
  rawObj <- rawConnection(obj, "r+")
  on.exit(close(rawObj))
  readRow(rawObj)
}

readRow <- function(inputCon) {
  numCols <- readInt(inputCon)
  if (length(numCols) > 0 && numCols > 0) {
    lapply(1:numCols, function(x) {
      obj <- readObject(inputCon)
      if (is.null(obj)) {
        NA
      } else {
        obj
      }
    }) # each row is a list now
  } else {
    list()
  }
}

# Take a single column as Array[Byte] and deserialize it into an atomic vector
readCol <- function(inputCon, numRows) {
  # sapply can not work with POSIXlt
  do.call(c, lapply(1:numRows, function(x) {
    value <- readObject(inputCon)
    # Replace NULL with NA so we can coerce to vectors
    if (is.null(value)) NA else value
  }))
}

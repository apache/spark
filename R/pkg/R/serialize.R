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

# Utility functions to serialize R objects so they can be read in Java.

# nolint start
# Type mapping from R to Java
#
# NULL -> Void
# integer -> Int
# character -> String
# logical -> Boolean
# double, numeric -> Double
# raw -> Array[Byte]
# Date -> Date
# POSIXct,POSIXlt -> Time
#
# list[T] -> Array[T], where T is one of above mentioned types
# Multi-element vector of any of the above (except raw) -> Array[T]
# environment -> Map[String, T], where T is a native type
# jobj -> Object, where jobj is an object created in the backend
# nolint end

getSerdeType <- function(object) {
  type <- class(object)[[1]]
  if (is.atomic(object) & !is.raw(object) & length(object) > 1) {
    "array"
  } else if (type != "list") {
     type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) { getSerdeType(elem) }))
    if (length(elemType) <= 1) {
      "array"
    } else {
      "list"
    }
  }
}

writeObject <- function(con, object, writeType = TRUE) {
  # NOTE: In R vectors have same type as objects
  type <- class(object)[[1]]  # class of POSIXlt is c("POSIXlt", "POSIXt")
  # Checking types is needed here, since 'is.na' only handles atomic vectors,
  # lists and pairlists
  if (type %in% c("integer", "character", "logical", "double", "numeric")) {
    if (is.na(object[[1]])) {
      # Uses the first element for now to keep the behavior same as R before
      # 4.2.0. This is wrong because we should differenciate c(NA) from a
      # single NA as the former means array(null) and the latter means null
      # in Spark SQL. However, it requires non-trivial comparison to distinguish
      # both in R. We should ideally fix this.
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(serdeType,
         NULL = writeVoid(con),
         integer = writeInt(con, object),
         character = writeString(con, object),
         logical = writeBoolean(con, object),
         double = writeDouble(con, object),
         numeric = writeDouble(con, object),
         raw = writeRaw(con, object),
         array = writeArray(con, object),
         list = writeList(con, object),
         struct = writeList(con, object),
         jobj = writeJobj(con, object),
         environment = writeEnv(con, object),
         Date = writeDate(con, object),
         POSIXlt = writeTime(con, object),
         POSIXct = writeTime(con, object),
         stop("Unsupported type for serialization ", type))
}

writeVoid <- function(con) {
  # no value for NULL
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(value)
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRawSerialize <- function(outputCon, batch) {
  outputSer <- serialize(batch, ascii = FALSE, connection = NULL)
  writeRaw(outputCon, outputSer)
}

writeRowSerialize <- function(outputCon, rows) {
  invisible(lapply(rows, function(r) {
    bytes <- serializeRow(r)
    writeRaw(outputCon, bytes)
  }))
}

serializeRow <- function(row) {
  rawObj <- rawConnection(raw(0), "wb")
  on.exit(close(rawObj))
  writeList(rawObj, row)
  rawConnectionValue(rawObj)
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

writeType <- function(con, class) {
  type <- switch(class,
                 NULL = "n",
                 integer = "i",
                 character = "c",
                 logical = "b",
                 double = "d",
                 numeric = "d",
                 raw = "r",
                 array = "a",
                 list = "l",
                 struct = "s",
                 jobj = "j",
                 environment = "e",
                 Date = "D",
                 POSIXlt = "t",
                 POSIXct = "t",
                 stop("Unsupported type for serialization ", class))
  writeBin(charToRaw(type), con)
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) { env[[x]] })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeString(con, as.character(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}

writeSerializeInArrow <- function(conn, df) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    # There looks no way to send each batch in streaming format via socket
    # connection. See ARROW-4512.
    # So, it writes the whole Arrow streaming-formatted binary at once for now.
    writeRaw(conn, arrow::write_to_raw(df))
  } else {
    stop("'arrow' package should be installed.")
  }
}

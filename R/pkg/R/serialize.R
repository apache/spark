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

# TODO: this can be replaced by a single has_unique_serde_type function
#       that simply returns TRUE/FALSE
getSerdeType <- function(object) {
  type <- class(object)[[1L]]
  if (is.atomic(object) && !is.raw(object) && length(object) > 1L) {
    "array"
  } else if (type != "list") {
     type
  } else {
    if (has_unique_serde_type(object)) {
      "array"
    } else {
      "list"
    }
  }
}

has_unique_serde_type <- function(object) {
  # i.e., either length-0 or length-1
  length(unique(sapply(object, getSerdeType))) <= 1L
}

# NOTE: In R vectors have same type as objects
# NOTE: handle writeType in the respective methods because of
#         some minor idiosyncrasies, e.g. handling of writeObject(list(), con)
writeObject <- function(object, con, writeType = TRUE) UseMethod("writeObject")
writeObject.default <- function(object, con, writeType = TRUE) {
  stop(paste("Unsupported type for serialization", class(object)))
}
writeObject.NULL <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
}

# integer same as logical; will cast TRUE -> 1, FALSE -> 0
writeObject.integer <-
writeObject.logical <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  # non-scalar value written as array
  if (length(object) > 1L) {
    writeObject(length(object), con, writeType = FALSE)
  } else if (is.na(object)) return() # no value for NULL

  for (elem in object) writeBin(as.integer(elem), con, endian = "big")
}

writeObject.character <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  # non-scalar value written as array
  if (length(object) > 1L) {
    writeObject(length(object), con, writeType = FALSE)
  } else if (is.na(object)) return() # no value for NULL

  utfVal <- enc2utf8(object)
  for (elem in object) {
    writeObject(as.integer(nchar(elem, type = "bytes") + 1L), con, writeType = FALSE)
    writeBin(elem, con, endian = "big", useBytes = TRUE)
  }
}

writeObject.numeric <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  # non-scalar value written as array
  if (length(object) > 1L) {
    writeObject(length(object), con, writeType = FALSE)
  } else if (is.na(object)) return() # no value for NULL

  for (elem in object) writeBin(elem, con, endian = "big")
}

writeObject.raw <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  writeObject(length(object), con, writeType = FALSE)
  writeBin(object, con, endian = "big")
}

writeObject.struct <-
writeObject.list <- function(object, con, writeType = TRUE) {
  if (has_unique_serde_type(object)) {
    class(object) <- "ArrayList"
    return(writeObject(object, con, writeType))
  }
  if (writeType) {
    writeType(object, con)
  }
  writeObject(length(object), con, writeType = FALSE)
  for (elem in object) writeObject(elem, con, writeType = TRUE)
}
writeObject.ArrayList <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(array(), con)
  }

  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  writeType(if (length(object)) object[[1L]] else "", con)

  writeObject(length(object), con, writeType = FALSE)
  for (elem in object) writeObject(elem, con, writeType = FALSE)
}

writeObject.jobj <- function(object, con, writeType = TRUE) {
  if (!isValidJobj(object)) {
    stop("invalid jobj ", object$id)
  }
  if (writeType) {
    writeType(object, con)
  }
  writeObject(object$id, con, writeType = FALSE)
}
# Used to pass in hash maps required on Java side.
writeObject.environment <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  len <- length(object)
  writeObject(len, con, writeType = FALSE)
  if (len > 0L) {
    envObj <- ls(object)
    # vector of names for environment doesn't include the array marker,
    #   so manually write the character marker & then the names object itself
    writeType("", con)
    # force array-like writing (even for singleton object)
    writeObject(length(envObj), con, writeType = FALSE)
    for (nm in envObj) writeObject(nm, con, writeType = FALSE)
    # also force list writing (even for array-able env contents)
    vals <- mget(envObj, object)
    writeObject(length(vals), con, writeType = FALSE)
    for (val in vals) writeObject(val, con, writeType = TRUE)
  }
}

writeObject.Date <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  writeObject(as.character(object), con, writeType = FALSE)
}

# covers POSIXct and POSIXt
writeObject.POSIXt <- function(object, con, writeType = TRUE) {
  if (writeType) {
    writeType(object, con)
  }
  writeObject(as.double(object), con, writeType = FALSE)
}

writeRawSerialize <- function(batch, outputCon) {
  outputSer <- serialize(batch, ascii = FALSE, connection = NULL)
  writeObject(outputSer, outputCon, writeType = FALSE)
}

writeRowSerialize <- function(rows, outputCon) {
  invisible(lapply(rows, function(r) {
    bytes <- serializeRow(r)
    writeObject(bytes, outputCon, writeType = FALSE)
  }))
}

serializeRow <- function(row) {
  rawObj <- rawConnection(raw(0L), "wb")
  on.exit(close(rawObj))
  writeObject(as.list(row), rawObj, writeType = FALSE)
  rawConnectionValue(rawObj)
}


writeType <- function(object, con) UseMethod("writeType")
writeType.default <- function(object, con) {
  stop("Unsupported type for serialization", class(object))
}
writeType.NULL <- function(object, con) {
  writeBin(as.raw(0x6e), con)
}

# markers are written into con to signal incoming object
#   type according to the following mapping:
#        type marker  raw
#        Date      D 0x44
#       array      a 0x61
#     logical      b 0x62
#   character      c 0x63
#     numeric      d 0x64
# environment      e 0x65
#     integer      i 0x69
#        jobj      j 0x6a
#        list      l 0x6c
#        null      n 0x6e
#         raw      r 0x72
#      struct      s 0x73
#      POSIXt      t 0x74

# 'is.na' only handles atomic vectors, lists and pairlists;
#   all atomic classes except complex are handled; complex will error

# typically, non-scalar value written as array; array writes array marker,
#   then the component type, then the length, then the elements. the exception
#   is for writeObject.environment, which writes the names of the objects
#   without an array marker
atomic_write_type <- function(object, con, r) {
  if (length(object) > 1L) {
    writeBin(as.raw(0x61), con)
  } else if (is.na(object)) return(writeBin(as.raw(0x6e), con))
  writeBin(r, con)
}
writeType.integer <- function(object, con) {
  atomic_write_type(object, con, as.raw(0x69))
}
writeType.character <- function(object, con) {
  atomic_write_type(object, con, as.raw(0x63))
}
writeType.logical <- function(object, con) {
  atomic_write_type(object, con, as.raw(0x62))
}
writeType.numeric <- function(object, con) {
  atomic_write_type(object, con, as.raw(0x64))
}
writeType.raw <- function(object, con) {
  writeBin(as.raw(0x72), con)
}
# mostly we can rely on atomic_write_type for the array flag,
#   but e.g. for list(as.list(1:5)), we have a (serde) array
#   because all the elements are also (serde) arrays. Using
#   writeType(array(), con) for this case seems like a good
#   compromise to me
writeType.array <- function(object, con) {
  writeBin(as.raw(0x61), con)
}
writeType.list <- function(object, con) {
  if (has_unique_serde_type(object)) {
    writeType(array(), con)
  } else {
    writeBin(as.raw(0x6c), con)
  }
}
writeType.struct <- function(object, con) {
  writeBin(as.raw(0x73), con)
}
writeType.jobj <- function(object, con) {
  writeBin(as.raw(0x6a), con)
}
writeType.environment <- function(object, con) {
  writeBin(as.raw(0x65), con)
}
writeType.Date <- function(object, con) {
  writeBin(as.raw(0x44), con)
}
# covers POSIXct and POSIXt
writeType.POSIXt <- function(object, con) {
  writeBin(as.raw(0x74), con)
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(args, con) {
  for (arg in args) writeObject(arg, con)
}

writeSerializeInArrow <- function(conn, df) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    # There looks no way to send each batch in streaming format via socket
    # connection. See ARROW-4512.
    # So, it writes the whole Arrow streaming-formatted binary at once for now.
    writeObject(arrow::write_arrow(df, raw()), conn, writeType = FALSE)
  } else {
    stop("'arrow' package should be installed.")
  }
}

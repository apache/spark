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
# types.R. This file handles the data type mapping between Spark and R

# The primitive data types, where names(PRIMITIVE_TYPES) are Scala types whereas
# values are equivalent R types.
PRIMITIVE_TYPES <- c(
  "byte"="integer",
  "tinyint"="integer",
  "integer"="integer",
  "float"="numeric",
  "double"="numeric",
  "numeric"="numeric",
  "character"="character",
  "string"="character",
  "binary"=NA,
  "raw"=NA,
  "logical"="logical",
  "boolean"="logical",
  "decimal"="numeric",
  "timestamp"=NA,
  "date"=NA)

# The complex data types. These do not have any direct mapping to R's types.
COMPLEX_TYPES <- c("map"=NA, "array"=NA, "struct"=NA)

# The full list of data types.
DATA_TYPES <- c(PRIMITIVE_TYPES, COMPLEX_TYPES)

#' Returns the column types of a DataFrame.
#' 
#' @name coltypes
#' @title Get column types of a DataFrame
#' @param x (DataFrame)
#' @return value (character) A character vector with the column types of the given DataFrame
#' @rdname coltypes
setMethod("coltypes",
          signature(x = "DataFrame"),
          function(x) {
            # Get the data types of the DataFrame by invoking dtypes() function.
            # Some post-processing is needed.
            types <- as.character(t(as.data.frame(dtypes(x))[2, ]))

            # Map Spark data types into R's data types
            rTypes <- as.character(DATA_TYPES[types])

            # Find which types could not be mapped
            naIndices <- which(is.na(rTypes))

            # Assign the original scala data types to the unmatched ones
            rTypes[naIndices] <- types[naIndices]

            rTypes
          })

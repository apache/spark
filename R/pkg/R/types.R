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
# values are equivalent R types. This is stored in an environment to allow for
# more efficient look up (environments use hashmaps).
PRIMITIVE_TYPES <- as.environment(list(
  "tinyint" = "integer",
  "smallint" = "integer",
  "int" = "integer",
  "bigint" = "numeric",
  "float" = "numeric",
  "double" = "numeric",
  "decimal" = "numeric",
  "string" = "character",
  "binary" = "raw",
  "boolean" = "logical",
  "timestamp" = c("POSIXct", "POSIXt"),
  "date" = "Date",
  # following types are not SQL types returned by dtypes(). They are listed here for usage
  # by checkType() in schema.R.
  # TODO: refactor checkType() in schema.R.
  "byte" = "integer",
  "integer" = "integer"
  ))

# The complex data types. These do not have any direct mapping to R's types.
COMPLEX_TYPES <- list(
  "map" = NA,
  "array" = NA,
  "struct" = NA)

# The full list of data types.
DATA_TYPES <- as.environment(c(as.list(PRIMITIVE_TYPES), COMPLEX_TYPES))

SHORT_TYPES <- as.environment(list(
  "character" = "chr",
  "logical" = "logi",
  "POSIXct" = "POSIXct",
  "integer" = "int",
  "numeric" = "num",
  "raw" = "raw",
  "Date" = "Date",
  "map" = "map",
  "array" = "array",
  "struct" = "struct"
))

# An environment for mapping R to Scala, names are R types and values are Scala types.
rToSQLTypes <- as.environment(list(
  "integer" = "integer", # in R, integer is 32bit
  "numeric" = "double",  # in R, numeric == double which is 64bit
  "double" = "double",
  "character" = "string",
  "logical" = "boolean"))

# Helper function of coverting decimal type. When backend returns column type in the
# format of decimal(,) (e.g., decimal(10, 0)), this function coverts the column type
# as double type. This function converts backend returned types that are not the key
# of PRIMITIVE_TYPES, but should be treated as PRIMITIVE_TYPES.
# @param A type returned from the JVM backend.
# @return A type is the key of the PRIMITIVE_TYPES.
specialtypeshandle <- function(type) {
  returntype <- NULL
  m <- regexec("^decimal(.+)$", type)
  matchedStrings <- regmatches(type, m)
  if (length(matchedStrings[[1]]) >= 2) {
    returntype <- "double"
  }
  returntype
}

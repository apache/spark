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
  "byte"="integer",
  "tinyint"="integer",
  "smallint"="integer",
  "integer"="integer",
  "bigint"="numeric",
  "float"="numeric",
  "double"="numeric",
  "decimal"="numeric",
  "string"="character",
  "binary"="raw",
  "boolean"="logical",
  "timestamp"="POSIXct",
  "date"="Date"))

# The complex data types. These do not have any direct mapping to R's types.
COMPLEX_TYPES <- list(
  "map"=NA,
  "array"=NA,
  "struct"=NA)

# The full list of data types.
DATA_TYPES <- as.environment(c(as.list(PRIMITIVE_TYPES), COMPLEX_TYPES))

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

#' Avro processing functions for Column operations
#'
#' Avro processing functions defined for \code{Column}.
#'
#' @param x Column to compute on.
#' @param jsonFormatSchema character Avro schema in JSON string format
#' @param ... additional argument(s) passed as parser options.
#' @name column_avro_functions
#' @rdname column_avro_functions
#' @family avro functions
#' @note Avro is built-in but external data source module since Spark 2.4.
#'   Please deploy the application as per the deployment section of "Apache Avro Data Source Guide".
#' @examples
#' \dontrun{
#' df <- createDataFrame(iris)
#' schema <- paste(
#'   c(
#'     '{"type": "record", "namespace": "example.avro", "name": "Iris", "fields": [',
#'     '{"type": ["double", "null"], "name": "Sepal_Length"},',
#'     '{"type": ["double", "null"], "name": "Sepal_Width"},',
#'     '{"type": ["double", "null"], "name": "Petal_Length"},',
#'     '{"type": ["double", "null"], "name": "Petal_Width"},',
#'     '{"type": ["string", "null"], "name": "Species"}]}'
#'   ),
#'   collapse="\\n"
#' )
#'
#' df_serialized <- select(
#'   df,
#'   alias(to_avro(alias(struct(column("*")), "fields")), "payload")
#' )
#'
#' df_deserialized <- select(
#'   df_serialized,
#'   from_avro(df_serialized$payload, schema)
#' )
#'
#' head(df_deserialized)
#' }
NULL

#' @include generics.R column.R
NULL

#' @details
#' \code{from_avro} Converts a binary column of Avro format into its corresponding catalyst value.
#' The specified schema must match the read data, otherwise the behavior is undefined:
#' it may fail or return arbitrary result.
#' To deserialize the data with a compatible and evolved schema, the expected Avro schema can be
#' set via the option avroSchema.
#'
#' @rdname column_avro_functions
#' @aliases from_avro from_avro,Column-method
#' @note from_avro since 3.1.0
setMethod("from_avro",
          signature(x = "characterOrColumn"),
          function(x, jsonFormatSchema, ...) {
            x <- if (is.character(x)) {
              column(x)
            } else {
              x
            }

            options <- varargsToStrEnv(...)
            jc <- callJStatic(
              "org.apache.spark.sql.avro.functions", "from_avro",
              x@jc,
              jsonFormatSchema,
              options
            )
            column(jc)
          })

#' @details
#' \code{to_avro} Converts a column into binary of Avro format.
#'
#' @rdname column_avro_functions
#' @aliases to_avro to_avro,Column-method
#' @note to_avro since 3.1.0
setMethod("to_avro",
          signature(x = "characterOrColumn"),
          function(x, jsonFormatSchema = NULL) {
            x <- if (is.character(x)) {
              column(x)
            } else {
              x
            }

            jc <- if (is.null(jsonFormatSchema)) {
              callJStatic("org.apache.spark.sql.avro.functions", "to_avro", x@jc)
            } else {
              callJStatic(
                "org.apache.spark.sql.avro.functions",
                "to_avro",
                x@jc,
                jsonFormatSchema
              )
            }
            column(jc)
          })

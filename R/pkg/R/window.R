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

# window.R - Utility functions for defining window in DataFrames

#' window.partitionBy
#'
#' Creates a WindowSpec with the partitioning defined.
#'
#' @rdname window.partitionBy
#' @name window.partitionBy
#' @export
#' @examples
#' \dontrun{
#'   ws <- window.partitionBy("key1", "key2")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- window.partitionBy(df$key1, df$key2)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
setMethod("window.partitionBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "partitionBy",
                          col,
                          list(...)))
          })

#' @rdname window.partitionBy
#' @name window.partitionBy
#' @export
setMethod("window.partitionBy",
          signature(col = "Column"),
          function(col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "partitionBy",
                          jcols))
          })

#' window.orderBy
#'
#' Creates a WindowSpec with the ordering defined.
#'
#' @rdname window.orderBy
#' @name window.orderBy
#' @export
#' @examples
#' \dontrun{
#'   ws <- window.orderBy("key1", "key2")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- window.orderBy(df$key1, df$key2)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
setMethod("window.orderBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "orderBy",
                          col,
                          list(...)))
          })

#' @rdname window.orderBy
#' @name window.orderBy
#' @export
setMethod("window.orderBy",
          signature(col = "Column"),
          function(col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "orderBy",
                          jcols))
          })

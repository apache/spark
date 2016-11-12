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

#' windowPartitionBy
#'
#' Creates a WindowSpec with the partitioning defined.
#'
#' @param col A column name or Column by which rows are partitioned to
#'            windows.
#' @param ... Optional column names or Columns in addition to col, by
#'            which rows are partitioned to windows.
#'
#' @rdname windowPartitionBy
#' @name windowPartitionBy
#' @aliases windowPartitionBy,character-method
#' @export
#' @examples
#' \dontrun{
#'   ws <- orderBy(windowPartitionBy("key1", "key2"), "key3")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- orderBy(windowPartitionBy(df$key1, df$key2), df$key3)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
#' @note windowPartitionBy(character) since 2.0.0
setMethod("windowPartitionBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "partitionBy",
                          col,
                          list(...)))
          })

#' @rdname windowPartitionBy
#' @name windowPartitionBy
#' @aliases windowPartitionBy,Column-method
#' @export
#' @note windowPartitionBy(Column) since 2.0.0
setMethod("windowPartitionBy",
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

#' windowOrderBy
#'
#' Creates a WindowSpec with the ordering defined.
#'
#' @param col A column name or Column by which rows are ordered within
#'            windows.
#' @param ... Optional column names or Columns in addition to col, by
#'            which rows are ordered within windows.
#'
#' @rdname windowOrderBy
#' @name windowOrderBy
#' @aliases windowOrderBy,character-method
#' @export
#' @examples
#' \dontrun{
#'   ws <- windowOrderBy("key1", "key2")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- windowOrderBy(df$key1, df$key2)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
#' @note windowOrderBy(character) since 2.0.0
setMethod("windowOrderBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              callJStatic("org.apache.spark.sql.expressions.Window",
                          "orderBy",
                          col,
                          list(...)))
          })

#' @rdname windowOrderBy
#' @name windowOrderBy
#' @aliases windowOrderBy,Column-method
#' @export
#' @note windowOrderBy(Column) since 2.0.0
setMethod("windowOrderBy",
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

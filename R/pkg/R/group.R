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

# group.R - GroupedData class and methods implemented in S4 OO classes

#' @include generics.R jobj.R schema.R column.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a GroupedData
#' @description GroupedDatas can be created using groupBy() on a SparkDataFrame
#' @rdname GroupedData
#' @seealso groupBy
#'
#' @param sgd A Java object reference to the backing Scala GroupedData
#' @export
setClass("GroupedData",
         slots = list(sgd = "jobj"))

setMethod("initialize", "GroupedData", function(.Object, sgd) {
  .Object@sgd <- sgd
  .Object
})

#' @rdname GroupedData
groupedData <- function(sgd) {
  new("GroupedData", sgd)
}


#' @rdname show
setMethod("show", "GroupedData",
          function(object) {
            cat("GroupedData\n")
          })

#' Count
#'
#' Count the number of rows for each group.
#' The resulting SparkDataFrame will also contain the grouping columns.
#'
#' @param x a GroupedData
#' @return a SparkDataFrame
#' @rdname agg
#' @export
#' @examples
#' \dontrun{
#'   count(groupBy(df, "name"))
#' }
setMethod("count",
          signature(x = "GroupedData"),
          function(x) {
            dataFrame(callJMethod(x@sgd, "count"))
          })

#' summarize
#'
#' Aggregates on the entire SparkDataFrame without groups.
#' The resulting SparkDataFrame will also contain the grouping columns.
#'
#' df2 <- agg(df, <column> = <aggFunction>)
#' df2 <- agg(df, newColName = aggFunction(column))
#'
#' @param x a GroupedData
#' @return a SparkDataFrame
#' @rdname summarize
#' @name agg
#' @family agg_funcs
#' @examples
#' \dontrun{
#'  df2 <- agg(df, age = "sum")  # new column name will be created as 'SUM(age#0)'
#'  df3 <- agg(df, ageSum = sum(df$age)) # Creates a new column named ageSum
#'  df4 <- summarize(df, ageSum = max(df$age))
#' }
setMethod("agg",
          signature(x = "GroupedData"),
          function(x, ...) {
            cols <- list(...)
            stopifnot(length(cols) > 0)
            if (is.character(cols[[1]])) {
              cols <- varargsToEnv(...)
              sdf <- callJMethod(x@sgd, "agg", cols)
            } else if (class(cols[[1]]) == "Column") {
              ns <- names(cols)
              if (!is.null(ns)) {
                for (n in ns) {
                  if (n != "") {
                    cols[[n]] <- alias(cols[[n]], n)
                  }
                }
              }
              jcols <- lapply(cols, function(c) { c@jc })
              sdf <- callJMethod(x@sgd, "agg", jcols[[1]], jcols[-1])
            } else {
              stop("agg can only support Column or character")
            }
            dataFrame(sdf)
          })

#' @rdname summarize
#' @name summarize
setMethod("summarize",
          signature(x = "GroupedData"),
          function(x, ...) {
            agg(x, ...)
          })

# Aggregate Functions by name
methods <- c("avg", "max", "mean", "min", "sum")

# These are not exposed on GroupedData: "kurtosis", "skewness", "stddev", "stddev_samp", "stddev_pop",
# "variance", "var_samp", "var_pop"

createMethod <- function(name) {
  setMethod(name,
            signature(x = "GroupedData"),
            function(x, ...) {
              sdf <- callJMethod(x@sgd, name, list(...))
              dataFrame(sdf)
            })
}

createMethods <- function() {
  for (name in methods) {
    createMethod(name)
  }
}

createMethods()

#' gapply
#'
#' Applies a R function to each group in the input GroupedData
#'
#' @param x a GroupedData
#' @param func A function to be applied to each group partition specified by GroupedData.
#'             The function `func` takes as argument a key - grouping columns and
#'             a data frame - a local R data.frame.
#'             The output of `func` is a local R data.frame.
#' @param schema The schema of the resulting SparkDataFrame after the function is applied.
#'               The schema must match to output of `func`. It has to be defined for each
#'               output column with preferred output column name and corresponding data type.
#' @return a SparkDataFrame
#' @rdname gapply
#' @name gapply
#' @examples
#' \dontrun{
#' Computes the arithmetic mean of the second column by grouping
#' on the first and third columns. Output the grouping values and the average.
#'
#' df <- createDataFrame (
#' list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
#'   c("a", "b", "c", "d"))
#'
#' Here our output contains three columns, the key which is a combination of two
#' columns with data types integer and string and the mean which is a double.
#' schema <-  structType(structField("a", "integer"), structField("c", "string"),
#'   structField("avg", "double"))
#' df1 <- gapply(
#'   df,
#'   list("a", "c"),
#'   function(key, x) {
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#'   },
#' schema)
#' collect(df1)
#'
#' Result
#' ------
#' a c avg
#' 3 3 3.0
#' 1 1 1.5
#' }
setMethod("gapply",
          signature(x = "GroupedData"),
          function(x, func, schema) {
            try(if (is.null(schema)) stop("schema cannot be NULL"))
            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                 connection = NULL)
            broadcastArr <- lapply(ls(.broadcastNames),
                              function(name) { get(name, .broadcastNames) })
            sdf <- callJStatic(
                     "org.apache.spark.sql.api.r.SQLUtils",
                     "gapply",
                     x@sgd,
                     serialize(cleanClosure(func), connection = NULL),
                     packageNamesArr,
                     broadcastArr,
                     schema$jobj)
            dataFrame(sdf)
          })

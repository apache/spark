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
#' @description GroupedDatas can be created using groupBy() on a DataFrame
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

#' @rdname DataFrame
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
#' The resulting DataFrame will also contain the grouping columns.
#'
#' @param x a GroupedData
#' @return a DataFrame
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
#' Aggregates on the entire DataFrame without groups.
#' The resulting DataFrame will also contain the grouping columns.
#'
#' df2 <- agg(df, <column> = <aggFunction>)
#' df2 <- agg(df, newColName = aggFunction(column))
#'
#' @param x a GroupedData
#' @return a DataFrame
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

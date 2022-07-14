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

#' S4 class that represents a GroupedData
#'
#' GroupedDatas can be created using groupBy() on a SparkDataFrame
#'
#' @rdname GroupedData
#' @seealso groupBy
#'
#' @param sgd A Java object reference to the backing Scala GroupedData
#' @note GroupedData since 1.4.0
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
#' @aliases show,GroupedData-method
#' @note show(GroupedData) since 1.4.0
setMethod("show", "GroupedData",
          function(object) {
            cat("GroupedData\n")
          })

#' Count
#'
#' Count the number of rows for each group when we have \code{GroupedData} input.
#' The resulting SparkDataFrame will also contain the grouping columns.
#'
#' @return A SparkDataFrame.
#' @rdname count
#' @aliases count,GroupedData-method
#' @examples
#' \dontrun{
#'   count(groupBy(df, "name"))
#' }
#' @note count since 1.4.0
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
#' @rdname summarize
#' @aliases agg,GroupedData-method
#' @name agg
#' @family agg_funcs
#' @examples
#' \dontrun{
#'  df2 <- agg(df, age = "sum")  # new column name will be created as 'SUM(age#0)'
#'  df3 <- agg(df, ageSum = sum(df$age)) # Creates a new column named ageSum
#'  df4 <- summarize(df, ageSum = max(df$age))
#' }
#' @note agg since 1.4.0
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
#' @aliases summarize,GroupedData-method
#' @note summarize since 1.4.0
setMethod("summarize",
          signature(x = "GroupedData"),
          function(x, ...) {
            agg(x, ...)
          })

# Aggregate Functions by name
methods <- c("avg", "max", "mean", "min", "sum")

# These are not exposed on GroupedData: "kurtosis", "skewness", "stddev", "stddev_samp",
# "stddev_pop", "variance", "var_samp", "var_pop"

#' Pivot a column of the GroupedData and perform the specified aggregation.
#'
#' Pivot a column of the GroupedData and perform the specified aggregation.
#' There are two versions of pivot function: one that requires the caller to specify the list
#' of distinct values to pivot on, and one that does not. The latter is more concise but less
#' efficient, because Spark needs to first compute the list of distinct values internally.
#'
#' @param x a GroupedData object
#' @param colname A column name
#' @param values A value or a list/vector of distinct values for the output columns.
#' @return GroupedData object
#' @rdname pivot
#' @aliases pivot,GroupedData,character-method
#' @name pivot
#' @examples
#' \dontrun{
#' df <- createDataFrame(data.frame(
#'     earnings = c(10000, 10000, 11000, 15000, 12000, 20000, 21000, 22000),
#'     course = c("R", "Python", "R", "Python", "R", "Python", "R", "Python"),
#'     period = c("1H", "1H", "2H", "2H", "1H", "1H", "2H", "2H"),
#'     year = c(2015, 2015, 2015, 2015, 2016, 2016, 2016, 2016)
#' ))
#' group_sum <- sum(pivot(groupBy(df, "year"), "course"), "earnings")
#' group_min <- min(pivot(groupBy(df, "year"), "course", "R"), "earnings")
#' group_max <- max(pivot(groupBy(df, "year"), "course", c("Python", "R")), "earnings")
#' group_mean <- mean(pivot(groupBy(df, "year"), "course", list("Python", "R")), "earnings")
#' }
#' @note pivot since 2.0.0
setMethod("pivot",
          signature(x = "GroupedData", colname = "character"),
          function(x, colname, values = list()) {
            stopifnot(length(colname) == 1)
            if (length(values) == 0) {
              result <- callJMethod(x@sgd, "pivot", colname)
            } else {
              if (length(values) > length(unique(values))) {
                stop("Values are not unique")
              }
              result <- callJMethod(x@sgd, "pivot", colname, as.list(values))
            }
            groupedData(result)
          })

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
#' @rdname gapply
#' @aliases gapply,GroupedData-method
#' @name gapply
#' @note gapply(GroupedData) since 2.0.0
setMethod("gapply",
          signature(x = "GroupedData"),
          function(x, func, schema) {
            if (is.null(schema)) stop("schema cannot be NULL")
            gapplyInternal(x, func, schema)
          })

#' gapplyCollect
#'
#' @rdname gapplyCollect
#' @aliases gapplyCollect,GroupedData-method
#' @name gapplyCollect
#' @note gapplyCollect(GroupedData) since 2.0.0
setMethod("gapplyCollect",
          signature(x = "GroupedData"),
          function(x, func) {
            gdf <- gapplyInternal(x, func, NULL)
            content <- callJMethod(gdf@sdf, "collect")
            # content is a list of items of struct type. Each item has a single field
            # which is a serialized data.frame corresponds to one group of the
            # SparkDataFrame.
            ldfs <- lapply(content, function(x) { unserialize(x[[1]]) })
            ldf <- do.call(rbind, ldfs)
            row.names(ldf) <- NULL
            ldf
          })

gapplyInternal <- function(x, func, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]] == "true"
  if (arrowEnabled) {
    if (inherits(schema, "structType")) {
      checkSchemaInArrow(schema)
    } else if (is.null(schema)) {
      stop("Arrow optimization does not support 'gapplyCollect' yet. Please disable ",
           "Arrow optimization or use 'collect' and 'gapply' APIs instead.")
    } else {
      stop("'schema' should be DDL-formatted string or structType.")
    }
  }

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
           if (class(schema) == "structType") { schema$jobj } else { NULL })
  dataFrame(sdf)
}

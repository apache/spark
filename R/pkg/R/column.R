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
setOldClass("SparkDataFrame")
setClassUnion("SparkDataFrameOrNull", c("SparkDataFrame", "NULL"))

# Column Class

#' @include generics.R jobj.R schema.R
NULL

setOldClass("jobj")

#' S4 class that represents a SparkDataFrame column
#'
#' The column class supports unary, binary operations on SparkDataFrame columns
#'
#' @rdname column
#'
#' @slot jc reference to JVM SparkDataFrame column
#' @slot df the parent SparkDataFrame of the Column object
#' @export
#' @note Column since 1.4.0
setClass("Column",
         slots = list(jc = "jobj", df = "SparkDataFrameOrNull"))

#' A set of operations working with SparkDataFrame columns
#' @rdname columnfunctions
#' @name columnfunctions
NULL
setMethod("initialize", "Column", function(.Object, jc, df) {
  .Object@jc <- jc

  # Some Column objects don't have any referencing DataFrame. In such case, df will be NULL.
  if (missing(df)) {
    df <- NULL
  }
  .Object@df <- df
  .Object
})

#' @rdname show
setMethod("show", signature = "Column", function(object) {
  MAX_ELEMENTS <- 20
  head.df <- head(object, MAX_ELEMENTS)

  if (length(head.df) == 0) {
    colname <- callJMethod(object@jc, "toString")
    cat(paste0(colname, "\n"))
    cat(paste0("<Empty column>\n"))
  } else {
    show(head.df)
  }
  if (length(head.df) == MAX_ELEMENTS)  {
    cat(paste0("\b...\nDisplaying up to ", as.character(MAX_ELEMENTS), " elements only."))
  }
})

#' @rdname head
setMethod("head",
          signature(x = "Column"),
          function(x, num = 6L) {
            if (is.null(x@df)) {
              character(0)
            } else {
              head(select(x@df, x), num)[, 1]
            }
          })

#' @rdname column
#' @name column
#' @param df the parent SparkDataFrame. This is used to retrieve the contents of the column through method head.
#' @aliases column,jobj-method
setMethod("column",
          signature(x = "jobj"),
          function(x, df) {
            if (missing(df)) {
              df <- NULL
            }
            new("Column", jc = x, df = df)
          })

operators <- list(
  "+" = "plus", "-" = "minus", "*" = "multiply", "/" = "divide", "%%" = "mod",
  "==" = "equalTo", ">" = "gt", "<" = "lt", "!=" = "notEqual", "<=" = "leq", ">=" = "geq",
  # we can not override `&&` and `||`, so use `&` and `|` instead
  "&" = "and", "|" = "or", #, "!" = "unary_$bang"
  "^" = "pow"
)
column_functions1 <- c("asc", "desc", "isNaN", "isNull", "isNotNull")
column_functions2 <- c("like", "rlike", "getField", "getItem", "contains")

createOperator <- function(op) {
  setMethod(op,
            signature(e1 = "Column"),
            function(e1, e2) {
              jc <- if (missing(e2)) {
                if (op == "-") {
                  callJMethod(e1@jc, "unary_$minus")
                } else {
                  callJMethod(e1@jc, operators[[op]])
                }
              } else {
                if (class(e2) == "Column") {
                  e2 <- e2@jc
                }
                if (op == "^") {
                  jc <- callJStatic("org.apache.spark.sql.functions", operators[[op]], e1@jc, e2)
                } else {
                  callJMethod(e1@jc, operators[[op]], e2)
                }
              }
              column(jc, e1@df)
            })
}

createColumnFunction1 <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              column(callJMethod(x@jc, name), x@df)
            })
}

createColumnFunction2 <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x, data) {
              if (class(data) == "Column") {
                data <- data@jc
              }
              jc <- callJMethod(x@jc, name, data)
              column(jc, x@df)
            })
}

createMethods <- function() {
  for (op in names(operators)) {
    createOperator(op)
  }
  for (name in column_functions1) {
    createColumnFunction1(name)
  }
  for (name in column_functions2) {
    createColumnFunction2(name)
  }
}

createMethods()

#' alias
#'
#' Set a new name for a column
#'
#' @param object Column to rename
#' @param data new name to use
#'
#' @rdname alias
#' @name alias
#' @aliases alias,Column-method
#' @family colum_func
#' @export
#' @note alias since 1.4.0
setMethod("alias",
          signature(object = "Column"),
          function(object, data) {
            if (is.character(data)) {
              column(callJMethod(object@jc, "as", data), object@df)
            } else {
              stop("data should be character")
            }
          })

#' substr
#'
#' An expression that returns a substring.
#'
#' @rdname substr
#' @name substr
#' @family colum_func
#' @aliases substr,Column-method
#'
#' @param x a Column.
#' @param start starting position.
#' @param stop ending position.
#' @note substr since 1.4.0
setMethod("substr", signature(x = "Column"),
          function(x, start, stop) {
            jc <- callJMethod(x@jc, "substr", as.integer(start - 1), as.integer(stop - start + 1))
            column(jc, x@df)
          })

#' startsWith
#'
#' Determines if entries of x start with string (entries of) prefix respectively,
#' where strings are recycled to common lengths.
#'
#' @rdname startsWith
#' @name startsWith
#' @family colum_func
#' @aliases startsWith,Column-method
#'
#' @param x vector of character string whose "starts" are considered
#' @param prefix character vector (often of length one)
#' @note startsWith since 1.4.0
setMethod("startsWith", signature(x = "Column"),
          function(x, prefix) {
            jc <- callJMethod(x@jc, "startsWith", as.vector(prefix))
            column(jc)
          })

#' endsWith
#'
#' Determines if entries of x end with string (entries of) suffix respectively,
#' where strings are recycled to common lengths.
#'
#' @rdname endsWith
#' @name endsWith
#' @family colum_func
#' @aliases endsWith,Column-method
#'
#' @param x vector of character string whose "ends" are considered
#' @param suffix character vector (often of length one)
#' @note endsWith since 1.4.0
setMethod("endsWith", signature(x = "Column"),
          function(x, suffix) {
            jc <- callJMethod(x@jc, "endsWith", as.vector(suffix))
            column(jc)
          })

#' between
#'
#' Test if the column is between the lower bound and upper bound, inclusive.
#'
#' @rdname between
#' @name between
#' @family colum_func
#' @aliases between,Column-method
#'
#' @param x a Column
#' @param bounds lower and upper bounds
#' @note between since 1.5.0
setMethod("between", signature(x = "Column"),
          function(x, bounds) {
            if (is.vector(bounds) && length(bounds) == 2) {
              jc <- callJMethod(x@jc, "between", bounds[1], bounds[2])
              column(jc, x@df)
            } else {
              stop("bounds should be a vector of lower and upper bounds")
            }
          })

#' Casts the column to a different data type.
#'
#' @param x a Column.
#' @param dataType a character object describing the target data type.
#'        See
#'        \href{https://spark.apache.org/docs/latest/sparkr.html#data-type-mapping-between-r-and-spark}{
#'        Spark Data Types} for available data types.
#' @rdname cast
#' @name cast
#' @family colum_func
#' @aliases cast,Column-method
#'
#' @examples \dontrun{
#'   cast(df$age, "string")
#' }
#' @note cast since 1.4.0
setMethod("cast",
          signature(x = "Column"),
          function(x, dataType) {
            if (is.character(dataType)) {
              column(callJMethod(x@jc, "cast", dataType), x@df)
            } else {
              stop("dataType should be character")
            }
          })

#' Match a column with given values.
#'
#' @param x a Column.
#' @param table a collection of values (coercible to list) to compare with.
#' @rdname match
#' @name %in%
#' @aliases %in%,Column-method
#' @return A matched values as a result of comparing with given values.
#' @export
#' @examples
#' \dontrun{
#' filter(df, "age in (10, 30)")
#' where(df, df$age %in% c(10, 30))
#' }
#' @note \%in\% since 1.5.0
setMethod("%in%",
          signature(x = "Column"),
          function(x, table) {
            jc <- callJMethod(x@jc, "isin", as.list(table))
            column(jc, x@df)
          })

#' otherwise
#'
#' If values in the specified column are null, returns the value.
#' Can be used in conjunction with \code{when} to specify a default value for expressions.
#'
#' @param x a Column.
#' @param value value to replace when the corresponding entry in \code{x} is NA.
#'              Can be a single value or a Column.
#' @rdname otherwise
#' @name otherwise
#' @family colum_func
#' @aliases otherwise,Column-method
#' @export
#' @note otherwise since 1.5.0
setMethod("otherwise",
          signature(x = "Column", value = "ANY"),
          function(x, value) {
            value <- if (class(value) == "Column") { value@jc } else { value }
            jc <- callJMethod(x@jc, "otherwise", value)
            column(jc, x@df)
          })

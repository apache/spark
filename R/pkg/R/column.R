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

# Column Class

#' @include generics.R jobj.R schema.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a DataFrame column
#' @description The column class supports unary, binary operations on DataFrame columns

#' @rdname column
#'
#' @param jc reference to JVM DataFrame column
#' @export
setClass("Column",
         slots = list(jc = "jobj"))

setMethod("initialize", "Column", function(.Object, jc) {
  .Object@jc <- jc
  .Object
})

column <- function(jc) {
  new("Column", jc)
}

col <- function(x) {
  column(callJStatic("org.apache.spark.sql.functions", "col", x))
}

#' @rdname show
setMethod("show", "Column",
          function(object) {
            cat("Column", callJMethod(object@jc, "toString"), "\n")
          })

operators <- list(
  "+" = "plus", "-" = "minus", "*" = "multiply", "/" = "divide", "%%" = "mod",
  "==" = "equalTo", ">" = "gt", "<" = "lt", "!=" = "notEqual", "<=" = "leq", ">=" = "geq",
  # we can not override `&&` and `||`, so use `&` and `|` instead
  "&" = "and", "|" = "or" #, "!" = "unary_$bang"
)
column_functions1 <- c("asc", "desc", "isNull", "isNotNull")
column_functions2 <- c("like", "rlike", "startsWith", "endsWith", "getField", "getItem", "contains")
functions <- c("min", "max", "sum", "avg", "mean", "count", "abs", "sqrt",
               "first", "last", "lower", "upper", "sumDistinct")

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
                callJMethod(e1@jc, operators[[op]], e2)
              }
              column(jc)
            })
}

createColumnFunction1 <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              column(callJMethod(x@jc, name))
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
              column(jc)
            })
}

createStaticFunction <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              jc <- callJStatic("org.apache.spark.sql.functions", name, x@jc)
              column(jc)
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
  for (x in functions) {
    createStaticFunction(x)
  }
}

createMethods()

#' alias
#'
#' Set a new name for a column
setMethod("alias",
          signature(object = "Column"),
          function(object, data) {
            if (is.character(data)) {
              column(callJMethod(object@jc, "as", data))
            } else {
              stop("data should be character")
            }
          })

#' An expression that returns a substring.
#'
#' @param start starting position
#' @param stop ending position
setMethod("substr", signature(x = "Column"),
          function(x, start, stop) {
            jc <- callJMethod(x@jc, "substr", as.integer(start - 1), as.integer(stop - start + 1))
            column(jc)
          })

#' Casts the column to a different data type.
#' @examples
#' \dontrun{
#'   cast(df$age, "string")
#'   cast(df$name, list(type="array", elementType="byte", containsNull = TRUE))
#' }
setMethod("cast",
          signature(x = "Column"),
          function(x, dataType) {
            if (is.character(dataType)) {
              column(callJMethod(x@jc, "cast", dataType))
            } else if (is.list(dataType)) {
              json <- tojson(dataType)
              jdataType <- callJStatic("org.apache.spark.sql.types.DataType", "fromJson", json)
              column(callJMethod(x@jc, "cast", jdataType))
            } else {
              stop("dataType should be character or list")
            }
          })

#' Approx Count Distinct
#'
#' Returns the approximate number of distinct items in a group.
#'
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x, rsd = 0.95) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approxCountDistinct", x@jc, rsd)
            column(jc)
          })

#' Count Distinct
#'
#' returns the number of distinct items in a group.
#'
setMethod("countDistinct",
          signature(x = "Column"),
          function(x, ...) {
            jcol <- lapply(list(...), function (x) {
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "countDistinct", x@jc,
                              listToSeq(jcol))
            column(jc)
          })


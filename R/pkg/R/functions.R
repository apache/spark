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

#' @include generics.R column.R
NULL

#' @title S4 expression functions for DataFrame column(s)
#' @description These are expression functions on DataFrame columns

functions1 <- c(
  "abs", "acos", "approxCountDistinct", "ascii", "asin", "atan",
  "avg", "base64", "bin", "bitwiseNOT", "cbrt", "ceil", "cos", "cosh", "count",
  "crc32", "dayofmonth", "dayofyear", "exp", "explode", "expm1", "factorial",
  "first", "floor", "hex", "hour", "initcap", "isNaN", "last", "last_day",
  "length", "log", "log10", "log1p", "log2", "lower", "ltrim", "max", "md5",
  "mean", "min", "minute", "month", "negate", "quarter", "reverse",
  "rint", "round", "rtrim", "second", "sha1", "signum", "sin", "sinh", "size",
  "soundex", "sqrt", "sum", "sumDistinct", "tan", "tanh", "toDegrees",
  "toRadians", "to_date", "trim", "unbase64", "unhex", "upper", "weekofyear",
  "year")
functions2 <- c(
  "atan2", "datediff", "hypot", "levenshtein", "months_between", "nanvl", "pmod")

createFunction1 <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              jc <- callJStatic("org.apache.spark.sql.functions", name, x@jc)
              column(jc)
            })
}

createFunction2 <- function(name) {
  setMethod(name,
            signature(y = "Column"),
            function(y, x) {
              if (class(x) == "Column") {
                x <- x@jc
              }
              jc <- callJStatic("org.apache.spark.sql.functions", name, y@jc, x)
              column(jc)
            })
}

createFunctions <- function() {
  for (name in functions1) {
    createFunction1(name)
  }
  for (name in functions2) {
    createFunction2(name)
  }
}

createFunctions()

#' Approx Count Distinct
#'
#' @rdname functions
#' @return the approximate number of distinct items in a group.
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x, rsd = 0.95) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approxCountDistinct", x@jc, rsd)
            column(jc)
          })

#' Count Distinct
#'
#' @rdname functions
#' @return the number of distinct items in a group.
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

#' @rdname functions
#' @aliases ceil
setMethod("ceiling",
          signature(x = "Column"),
          function(x) {
            ceil(x)
          })

#' @rdname functions
#' @aliases signum
setMethod("sign", signature(x = "Column"),
          function(x) {
            signum(x)
          })

#' @rdname functions
#' @aliases countDistinct
setMethod("n_distinct", signature(x = "Column"),
          function(x, ...) {
            countDistinct(x, ...)
          })

#' @rdname functions
#' @aliases count
setMethod("n", signature(x = "Column"),
          function(x) {
            count(x)
          })

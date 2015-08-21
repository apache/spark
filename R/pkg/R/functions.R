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

#' @rdname functions
#' @return Creates a Column class of literal value.
setMethod("lit", signature("ANY"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lit",
                              ifelse(class(x) == "Column", x@jc, x))
            column(jc)
          })

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
#' @return Concatenates multiple input string columns together into a single string column.
setMethod("concat",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) { x@jc })
            jc <- callJStatic("org.apache.spark.sql.functions", "concat", listToSeq(jcols))
            column(jc)
          })

#' @rdname functions
#' @return Returns the greatest value of the list of column names, skipping null values.
#'         This function takes at least 2 parameters. It will return null if all parameters are null.
setMethod("greatest",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function(x) { x@jc })
            jc <- callJStatic("org.apache.spark.sql.functions", "greatest", listToSeq(jcols))
            column(jc)
          })

#' @rdname functions
#' @return Returns the least value of the list of column names, skipping null values.
#'         This function takes at least 2 parameters. It will return null iff all parameters are null.
setMethod("least",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function(x) { x@jc })
            jc <- callJStatic("org.apache.spark.sql.functions", "least", listToSeq(jcols))
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

#' date_format
#'
#' Converts a date/timestamp/string to a value of string in the format specified by the date
#' format given by the second argument.
#'
#' A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
#' pattern letters of `java.text.SimpleDateFormat` can be used.
#'
#' NOTE: Use when ever possible specialized functions like `year`. These benefit from a
#' specialized implementation.
#'
#' @rdname functions
setMethod("date_format", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_format", y@jc, x)
            column(jc)
          })

#' from_utc_timestamp
#'
#' Assumes given timestamp is UTC and converts to given timezone.
#'
#' @rdname functions
setMethod("from_utc_timestamp", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "from_utc_timestamp", y@jc, x)
            column(jc)
          })

#' instr
#'
#' Locate the position of the first occurrence of substr column in the given string.
#' Returns null if either of the arguments are null.
#'
#' NOTE: The position is not zero based, but 1 based index, returns 0 if substr
#' could not be found in str.
#'
#' @rdname functions
setMethod("instr", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "instr", y@jc, x)
            column(jc)
          })

#' next_day
#'
#' Given a date column, returns the first date which is later than the value of the date column
#' that is on the specified day of the week.
#'
#' For example, `next <- day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
#' Sunday after 2015-07-27.
#'
#' Day of the week parameter is case insensitive, and accepts:
#' "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
#'
#' @rdname functions
setMethod("next_day", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "next_day", y@jc, x)
            column(jc)
          })

#' to_utc_timestamp
#'
#' Assumes given timestamp is in given timezone and converts to UTC.
#'
#' @rdname functions
setMethod("to_utc_timestamp", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_utc_timestamp", y@jc, x)
            column(jc)
          })

#' add_months
#'
#' Returns the date that is numMonths after startDate.
#'
#' @rdname functions
setMethod("add_months", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "add_months", y@jc, as.integer(x))
            column(jc)
          })

#' date_add
#'
#' Returns the date that is `days` days after `start`
#'
#' @rdname functions
setMethod("date_add", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_add", y@jc, as.integer(x))
            column(jc)
          })

#' date_sub
#'
#' Returns the date that is `days` days before `start`
#'
#' @rdname functions
setMethod("date_sub", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_sub", y@jc, as.integer(x))
            column(jc)
          })

#' format_number
#'
#' Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places,
#' and returns the result as a string column.
#'
#' If d is 0, the result has no decimal point or fractional part.
#' If d < 0, the result will be null.'
#'
#' @rdname functions
setMethod("format_number", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "format_number",
                              y@jc, as.integer(x))
            column(jc)
          })

#' sha2
#'
#' Calculates the SHA-2 family of hash functions of a binary column and
#' returns the value as a hex string.
#'
#' @rdname functions
#' @param y column to compute SHA-2 on.
#' @param x one of 224, 256, 384, or 512.
setMethod("sha2", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sha2", y@jc, as.integer(x))
            column(jc)
          })

#' shiftLeft
#'
#' Shift the the given value numBits left. If the given value is a long value, this function
#' will return a long value else it will return an integer value.
#'
#' @rdname functions
setMethod("shiftLeft", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftLeft",
                              y@jc, as.integer(x))
            column(jc)
          })

#' shiftRight
#'
#' Shift the the given value numBits right. If the given value is a long value, it will return
#' a long value else it will return an integer value.
#'
#' @rdname functions
setMethod("shiftRight", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftRight",
                              y@jc, as.integer(x))
            column(jc)
          })

#' shiftRightUnsigned
#'
#' Unsigned shift the the given value numBits right. If the given value is a long value,
#' it will return a long value else it will return an integer value.
#'
#' @rdname functions
setMethod("shiftRightUnsigned", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftRightUnsigned",
                              y@jc, as.integer(x))
            column(jc)
          })

#' concat_ws
#'
#' Concatenates multiple input string columns together into a single string column,
#' using the given separator.
#'
#' @rdname functions
setMethod("concat_ws", signature(sep = "character", x = "Column"),
          function(sep, x, ...) {
            jcols <- listToSeq(lapply(list(x, ...), function(x) { x@jc }))
            jc <- callJStatic("org.apache.spark.sql.functions", "concat_ws", sep, jcols)
            column(jc)
          })

#' conv
#'
#' Convert a number in a string column from one base to another.
#'
#' @rdname functions
setMethod("conv", signature(x = "Column", fromBase = "numeric", toBase = "numeric"),
          function(x, fromBase, toBase) {
            fromBase <- as.integer(fromBase)
            toBase <- as.integer(toBase)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "conv",
                              x@jc, fromBase, toBase)
            column(jc)
          })

#' expr
#'
#' Parses the expression string into the column that it represents, similar to
#' DataFrame.selectExpr
#'
#' @rdname functions
setMethod("expr", signature(x = "character"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "expr", x)
            column(jc)
          })

#' format_string
#'
#' Formats the arguments in printf-style and returns the result as a string column.
#'
#' @rdname functions
setMethod("format_string", signature(format = "character", x = "Column"),
          function(format, x, ...) {
            jcols <- listToSeq(lapply(list(x, ...), function(arg) { arg@jc }))
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "format_string",
                              format, jcols)
            column(jc)
          })

#' from_unixtime
#'
#' Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
#' representing the timestamp of that moment in the current system time zone in the given
#' format.
#'
#' @rdname functions
setMethod("from_unixtime", signature(x = "Column"),
          function(x, format = "yyyy-MM-dd HH:mm:ss") {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_unixtime",
                              x@jc, format)
            column(jc)
          })

#' locate
#'
#' Locate the position of the first occurrence of substr.
#' NOTE: The position is not zero based, but 1 based index, returns 0 if substr
#' could not be found in str.
#'
#' @rdname functions
setMethod("locate", signature(substr = "character", str = "Column"),
          function(substr, str, pos = 0) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "locate",
                              substr, str@jc, as.integer(pos))
            column(jc)
          })

#' lpad
#'
#' Left-pad the string column with
#'
#' @rdname functions
setMethod("lpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' rand
#'
#' Generate a random column with i.i.d. samples from U[0.0, 1.0].
#'
#' @rdname functions
setMethod("rand", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand")
            column(jc)
          })
setMethod("rand", signature(seed = "numeric"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand", as.integer(seed))
            column(jc)
          })

#' randn
#'
#' Generate a column with i.i.d. samples from the standard normal distribution.
#'
#' @rdname functions
setMethod("randn", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "randn")
            column(jc)
          })
setMethod("randn", signature(seed = "numeric"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "randn", as.integer(seed))
            column(jc)
          })

#' regexp_extract
#'
#' Extract a specific(idx) group identified by a java regex, from the specified string column.
#'
#' @rdname functions
setMethod("regexp_extract",
          signature(x = "Column", pattern = "character", idx = "numeric"),
          function(x, pattern, idx) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "regexp_extract",
                              x@jc, pattern, as.integer(idx))
            column(jc)
          })

#' regexp_replace
#'
#' Replace all substrings of the specified string value that match regexp with rep.
#'
#' @rdname functions
setMethod("regexp_replace",
          signature(x = "Column", pattern = "character", replacement = "character"),
          function(x, pattern, replacement) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "regexp_replace",
                              x@jc, pattern, replacement)
            column(jc)
          })

#' rpad
#'
#' Right-padded with pad to a length of len.
#'
#' @rdname functions
setMethod("rpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "rpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' substring_index
#'
#' Returns the substring from string str before count occurrences of the delimiter delim.
#' If count is positive, everything the left of the final delimiter (counting from left) is
#' returned. If count is negative, every to the right of the final delimiter (counting from the
#' right) is returned. substring <- index performs a case-sensitive match when searching for delim.
#'
#' @rdname functions
setMethod("substring_index",
          signature(x = "Column", delim = "character", count = "numeric"),
          function(x, delim, count) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "substring_index",
                              x@jc, delim, as.integer(count))
            column(jc)
          })

#' translate
#'
#' Translate any character in the src by a character in replaceString.
#' The characters in replaceString is corresponding to the characters in matchingString.
#' The translate will happen when any character in the string matching with the character
#' in the matchingString.
#'
#' @rdname functions
setMethod("translate",
          signature(x = "Column", matchingString = "character", replaceString = "character"),
          function(x, matchingString, replaceString) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "translate", x@jc, matchingString, replaceString)
            column(jc)
          })

#' unix_timestamp
#'
#' Gets current Unix timestamp in seconds.
#'
#' @rdname functions
setMethod("unix_timestamp", signature(x = "missing", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp")
            column(jc)
          })
#' unix_timestamp
#'
#' Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
#' using the default timezone and the default locale, return null if fail.
#'
#' @rdname functions
setMethod("unix_timestamp", signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp", x@jc)
            column(jc)
          })
#' unix_timestamp
#'
#' Convert time string with given pattern
#' (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
#' to Unix time stamp (in seconds), return null if fail.
#'
#' @rdname functions
setMethod("unix_timestamp", signature(x = "Column", format = "character"),
          function(x, format = "yyyy-MM-dd HH:mm:ss") {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp", x@jc, format)
            column(jc)
          })
#' when
#'
#' Evaluates a list of conditions and returns one of multiple possible result expressions.
#' For unmatched expressions null is returned.
#'
#' @rdname column
setMethod("when", signature(condition = "Column", value = "ANY"),
          function(condition, value) {
              condition <- condition@jc
              value <- ifelse(class(value) == "Column", value@jc, value)
              jc <- callJStatic("org.apache.spark.sql.functions", "when", condition, value)
              column(jc)
          })

#' ifelse
#'
#' Evaluates a list of conditions and returns `yes` if the conditions are satisfied.
#' Otherwise `no` is returned for unmatched conditions.
#'
#' @rdname column
setMethod("ifelse",
          signature(test = "Column", yes = "ANY", no = "ANY"),
          function(test, yes, no) {
              test <- test@jc
              yes <- ifelse(class(yes) == "Column", yes@jc, yes)
              no <- ifelse(class(no) == "Column", no@jc, no)
              jc <- callJMethod(callJStatic("org.apache.spark.sql.functions",
                                            "when",
                                            test, yes),
                                "otherwise", no)
              column(jc)
          })

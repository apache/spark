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

#' Aggregate functions for Column operations
#'
#' Aggregate functions defined for \code{Column}.
#'
#' @param x Column to compute on.
#' @param y,na.rm,use currently not used.
#' @param ... additional argument(s). For example, it could be used to pass additional Columns.
#' @name column_aggregate_functions
#' @rdname column_aggregate_functions
#' @family aggregate functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))}
NULL

#' Date time functions for Column operations
#'
#' Date time functions defined for \code{Column}.
#'
#' @param x Column to compute on.
#' @param format For \code{to_date} and \code{to_timestamp}, it is the string to use to parse
#'               x Column to DateType or TimestampType. For \code{trunc}, it is the string used
#'               for specifying the truncation method. For example, "year", "yyyy", "yy" for
#'               truncate by year, or "month", "mon", "mm" for truncate by month.
#' @param ... additional argument(s).
#' @name column_datetime_functions
#' @rdname column_datetime_functions
#' @family data time functions
#' @examples
#' \dontrun{
#' dts <- c("2005-01-02 18:47:22",
#'         "2005-12-24 16:30:58",
#'         "2005-10-28 07:30:05",
#'         "2005-12-28 07:01:05",
#'         "2006-01-24 00:01:10")
#' y <- c(2.0, 2.2, 3.4, 2.5, 1.8)
#' df <- createDataFrame(data.frame(time = as.POSIXct(dts), y = y))}
NULL

#' Date time arithmetic functions for Column operations
#'
#' Date time arithmetic functions defined for \code{Column}.
#'
#' @param y Column to compute on.
#' @param x For class \code{Column}, it is the column used to perform arithmetic operations
#'          with column \code{y}. For class \code{numeric}, it is the number of months or
#'          days to be added to or subtracted from \code{y}. For class \code{character}, it is
#'          \itemize{
#'          \item \code{date_format}: date format specification.
#'          \item \code{from_utc_timestamp}, \code{to_utc_timestamp}: time zone to use.
#'          \item \code{next_day}: day of the week string.
#'          }
#'
#' @name column_datetime_diff_functions
#' @rdname column_datetime_diff_functions
#' @family data time functions
#' @examples
#' \dontrun{
#' dts <- c("2005-01-02 18:47:22",
#'         "2005-12-24 16:30:58",
#'         "2005-10-28 07:30:05",
#'         "2005-12-28 07:01:05",
#'         "2006-01-24 00:01:10")
#' y <- c(2.0, 2.2, 3.4, 2.5, 1.8)
#' df <- createDataFrame(data.frame(time = as.POSIXct(dts), y = y))}
NULL

#' String functions for Column operations
#'
#' String functions defined for \code{Column}.
#'
#' @param x Column to compute on. In \code{instr}, it is the substring to check. In \code{format_number},
#'          it is the number of decimal place to format to.
#' @param ... additional columns.
#' @name column_string_functions
#' @rdname column_string_functions
#' @family string functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(as.data.frame(Titanic, stringsAsFactors = FALSE))}
NULL

#' lit
#'
#' A new \linkS4class{Column} is created to represent the literal value.
#' If the parameter is a \linkS4class{Column}, it is returned unchanged.
#'
#' @param x a literal value or a Column.
#' @family non-aggregate functions
#' @rdname lit
#' @name lit
#' @export
#' @aliases lit,ANY-method
#' @examples
#' \dontrun{
#' lit(df$name)
#' select(df, lit("x"))
#' select(df, lit("2015-01-01"))
#'}
#' @note lit since 1.5.0
setMethod("lit", signature("ANY"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lit",
                              if (class(x) == "Column") { x@jc } else { x })
            column(jc)
          })

#' abs
#'
#' Computes the absolute value.
#'
#' @param x Column to compute on.
#'
#' @rdname abs
#' @name abs
#' @family non-aggregate functions
#' @export
#' @examples \dontrun{abs(df$c)}
#' @aliases abs,Column-method
#' @note abs since 1.5.0
setMethod("abs",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "abs", x@jc)
            column(jc)
          })

#' acos
#'
#' Computes the cosine inverse of the given value; the returned angle is in the range
#' 0.0 through pi.
#'
#' @param x Column to compute on.
#'
#' @rdname acos
#' @name acos
#' @family math functions
#' @export
#' @examples \dontrun{acos(df$c)}
#' @aliases acos,Column-method
#' @note acos since 1.5.0
setMethod("acos",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "acos", x@jc)
            column(jc)
          })

#' @details
#' \code{approxCountDistinct}: Returns the approximate number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @export
#' @aliases approxCountDistinct approxCountDistinct,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, approxCountDistinct(df$gear)))
#' head(select(df, approxCountDistinct(df$gear, 0.02)))
#' head(select(df, countDistinct(df$gear, df$cyl)))
#' head(select(df, n_distinct(df$gear)))
#' head(distinct(select(df, "gear")))}
#' @note approxCountDistinct(Column) since 1.4.0
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approxCountDistinct", x@jc)
            column(jc)
          })

#' @details
#' \code{ascii}: Computes the numeric value of the first character of the string column,
#' and returns the result as an int column.
#'
#' @rdname column_string_functions
#' @export
#' @aliases ascii ascii,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, ascii(df$Class), ascii(df$Sex)))}
#' @note ascii since 1.5.0
setMethod("ascii",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ascii", x@jc)
            column(jc)
          })

#' asin
#'
#' Computes the sine inverse of the given value; the returned angle is in the range
#' -pi/2 through pi/2.
#'
#' @param x Column to compute on.
#'
#' @rdname asin
#' @name asin
#' @family math functions
#' @export
#' @aliases asin,Column-method
#' @examples \dontrun{asin(df$c)}
#' @note asin since 1.5.0
setMethod("asin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "asin", x@jc)
            column(jc)
          })

#' atan
#'
#' Computes the tangent inverse of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname atan
#' @name atan
#' @family math functions
#' @export
#' @aliases atan,Column-method
#' @examples \dontrun{atan(df$c)}
#' @note atan since 1.5.0
setMethod("atan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "atan", x@jc)
            column(jc)
          })

#' avg
#'
#' Aggregate function: returns the average of the values in a group.
#'
#' @rdname avg
#' @name avg
#' @family aggregate functions
#' @export
#' @aliases avg,Column-method
#' @examples \dontrun{avg(df$c)}
#' @note avg since 1.4.0
setMethod("avg",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "avg", x@jc)
            column(jc)
          })

#' @details
#' \code{base64}: Computes the BASE64 encoding of a binary column and returns it as
#' a string column. This is the reverse of unbase64.
#'
#' @rdname column_string_functions
#' @export
#' @aliases base64 base64,Column-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, s1 = encode(df$Class, 'UTF-8'))
#' str(tmp)
#' tmp2 <- mutate(tmp, s2 = base64(tmp$s1), s3 = decode(tmp$s1, 'UTF-8'))
#' head(tmp2)
#' head(select(tmp2, unbase64(tmp2$s2)))}
#' @note base64 since 1.5.0
setMethod("base64",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "base64", x@jc)
            column(jc)
          })

#' bin
#'
#' An expression that returns the string representation of the binary value of the given long
#' column. For example, bin("12") returns "1100".
#'
#' @param x Column to compute on.
#'
#' @rdname bin
#' @name bin
#' @family math functions
#' @export
#' @aliases bin,Column-method
#' @examples \dontrun{bin(df$c)}
#' @note bin since 1.5.0
setMethod("bin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bin", x@jc)
            column(jc)
          })

#' bitwiseNOT
#'
#' Computes bitwise NOT.
#'
#' @param x Column to compute on.
#'
#' @rdname bitwiseNOT
#' @name bitwiseNOT
#' @family non-aggregate functions
#' @export
#' @aliases bitwiseNOT,Column-method
#' @examples \dontrun{bitwiseNOT(df$c)}
#' @note bitwiseNOT since 1.5.0
setMethod("bitwiseNOT",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bitwiseNOT", x@jc)
            column(jc)
          })

#' cbrt
#'
#' Computes the cube-root of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname cbrt
#' @name cbrt
#' @family math functions
#' @export
#' @aliases cbrt,Column-method
#' @examples \dontrun{cbrt(df$c)}
#' @note cbrt since 1.4.0
setMethod("cbrt",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cbrt", x@jc)
            column(jc)
          })

#' Computes the ceiling of the given value
#'
#' Computes the ceiling of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname ceil
#' @name ceil
#' @family math functions
#' @export
#' @aliases ceil,Column-method
#' @examples \dontrun{ceil(df$c)}
#' @note ceil since 1.5.0
setMethod("ceil",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ceil", x@jc)
            column(jc)
          })

#' Returns the first column that is not NA
#'
#' Returns the first column that is not NA, or NA if all inputs are.
#'
#' @rdname coalesce
#' @name coalesce
#' @family non-aggregate functions
#' @export
#' @aliases coalesce,Column-method
#' @examples \dontrun{coalesce(df$c, df$d, df$e)}
#' @note coalesce(Column) since 2.1.1
setMethod("coalesce",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "coalesce", jcols)
            column(jc)
          })

#' Though scala functions has "col" function, we don't expose it in SparkR
#' because we don't want to conflict with the "col" function in the R base
#' package and we also have "column" function exported which is an alias of "col".
#' @noRd
col <- function(x) {
  column(callJStatic("org.apache.spark.sql.functions", "col", x))
}

#' Returns a Column based on the given column name
#'
#' Returns a Column based on the given column name.
#'
#' @param x Character column name.
#'
#' @rdname column
#' @name column
#' @family non-aggregate functions
#' @export
#' @aliases column,character-method
#' @examples \dontrun{column("name")}
#' @note column since 1.6.0
setMethod("column",
          signature(x = "character"),
          function(x) {
            col(x)
          })
#' corr
#'
#' Computes the Pearson Correlation Coefficient for two Columns.
#'
#' @param col2 a (second) Column.
#'
#' @rdname corr
#' @name corr
#' @family aggregate functions
#' @export
#' @aliases corr,Column-method
#' @examples
#' \dontrun{
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' head(select(df, corr(df$mpg, df$hp)))}
#' @note corr since 1.6.0
setMethod("corr", signature(x = "Column"),
          function(x, col2) {
            stopifnot(class(col2) == "Column")
            jc <- callJStatic("org.apache.spark.sql.functions", "corr", x@jc, col2@jc)
            column(jc)
          })

#' cov
#'
#' Compute the covariance between two expressions.
#'
#' @details
#' \code{cov}: Compute the sample covariance between two expressions.
#'
#' @rdname cov
#' @name cov
#' @family aggregate functions
#' @export
#' @aliases cov,characterOrColumn-method
#' @examples
#' \dontrun{
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' head(select(df, cov(df$mpg, df$hp), cov("mpg", "hp"),
#'                 covar_samp(df$mpg, df$hp), covar_samp("mpg", "hp"),
#'                 covar_pop(df$mpg, df$hp), covar_pop("mpg", "hp")))}
#' @note cov since 1.6.0
setMethod("cov", signature(x = "characterOrColumn"),
          function(x, col2) {
            stopifnot(is(class(col2), "characterOrColumn"))
            covar_samp(x, col2)
          })

#' @details
#' \code{covar_sample}: Alias for \code{cov}.
#'
#' @rdname cov
#'
#' @param col1 the first Column.
#' @param col2 the second Column.
#' @name covar_samp
#' @aliases covar_samp,characterOrColumn,characterOrColumn-method
#' @note covar_samp since 2.0.0
setMethod("covar_samp", signature(col1 = "characterOrColumn", col2 = "characterOrColumn"),
          function(col1, col2) {
            stopifnot(class(col1) == class(col2))
            if (class(col1) == "Column") {
              col1 <- col1@jc
              col2 <- col2@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "covar_samp", col1, col2)
            column(jc)
          })

#' @details
#' \code{covar_pop}: Computes the population covariance between two expressions.
#'
#' @rdname cov
#' @name covar_pop
#' @export
#' @aliases covar_pop,characterOrColumn,characterOrColumn-method
#' @note covar_pop since 2.0.0
setMethod("covar_pop", signature(col1 = "characterOrColumn", col2 = "characterOrColumn"),
          function(col1, col2) {
            stopifnot(class(col1) == class(col2))
            if (class(col1) == "Column") {
              col1 <- col1@jc
              col2 <- col2@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "covar_pop", col1, col2)
            column(jc)
          })

#' cos
#'
#' Computes the cosine of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname cos
#' @name cos
#' @family math functions
#' @aliases cos,Column-method
#' @export
#' @examples \dontrun{cos(df$c)}
#' @note cos since 1.5.0
setMethod("cos",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cos", x@jc)
            column(jc)
          })

#' cosh
#'
#' Computes the hyperbolic cosine of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname cosh
#' @name cosh
#' @family math functions
#' @aliases cosh,Column-method
#' @export
#' @examples \dontrun{cosh(df$c)}
#' @note cosh since 1.5.0
setMethod("cosh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cosh", x@jc)
            column(jc)
          })

#' Returns the number of items in a group
#'
#' This can be used as a column aggregate function with \code{Column} as input,
#' and returns the number of items in a group.
#'
#' @rdname count
#' @name count
#' @family aggregate functions
#' @aliases count,Column-method
#' @export
#' @examples \dontrun{count(df$c)}
#' @note count since 1.4.0
setMethod("count",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "count", x@jc)
            column(jc)
          })

#' crc32
#'
#' Calculates the cyclic redundancy check value  (CRC32) of a binary column and
#' returns the value as a bigint.
#'
#' @param x Column to compute on.
#'
#' @rdname crc32
#' @name crc32
#' @family misc functions
#' @aliases crc32,Column-method
#' @export
#' @examples \dontrun{crc32(df$c)}
#' @note crc32 since 1.5.0
setMethod("crc32",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "crc32", x@jc)
            column(jc)
          })

#' hash
#'
#' Calculates the hash code of given columns, and returns the result as a int column.
#'
#' @param x Column to compute on.
#' @param ... additional Column(s) to be included.
#'
#' @rdname hash
#' @name hash
#' @family misc functions
#' @aliases hash,Column-method
#' @export
#' @examples \dontrun{hash(df$c)}
#' @note hash since 2.0.0
setMethod("hash",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "hash", jcols)
            column(jc)
          })

#' @details
#' \code{dayofmonth}: Extracts the day of the month as an integer from a
#' given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases dayofmonth dayofmonth,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, df$time, year(df$time), quarter(df$time), month(df$time),
#'            dayofmonth(df$time), dayofyear(df$time), weekofyear(df$time)))
#' head(agg(groupBy(df, year(df$time)), count(df$y), avg(df$y)))
#' head(agg(groupBy(df, month(df$time)), avg(df$y)))}
#' @note dayofmonth since 1.5.0
setMethod("dayofmonth",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "dayofmonth", x@jc)
            column(jc)
          })

#' @details
#' \code{dayofyear}: Extracts the day of the year as an integer from a
#' given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases dayofyear dayofyear,Column-method
#' @export
#' @note dayofyear since 1.5.0
setMethod("dayofyear",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "dayofyear", x@jc)
            column(jc)
          })

#' @details
#' \code{decode}: Computes the first argument into a string from a binary using the provided
#' character set.
#'
#' @param charset Character set to use (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE',
#'                'UTF-16LE', 'UTF-16').
#'
#' @rdname column_string_functions
#' @aliases decode decode,Column,character-method
#' @export
#' @note decode since 1.6.0
setMethod("decode",
          signature(x = "Column", charset = "character"),
          function(x, charset) {
            jc <- callJStatic("org.apache.spark.sql.functions", "decode", x@jc, charset)
            column(jc)
          })

#' @details
#' \code{encode}: Computes the first argument into a binary from a string using the provided
#' character set.
#'
#' @rdname column_string_functions
#' @aliases encode encode,Column,character-method
#' @export
#' @note encode since 1.6.0
setMethod("encode",
          signature(x = "Column", charset = "character"),
          function(x, charset) {
            jc <- callJStatic("org.apache.spark.sql.functions", "encode", x@jc, charset)
            column(jc)
          })

#' exp
#'
#' Computes the exponential of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname exp
#' @name exp
#' @family math functions
#' @aliases exp,Column-method
#' @export
#' @examples \dontrun{exp(df$c)}
#' @note exp since 1.5.0
setMethod("exp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "exp", x@jc)
            column(jc)
          })

#' expm1
#'
#' Computes the exponential of the given value minus one.
#'
#' @param x Column to compute on.
#'
#' @rdname expm1
#' @name expm1
#' @aliases expm1,Column-method
#' @family math functions
#' @export
#' @examples \dontrun{expm1(df$c)}
#' @note expm1 since 1.5.0
setMethod("expm1",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "expm1", x@jc)
            column(jc)
          })

#' factorial
#'
#' Computes the factorial of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname factorial
#' @name factorial
#' @aliases factorial,Column-method
#' @family math functions
#' @export
#' @examples \dontrun{factorial(df$c)}
#' @note factorial since 1.5.0
setMethod("factorial",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "factorial", x@jc)
            column(jc)
          })

#' first
#'
#' Aggregate function: returns the first value in a group.
#'
#' The function by default returns the first values it sees. It will return the first non-missing
#' value it sees when na.rm is set to true. If all values are missing, then NA is returned.
#'
#' @param na.rm a logical value indicating whether NA values should be stripped
#'        before the computation proceeds.
#'
#' @rdname first
#' @name first
#' @aliases first,characterOrColumn-method
#' @family aggregate functions
#' @export
#' @examples
#' \dontrun{
#' first(df$c)
#' first(df$c, TRUE)
#' }
#' @note first(characterOrColumn) since 1.4.0
setMethod("first",
          signature(x = "characterOrColumn"),
          function(x, na.rm = FALSE) {
            col <- if (class(x) == "Column") {
              x@jc
            } else {
              x
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "first", col, na.rm)
            column(jc)
          })

#' floor
#'
#' Computes the floor of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname floor
#' @name floor
#' @aliases floor,Column-method
#' @family math functions
#' @export
#' @examples \dontrun{floor(df$c)}
#' @note floor since 1.5.0
setMethod("floor",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "floor", x@jc)
            column(jc)
          })

#' hex
#'
#' Computes hex value of the given column.
#'
#' @param x Column to compute on.
#'
#' @rdname hex
#' @name hex
#' @family math functions
#' @aliases hex,Column-method
#' @export
#' @examples \dontrun{hex(df$c)}
#' @note hex since 1.5.0
setMethod("hex",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "hex", x@jc)
            column(jc)
          })

#' @details
#' \code{hour}: Extracts the hours as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases hour hour,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, hour(df$time), minute(df$time), second(df$time)))
#' head(agg(groupBy(df, dayofmonth(df$time)), avg(df$y)))
#' head(agg(groupBy(df, hour(df$time)), avg(df$y)))
#' head(agg(groupBy(df, minute(df$time)), avg(df$y)))}
#' @note hour since 1.5.0
setMethod("hour",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "hour", x@jc)
            column(jc)
          })

#' @details
#' \code{initcap}: Returns a new string column by converting the first letter of
#' each word to uppercase. Words are delimited by whitespace. For example, "hello world"
#' will become "Hello World".
#'
#' @rdname column_string_functions
#' @aliases initcap initcap,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, SexLower = lower(df$Sex), AgeUpper = upper(df$age))
#' head(tmp)
#' tmp2 <- mutate(tmp, s1 = initcap(tmp$SexLower), s2 = reverse(df$Sex))
#' head(tmp2)}
#' @note initcap since 1.5.0
setMethod("initcap",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "initcap", x@jc)
            column(jc)
          })

#' is.nan
#'
#' Return true if the column is NaN, alias for \link{isnan}
#'
#' @param x Column to compute on.
#'
#' @rdname is.nan
#' @name is.nan
#' @family non-aggregate functions
#' @aliases is.nan,Column-method
#' @export
#' @examples
#' \dontrun{
#' is.nan(df$c)
#' isnan(df$c)
#' }
#' @note is.nan since 2.0.0
setMethod("is.nan",
          signature(x = "Column"),
          function(x) {
            isnan(x)
          })

#' @rdname is.nan
#' @name isnan
#' @aliases isnan,Column-method
#' @note isnan since 2.0.0
setMethod("isnan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "isnan", x@jc)
            column(jc)
          })

#' @details
#' \code{kurtosis}: Returns the kurtosis of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases kurtosis kurtosis,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, mean(df$mpg), sd(df$mpg), skewness(df$mpg), kurtosis(df$mpg)))}
#' @note kurtosis since 1.6.0
setMethod("kurtosis",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "kurtosis", x@jc)
            column(jc)
          })

#' last
#'
#' Aggregate function: returns the last value in a group.
#'
#' The function by default returns the last values it sees. It will return the last non-missing
#' value it sees when na.rm is set to true. If all values are missing, then NA is returned.
#'
#' @param x column to compute on.
#' @param na.rm a logical value indicating whether NA values should be stripped
#'        before the computation proceeds.
#' @param ... further arguments to be passed to or from other methods.
#'
#' @rdname last
#' @name last
#' @aliases last,characterOrColumn-method
#' @family aggregate functions
#' @export
#' @examples
#' \dontrun{
#' last(df$c)
#' last(df$c, TRUE)
#' }
#' @note last since 1.4.0
setMethod("last",
          signature(x = "characterOrColumn"),
          function(x, na.rm = FALSE) {
            col <- if (class(x) == "Column") {
              x@jc
            } else {
              x
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "last", col, na.rm)
            column(jc)
          })

#' @details
#' \code{last_day}: Given a date column, returns the last day of the month which the
#' given date belongs to. For example, input "2015-07-27" returns "2015-07-31" since
#' July 31 is the last day of the month in July 2015.
#'
#' @rdname column_datetime_functions
#' @aliases last_day last_day,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, df$time, last_day(df$time), month(df$time)))}
#' @note last_day since 1.5.0
setMethod("last_day",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "last_day", x@jc)
            column(jc)
          })

#' @details
#' \code{length}: Computes the length of a given string or binary column.
#'
#' @rdname column_string_functions
#' @aliases length length,Column-method
#' @export
#' @note length since 1.5.0
setMethod("length",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "length", x@jc)
            column(jc)
          })

#' log
#'
#' Computes the natural logarithm of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname log
#' @name log
#' @aliases log,Column-method
#' @family math functions
#' @export
#' @examples \dontrun{log(df$c)}
#' @note log since 1.5.0
setMethod("log",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log", x@jc)
            column(jc)
          })

#' log10
#'
#' Computes the logarithm of the given value in base 10.
#'
#' @param x Column to compute on.
#'
#' @rdname log10
#' @name log10
#' @family math functions
#' @aliases log10,Column-method
#' @export
#' @examples \dontrun{log10(df$c)}
#' @note log10 since 1.5.0
setMethod("log10",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log10", x@jc)
            column(jc)
          })

#' log1p
#'
#' Computes the natural logarithm of the given value plus one.
#'
#' @param x Column to compute on.
#'
#' @rdname log1p
#' @name log1p
#' @family math functions
#' @aliases log1p,Column-method
#' @export
#' @examples \dontrun{log1p(df$c)}
#' @note log1p since 1.5.0
setMethod("log1p",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log1p", x@jc)
            column(jc)
          })

#' log2
#'
#' Computes the logarithm of the given column in base 2.
#'
#' @param x Column to compute on.
#'
#' @rdname log2
#' @name log2
#' @family math functions
#' @aliases log2,Column-method
#' @export
#' @examples \dontrun{log2(df$c)}
#' @note log2 since 1.5.0
setMethod("log2",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log2", x@jc)
            column(jc)
          })

#' @details
#' \code{lower}: Converts a string column to lower case.
#'
#' @rdname column_string_functions
#' @aliases lower lower,Column-method
#' @export
#' @note lower since 1.4.0
setMethod("lower",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "lower", x@jc)
            column(jc)
          })

#' @details
#' \code{ltrim}: Trims the spaces from left end for the specified string value.
#'
#' @rdname column_string_functions
#' @aliases ltrim ltrim,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, SexLpad = lpad(df$Sex, 6, " "), SexRpad = rpad(df$Sex, 7, " "))
#' head(select(tmp, length(tmp$Sex), length(tmp$SexLpad), length(tmp$SexRpad)))
#' tmp2 <- mutate(tmp, SexLtrim = ltrim(tmp$SexLpad), SexRtrim = rtrim(tmp$SexRpad),
#'                     SexTrim = trim(tmp$SexLpad))
#' head(select(tmp2, length(tmp2$Sex), length(tmp2$SexLtrim),
#'                   length(tmp2$SexRtrim), length(tmp2$SexTrim)))
#'
#' tmp <- mutate(df, SexLpad = lpad(df$Sex, 6, "xx"), SexRpad = rpad(df$Sex, 7, "xx"))
#' head(tmp)}
#' @note ltrim since 1.5.0
setMethod("ltrim",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ltrim", x@jc)
            column(jc)
          })

#' @details
#' \code{max}: Returns the maximum value of the expression in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases max max,Column-method
#' @note max since 1.5.0
setMethod("max",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "max", x@jc)
            column(jc)
          })

#' md5
#'
#' Calculates the MD5 digest of a binary column and returns the value
#' as a 32 character hex string.
#'
#' @param x Column to compute on.
#'
#' @rdname md5
#' @name md5
#' @family misc functions
#' @aliases md5,Column-method
#' @export
#' @examples \dontrun{md5(df$c)}
#' @note md5 since 1.5.0
setMethod("md5",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "md5", x@jc)
            column(jc)
          })

#' @details
#' \code{mean}: Returns the average of the values in a group. Alias for \code{avg}.
#'
#' @rdname column_aggregate_functions
#' @aliases mean mean,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, avg(df$mpg), mean(df$mpg), sum(df$mpg), min(df$wt), max(df$qsec)))
#'
#' # metrics by num of cylinders
#' tmp <- agg(groupBy(df, "cyl"), avg(df$mpg), avg(df$hp), avg(df$wt), avg(df$qsec))
#' head(orderBy(tmp, "cyl"))
#'
#' # car with the max mpg
#' mpg_max <- as.numeric(collect(agg(df, max(df$mpg))))
#' head(where(df, df$mpg == mpg_max))}
#' @note mean since 1.5.0
setMethod("mean",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "mean", x@jc)
            column(jc)
          })

#' @details
#' \code{min}: Returns the minimum value of the expression in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases min min,Column-method
#' @export
#' @note min since 1.5.0
setMethod("min",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "min", x@jc)
            column(jc)
          })

#' @details
#' \code{minute}: Extracts the minutes as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases minute minute,Column-method
#' @export
#' @note minute since 1.5.0
setMethod("minute",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "minute", x@jc)
            column(jc)
          })

#' monotonically_increasing_id
#'
#' Return a column that generates monotonically increasing 64-bit integers.
#'
#' The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
#' The current implementation puts the partition ID in the upper 31 bits, and the record number
#' within each partition in the lower 33 bits. The assumption is that the SparkDataFrame has
#' less than 1 billion partitions, and each partition has less than 8 billion records.
#'
#' As an example, consider a SparkDataFrame with two partitions, each with 3 records.
#' This expression would return the following IDs:
#' 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
#'
#' This is equivalent to the MONOTONICALLY_INCREASING_ID function in SQL.
#'
#' @rdname monotonically_increasing_id
#' @aliases monotonically_increasing_id,missing-method
#' @name monotonically_increasing_id
#' @family misc functions
#' @export
#' @examples \dontrun{select(df, monotonically_increasing_id())}
setMethod("monotonically_increasing_id",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "monotonically_increasing_id")
            column(jc)
          })

#' @details
#' \code{month}: Extracts the month as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases month month,Column-method
#' @export
#' @note month since 1.5.0
setMethod("month",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "month", x@jc)
            column(jc)
          })

#' negate
#'
#' Unary minus, i.e. negate the expression.
#'
#' @param x Column to compute on.
#'
#' @rdname negate
#' @name negate
#' @family non-aggregate functions
#' @aliases negate,Column-method
#' @export
#' @examples \dontrun{negate(df$c)}
#' @note negate since 1.5.0
setMethod("negate",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "negate", x@jc)
            column(jc)
          })

#' @details
#' \code{quarter}: Extracts the quarter as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases quarter quarter,Column-method
#' @export
#' @note quarter since 1.5.0
setMethod("quarter",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "quarter", x@jc)
            column(jc)
          })

#' @details
#' \code{reverse}: Reverses the string column and returns it as a new string column.
#'
#' @rdname column_string_functions
#' @aliases reverse reverse,Column-method
#' @export
#' @note reverse since 1.5.0
setMethod("reverse",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "reverse", x@jc)
            column(jc)
          })

#' rint
#'
#' Returns the double value that is closest in value to the argument and
#' is equal to a mathematical integer.
#'
#' @param x Column to compute on.
#'
#' @rdname rint
#' @name rint
#' @family math functions
#' @aliases rint,Column-method
#' @export
#' @examples \dontrun{rint(df$c)}
#' @note rint since 1.5.0
setMethod("rint",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rint", x@jc)
            column(jc)
          })

#' round
#'
#' Returns the value of the column \code{e} rounded to 0 decimal places using HALF_UP rounding mode.
#'
#' @param x Column to compute on.
#'
#' @rdname round
#' @name round
#' @family math functions
#' @aliases round,Column-method
#' @export
#' @examples \dontrun{round(df$c)}
#' @note round since 1.5.0
setMethod("round",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "round", x@jc)
            column(jc)
          })

#' bround
#'
#' Returns the value of the column \code{e} rounded to \code{scale} decimal places using HALF_EVEN rounding
#' mode if \code{scale} >= 0 or at integer part when \code{scale} < 0.
#' Also known as Gaussian rounding or bankers' rounding that rounds to the nearest even number.
#' bround(2.5, 0) = 2, bround(3.5, 0) = 4.
#'
#' @param x Column to compute on.
#' @param scale round to \code{scale} digits to the right of the decimal point when \code{scale} > 0,
#'        the nearest even number when \code{scale} = 0, and \code{scale} digits to the left
#'        of the decimal point when \code{scale} < 0.
#' @param ... further arguments to be passed to or from other methods.
#' @rdname bround
#' @name bround
#' @family math functions
#' @aliases bround,Column-method
#' @export
#' @examples \dontrun{bround(df$c, 0)}
#' @note bround since 2.0.0
setMethod("bround",
          signature(x = "Column"),
          function(x, scale = 0) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bround", x@jc, as.integer(scale))
            column(jc)
          })

#' @details
#' \code{rtrim}: Trims the spaces from right end for the specified string value.
#'
#' @rdname column_string_functions
#' @aliases rtrim rtrim,Column-method
#' @export
#' @note rtrim since 1.5.0
setMethod("rtrim",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rtrim", x@jc)
            column(jc)
          })

#' @details
#' \code{sd}: Alias for \code{stddev_samp}.
#'
#' @rdname column_aggregate_functions
#' @aliases sd sd,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, sd(df$mpg), stddev(df$mpg), stddev_pop(df$wt), stddev_samp(df$qsec)))}
#' @note sd since 1.6.0
setMethod("sd",
          signature(x = "Column"),
          function(x) {
            # In R, sample standard deviation is calculated with the sd() function.
            stddev_samp(x)
          })

#' @details
#' \code{second}: Extracts the seconds as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases second second,Column-method
#' @export
#' @note second since 1.5.0
setMethod("second",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "second", x@jc)
            column(jc)
          })

#' sha1
#'
#' Calculates the SHA-1 digest of a binary column and returns the value
#' as a 40 character hex string.
#'
#' @param x Column to compute on.
#'
#' @rdname sha1
#' @name sha1
#' @family misc functions
#' @aliases sha1,Column-method
#' @export
#' @examples \dontrun{sha1(df$c)}
#' @note sha1 since 1.5.0
setMethod("sha1",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sha1", x@jc)
            column(jc)
          })

#' signum
#'
#' Computes the signum of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname sign
#' @name signum
#' @aliases signum,Column-method
#' @family math functions
#' @export
#' @examples \dontrun{signum(df$c)}
#' @note signum since 1.5.0
setMethod("signum",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "signum", x@jc)
            column(jc)
          })

#' sin
#'
#' Computes the sine of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname sin
#' @name sin
#' @family math functions
#' @aliases sin,Column-method
#' @export
#' @examples \dontrun{sin(df$c)}
#' @note sin since 1.5.0
setMethod("sin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sin", x@jc)
            column(jc)
          })

#' sinh
#'
#' Computes the hyperbolic sine of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname sinh
#' @name sinh
#' @family math functions
#' @aliases sinh,Column-method
#' @export
#' @examples \dontrun{sinh(df$c)}
#' @note sinh since 1.5.0
setMethod("sinh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sinh", x@jc)
            column(jc)
          })

#' @details
#' \code{skewness}: Returns the skewness of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases skewness skewness,Column-method
#' @export
#' @note skewness since 1.6.0
setMethod("skewness",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "skewness", x@jc)
            column(jc)
          })

#' @details
#' \code{soundex}: Returns the soundex code for the specified expression.
#'
#' @rdname column_string_functions
#' @aliases soundex soundex,Column-method
#' @export
#' @note soundex since 1.5.0
setMethod("soundex",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "soundex", x@jc)
            column(jc)
          })

#' Return the partition ID as a column
#'
#' Return the partition ID as a SparkDataFrame column.
#' Note that this is nondeterministic because it depends on data partitioning and
#' task scheduling.
#'
#' This is equivalent to the SPARK_PARTITION_ID function in SQL.
#'
#' @rdname spark_partition_id
#' @name spark_partition_id
#' @aliases spark_partition_id,missing-method
#' @export
#' @examples
#' \dontrun{select(df, spark_partition_id())}
#' @note spark_partition_id since 2.0.0
setMethod("spark_partition_id",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "spark_partition_id")
            column(jc)
          })

#' @details
#' \code{stddev}: Alias for \code{std_dev}.
#'
#' @rdname column_aggregate_functions
#' @aliases stddev stddev,Column-method
#' @note stddev since 1.6.0
setMethod("stddev",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "stddev", x@jc)
            column(jc)
          })

#' @details
#' \code{stddev_pop}: Returns the population standard deviation of the expression in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases stddev_pop stddev_pop,Column-method
#' @export
#' @note stddev_pop since 1.6.0
setMethod("stddev_pop",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "stddev_pop", x@jc)
            column(jc)
          })

#' @details
#' \code{stddev_samp}: Returns the unbiased sample standard deviation of the expression in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases stddev_samp stddev_samp,Column-method
#' @export
#' @note stddev_samp since 1.6.0
setMethod("stddev_samp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "stddev_samp", x@jc)
            column(jc)
          })

#' struct
#'
#' Creates a new struct column that composes multiple input columns.
#'
#' @param x a column to compute on.
#' @param ... optional column(s) to be included.
#'
#' @rdname struct
#' @name struct
#' @family non-aggregate functions
#' @aliases struct,characterOrColumn-method
#' @export
#' @examples
#' \dontrun{
#' struct(df$c, df$d)
#' struct("col1", "col2")
#' }
#' @note struct since 1.6.0
setMethod("struct",
          signature(x = "characterOrColumn"),
          function(x, ...) {
            if (class(x) == "Column") {
              jcols <- lapply(list(x, ...), function(x) { x@jc })
              jc <- callJStatic("org.apache.spark.sql.functions", "struct", jcols)
            } else {
              jc <- callJStatic("org.apache.spark.sql.functions", "struct", x, list(...))
            }
            column(jc)
          })

#' sqrt
#'
#' Computes the square root of the specified float value.
#'
#' @param x Column to compute on.
#'
#' @rdname sqrt
#' @name sqrt
#' @family math functions
#' @aliases sqrt,Column-method
#' @export
#' @examples \dontrun{sqrt(df$c)}
#' @note sqrt since 1.5.0
setMethod("sqrt",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sqrt", x@jc)
            column(jc)
          })

#' @details
#' \code{sum}: Returns the sum of all values in the expression.
#'
#' @rdname column_aggregate_functions
#' @aliases sum sum,Column-method
#' @export
#' @note sum since 1.5.0
setMethod("sum",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sum", x@jc)
            column(jc)
          })

#' @details
#' \code{sumDistinct}: Returns the sum of distinct values in the expression.
#'
#' @rdname column_aggregate_functions
#' @aliases sumDistinct sumDistinct,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, sumDistinct(df$gear)))
#' head(distinct(select(df, "gear")))}
#' @note sumDistinct since 1.4.0
setMethod("sumDistinct",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sumDistinct", x@jc)
            column(jc)
          })

#' tan
#'
#' Computes the tangent of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname tan
#' @name tan
#' @family math functions
#' @aliases tan,Column-method
#' @export
#' @examples \dontrun{tan(df$c)}
#' @note tan since 1.5.0
setMethod("tan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "tan", x@jc)
            column(jc)
          })

#' tanh
#'
#' Computes the hyperbolic tangent of the given value.
#'
#' @param x Column to compute on.
#'
#' @rdname tanh
#' @name tanh
#' @family math functions
#' @aliases tanh,Column-method
#' @export
#' @examples \dontrun{tanh(df$c)}
#' @note tanh since 1.5.0
setMethod("tanh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "tanh", x@jc)
            column(jc)
          })

#' toDegrees
#'
#' Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
#'
#' @param x Column to compute on.
#'
#' @rdname toDegrees
#' @name toDegrees
#' @family math functions
#' @aliases toDegrees,Column-method
#' @export
#' @examples \dontrun{toDegrees(df$c)}
#' @note toDegrees since 1.4.0
setMethod("toDegrees",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "toDegrees", x@jc)
            column(jc)
          })

#' toRadians
#'
#' Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
#'
#' @param x Column to compute on.
#'
#' @rdname toRadians
#' @name toRadians
#' @family math functions
#' @aliases toRadians,Column-method
#' @export
#' @examples \dontrun{toRadians(df$c)}
#' @note toRadians since 1.4.0
setMethod("toRadians",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "toRadians", x@jc)
            column(jc)
          })

#' @details
#' \code{to_date}: Converts the column into a DateType. You may optionally specify
#' a format according to the rules in:
#' \url{http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html}.
#' If the string cannot be parsed according to the specified format (or default),
#' the value of the column will be null.
#' By default, it follows casting rules to a DateType if the format is omitted
#' (equivalent to \code{cast(df$x, "date")}).
#'
#' @rdname column_datetime_functions
#' @aliases to_date to_date,Column,missing-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- createDataFrame(data.frame(time_string = dts))
#' tmp2 <- mutate(tmp, date1 = to_date(tmp$time_string),
#'                    date2 = to_date(tmp$time_string, "yyyy-MM-dd"),
#'                    date3 = date_format(tmp$time_string, "MM/dd/yyy"),
#'                    time1 = to_timestamp(tmp$time_string),
#'                    time2 = to_timestamp(tmp$time_string, "yyyy-MM-dd"))
#' head(tmp2)}
#' @note to_date(Column) since 1.5.0
setMethod("to_date",
          signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_date", x@jc)
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases to_date,Column,character-method
#' @export
#' @note to_date(Column, character) since 2.2.0
setMethod("to_date",
          signature(x = "Column", format = "character"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_date", x@jc, format)
            column(jc)
          })

#' to_json
#'
#' Converts a column containing a \code{structType} or array of \code{structType} into a Column
#' of JSON string. Resolving the Column can fail if an unsupported type is encountered.
#'
#' @param x Column containing the struct or array of the structs
#' @param ... additional named properties to control how it is converted, accepts the same options
#'            as the JSON data source.
#'
#' @family non-aggregate functions
#' @rdname to_json
#' @name to_json
#' @aliases to_json,Column-method
#' @export
#' @examples
#' \dontrun{
#' # Converts a struct into a JSON object
#' df <- sql("SELECT named_struct('date', cast('2000-01-01' as date)) as d")
#' select(df, to_json(df$d, dateFormat = 'dd/MM/yyyy'))
#'
#' # Converts an array of structs into a JSON array
#' df <- sql("SELECT array(named_struct('name', 'Bob'), named_struct('name', 'Alice')) as people")
#' select(df, to_json(df$people))
#'}
#' @note to_json since 2.2.0
setMethod("to_json", signature(x = "Column"),
          function(x, ...) {
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions", "to_json", x@jc, options)
            column(jc)
          })

#' @details
#' \code{to_timestamp}: Converts the column into a TimestampType. You may optionally specify
#' a format according to the rules in:
#' \url{http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html}.
#' If the string cannot be parsed according to the specified format (or default),
#' the value of the column will be null.
#' By default, it follows casting rules to a TimestampType if the format is omitted
#' (equivalent to \code{cast(df$x, "timestamp")}).
#'
#' @rdname column_datetime_functions
#' @aliases to_timestamp to_timestamp,Column,missing-method
#' @export
#' @note to_timestamp(Column) since 2.2.0
setMethod("to_timestamp",
          signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_timestamp", x@jc)
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases to_timestamp,Column,character-method
#' @export
#' @note to_timestamp(Column, character) since 2.2.0
setMethod("to_timestamp",
          signature(x = "Column", format = "character"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_timestamp", x@jc, format)
            column(jc)
          })

#' @details
#' \code{trim}: Trims the spaces from both ends for the specified string column.
#'
#' @rdname column_string_functions
#' @aliases trim trim,Column-method
#' @export
#' @note trim since 1.5.0
setMethod("trim",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "trim", x@jc)
            column(jc)
          })

#' @details
#' \code{unbase64}: Decodes a BASE64 encoded string column and returns it as a binary column.
#' This is the reverse of base64.
#'
#' @rdname column_string_functions
#' @aliases unbase64 unbase64,Column-method
#' @export
#' @note unbase64 since 1.5.0
setMethod("unbase64",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unbase64", x@jc)
            column(jc)
          })

#' unhex
#'
#' Inverse of hex. Interprets each pair of characters as a hexadecimal number
#' and converts to the byte representation of number.
#'
#' @param x Column to compute on.
#'
#' @rdname unhex
#' @name unhex
#' @family math functions
#' @aliases unhex,Column-method
#' @export
#' @examples \dontrun{unhex(df$c)}
#' @note unhex since 1.5.0
setMethod("unhex",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unhex", x@jc)
            column(jc)
          })

#' @details
#' \code{upper}: Converts a string column to upper case.
#'
#' @rdname column_string_functions
#' @aliases upper upper,Column-method
#' @export
#' @note upper since 1.4.0
setMethod("upper",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "upper", x@jc)
            column(jc)
          })

#' @details
#' \code{var}: Alias for \code{var_samp}.
#'
#' @rdname column_aggregate_functions
#' @aliases var var,Column-method
#' @export
#' @examples
#'
#'\dontrun{
#'head(agg(df, var(df$mpg), variance(df$mpg), var_pop(df$mpg), var_samp(df$mpg)))}
#' @note var since 1.6.0
setMethod("var",
          signature(x = "Column"),
          function(x) {
            # In R, sample variance is calculated with the var() function.
            var_samp(x)
          })

#' @rdname column_aggregate_functions
#' @aliases variance variance,Column-method
#' @export
#' @note variance since 1.6.0
setMethod("variance",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "variance", x@jc)
            column(jc)
          })

#' @details
#' \code{var_pop}: Returns the population variance of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases var_pop var_pop,Column-method
#' @export
#' @note var_pop since 1.5.0
setMethod("var_pop",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "var_pop", x@jc)
            column(jc)
          })

#' @details
#' \code{var_samp}: Returns the unbiased variance of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases var_samp var_samp,Column-method
#' @export
#' @note var_samp since 1.6.0
setMethod("var_samp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "var_samp", x@jc)
            column(jc)
          })

#' @details
#' \code{weekofyear}: Extracts the week number as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases weekofyear weekofyear,Column-method
#' @export
#' @note weekofyear since 1.5.0
setMethod("weekofyear",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "weekofyear", x@jc)
            column(jc)
          })

#' @details
#' \code{year}: Extracts the year as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases year year,Column-method
#' @export
#' @note year since 1.5.0
setMethod("year",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "year", x@jc)
            column(jc)
          })

#' atan2
#'
#' Returns the angle theta from the conversion of rectangular coordinates (x, y) to
#' polar coordinates (r, theta).
#
#' @param x Column to compute on.
#' @param y Column to compute on.
#'
#' @rdname atan2
#' @name atan2
#' @family math functions
#' @aliases atan2,Column-method
#' @export
#' @examples \dontrun{atan2(df$c, x)}
#' @note atan2 since 1.5.0
setMethod("atan2", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "atan2", y@jc, x)
            column(jc)
          })

#' @details
#' \code{datediff}: Returns the number of days from \code{y} to \code{x}.
#'
#' @rdname column_datetime_diff_functions
#' @aliases datediff datediff,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- createDataFrame(data.frame(time_string1 = as.POSIXct(dts),
#'              time_string2 = as.POSIXct(dts[order(runif(length(dts)))])))
#' tmp2 <- mutate(tmp, datediff = datediff(tmp$time_string1, tmp$time_string2),
#'                monthdiff = months_between(tmp$time_string1, tmp$time_string2))
#' head(tmp2)}
#' @note datediff since 1.5.0
setMethod("datediff", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "datediff", y@jc, x)
            column(jc)
          })

#' hypot
#'
#' Computes "sqrt(a^2 + b^2)" without intermediate overflow or underflow.
#
#' @param x Column to compute on.
#' @param y Column to compute on.
#'
#' @rdname hypot
#' @name hypot
#' @family math functions
#' @aliases hypot,Column-method
#' @export
#' @examples \dontrun{hypot(df$c, x)}
#' @note hypot since 1.4.0
setMethod("hypot", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "hypot", y@jc, x)
            column(jc)
          })

#' @details
#' \code{levenshtein}: Computes the Levenshtein distance of the two given string columns.
#'
#' @param y Column to compute on.
#'
#' @rdname column_string_functions
#' @aliases levenshtein levenshtein,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, d1 = levenshtein(df$Class, df$Sex),
#'                   d2 = levenshtein(df$Age, df$Sex),
#'                   d3 = levenshtein(df$Age, df$Age))
#' head(tmp)}
#' @note levenshtein since 1.5.0
setMethod("levenshtein", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "levenshtein", y@jc, x)
            column(jc)
          })

#' @details
#' \code{months_between}: Returns number of months between dates \code{y} and \code{x}.
#'
#' @rdname column_datetime_diff_functions
#' @aliases months_between months_between,Column-method
#' @export
#' @note months_between since 1.5.0
setMethod("months_between", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "months_between", y@jc, x)
            column(jc)
          })

#' nanvl
#'
#' Returns col1 if it is not NaN, or col2 if col1 is NaN.
#' Both inputs should be floating point columns (DoubleType or FloatType).
#'
#' @param x first Column.
#' @param y second Column.
#'
#' @rdname nanvl
#' @name nanvl
#' @family non-aggregate functions
#' @aliases nanvl,Column-method
#' @export
#' @examples \dontrun{nanvl(df$c, x)}
#' @note nanvl since 1.5.0
setMethod("nanvl", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "nanvl", y@jc, x)
            column(jc)
          })

#' pmod
#'
#' Returns the positive value of dividend mod divisor.
#'
#' @param x divisor Column.
#' @param y dividend Column.
#'
#' @rdname pmod
#' @name pmod
#' @docType methods
#' @family math functions
#' @aliases pmod,Column-method
#' @export
#' @examples \dontrun{pmod(df$c, x)}
#' @note pmod since 1.5.0
setMethod("pmod", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "pmod", y@jc, x)
            column(jc)
          })

#' @param rsd maximum estimation error allowed (default = 0.05)
#'
#' @rdname column_aggregate_functions
#' @aliases approxCountDistinct,Column-method
#' @export
#' @note approxCountDistinct(Column, numeric) since 1.4.0
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x, rsd = 0.05) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approxCountDistinct", x@jc, rsd)
            column(jc)
          })

#' @details
#' \code{countDistinct}: Returns the number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases countDistinct countDistinct,Column-method
#' @export
#' @note countDistinct since 1.4.0
setMethod("countDistinct",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "countDistinct", x@jc,
                              jcols)
            column(jc)
          })

#' @details
#' \code{concat}: Concatenates multiple input string columns together into a single string column.
#'
#' @rdname column_string_functions
#' @aliases concat concat,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' # concatenate strings
#' tmp <- mutate(df, s1 = concat(df$Class, df$Sex),
#'                   s2 = concat(df$Class, df$Sex, df$Age),
#'                   s3 = concat(df$Class, df$Sex, df$Age, df$Class),
#'                   s4 = concat_ws("_", df$Class, df$Sex),
#'                   s5 = concat_ws("+", df$Class, df$Sex, df$Age, df$Survived))
#' head(tmp)}
#' @note concat since 1.5.0
setMethod("concat",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "concat", jcols)
            column(jc)
          })

#' greatest
#'
#' Returns the greatest value of the list of column names, skipping null values.
#' This function takes at least 2 parameters. It will return null if all parameters are null.
#'
#' @param x Column to compute on
#' @param ... other columns
#'
#' @family non-aggregate functions
#' @rdname greatest
#' @name greatest
#' @aliases greatest,Column-method
#' @export
#' @examples \dontrun{greatest(df$c, df$d)}
#' @note greatest since 1.5.0
setMethod("greatest",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "greatest", jcols)
            column(jc)
          })

#' least
#'
#' Returns the least value of the list of column names, skipping null values.
#' This function takes at least 2 parameters. It will return null if all parameters are null.
#'
#' @param x Column to compute on
#' @param ... other columns
#'
#' @family non-aggregate functions
#' @rdname least
#' @aliases least,Column-method
#' @name least
#' @export
#' @examples \dontrun{least(df$c, df$d)}
#' @note least since 1.5.0
setMethod("least",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "least", jcols)
            column(jc)
          })

#' @rdname ceil
#'
#' @name ceiling
#' @aliases ceiling,Column-method
#' @export
#' @examples \dontrun{ceiling(df$c)}
#' @note ceiling since 1.5.0
setMethod("ceiling",
          signature(x = "Column"),
          function(x) {
            ceil(x)
          })

#' @rdname sign
#'
#' @name sign
#' @aliases sign,Column-method
#' @export
#' @examples \dontrun{sign(df$c)}
#' @note sign since 1.5.0
setMethod("sign", signature(x = "Column"),
          function(x) {
            signum(x)
          })

#' @details
#' \code{n_distinct}: Returns the number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases n_distinct n_distinct,Column-method
#' @export
#' @note n_distinct since 1.4.0
setMethod("n_distinct", signature(x = "Column"),
          function(x, ...) {
            countDistinct(x, ...)
          })

#' @rdname count
#' @name n
#' @aliases n,Column-method
#' @export
#' @examples \dontrun{n(df$c)}
#' @note n since 1.4.0
setMethod("n", signature(x = "Column"),
          function(x) {
            count(x)
          })

#' @details
#' \code{date_format}: Converts a date/timestamp/string to a value of string in the format
#' specified by the date format given by the second argument. A pattern could be for instance
#' \code{dd.MM.yyyy} and could return a string like '18.03.1993'. All
#' pattern letters of \code{java.text.SimpleDateFormat} can be used.
#' Note: Use when ever possible specialized functions like \code{year}. These benefit from a
#' specialized implementation.
#'
#' @rdname column_datetime_diff_functions
#'
#' @aliases date_format date_format,Column,character-method
#' @export
#' @note date_format since 1.5.0
setMethod("date_format", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_format", y@jc, x)
            column(jc)
          })

#' from_json
#'
#' Parses a column containing a JSON string into a Column of \code{structType} with the specified
#' \code{schema} or array of \code{structType} if \code{as.json.array} is set to \code{TRUE}.
#' If the string is unparseable, the Column will contains the value NA.
#'
#' @param x Column containing the JSON string.
#' @param schema a structType object to use as the schema to use when parsing the JSON string.
#' @param as.json.array indicating if input string is JSON array of objects or a single object.
#' @param ... additional named properties to control how the json is parsed, accepts the same
#'            options as the JSON data source.
#'
#' @family non-aggregate functions
#' @rdname from_json
#' @name from_json
#' @aliases from_json,Column,structType-method
#' @export
#' @examples
#' \dontrun{
#' schema <- structType(structField("name", "string"),
#' select(df, from_json(df$value, schema, dateFormat = "dd/MM/yyyy"))
#'}
#' @note from_json since 2.2.0
setMethod("from_json", signature(x = "Column", schema = "structType"),
          function(x, schema, as.json.array = FALSE, ...) {
            if (as.json.array) {
              jschema <- callJStatic("org.apache.spark.sql.types.DataTypes",
                                     "createArrayType",
                                     schema$jobj)
            } else {
              jschema <- schema$jobj
            }
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_json",
                              x@jc, jschema, options)
            column(jc)
          })

#' @details
#' \code{from_utc_timestamp}: Given a timestamp, which corresponds to a certain time of day in UTC,
#' returns another timestamp that corresponds to the same time of day in the given timezone.
#'
#' @rdname column_datetime_diff_functions
#'
#' @aliases from_utc_timestamp from_utc_timestamp,Column,character-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, from_utc = from_utc_timestamp(df$time, 'PST'),
#'                  to_utc = to_utc_timestamp(df$time, 'PST'))
#' head(tmp)}
#' @note from_utc_timestamp since 1.5.0
setMethod("from_utc_timestamp", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "from_utc_timestamp", y@jc, x)
            column(jc)
          })

#' @details
#' \code{instr}: Locates the position of the first occurrence of substr column
#' in the given string. Returns null if either of the arguments are null.
#' Note: The position is not zero based, but 1 based index. Returns 0 if substr
#' could not be found in str.
#'
#' @rdname column_string_functions
#' @aliases instr instr,Column,character-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, s1 = instr(df$Sex, "m"), s2 = instr(df$Sex, "M"),
#'                   s3 = locate("m", df$Sex), s4 = locate("m", df$Sex, pos = 4))
#' head(tmp)}
#' @note instr since 1.5.0
setMethod("instr", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "instr", y@jc, x)
            column(jc)
          })

#' @details
#' \code{next_day}: Given a date column, returns the first date which is later than the value of
#' the date column that is on the specified day of the week. For example,
#' \code{next_day('2015-07-27', "Sunday")} returns 2015-08-02 because that is the first Sunday
#' after 2015-07-27. Day of the week parameter is case insensitive, and accepts first three or
#' two characters: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
#'
#' @rdname column_datetime_diff_functions
#' @aliases next_day next_day,Column,character-method
#' @export
#' @note next_day since 1.5.0
setMethod("next_day", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "next_day", y@jc, x)
            column(jc)
          })

#' @details
#' \code{to_utc_timestamp}: Given a timestamp, which corresponds to a certain time of day
#' in the given timezone, returns another timestamp that corresponds to the same time of day in UTC.
#'
#' @rdname column_datetime_diff_functions
#' @aliases to_utc_timestamp to_utc_timestamp,Column,character-method
#' @export
#' @note to_utc_timestamp since 1.5.0
setMethod("to_utc_timestamp", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_utc_timestamp", y@jc, x)
            column(jc)
          })

#' @details
#' \code{add_months}: Returns the date that is numMonths (\code{x}) after startDate (\code{y}).
#'
#' @rdname column_datetime_diff_functions
#' @aliases add_months add_months,Column,numeric-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, t1 = add_months(df$time, 1),
#'                   t2 = date_add(df$time, 2),
#'                   t3 = date_sub(df$time, 3),
#'                   t4 = next_day(df$time, 'Sun'))
#' head(tmp)}
#' @note add_months since 1.5.0
setMethod("add_months", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "add_months", y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{date_add}: Returns the date that is \code{x} days after.
#'
#' @rdname column_datetime_diff_functions
#' @aliases date_add date_add,Column,numeric-method
#' @export
#' @note date_add since 1.5.0
setMethod("date_add", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_add", y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{date_sub}: Returns the date that is \code{x} days before.
#'
#' @rdname column_datetime_diff_functions
#'
#' @aliases date_sub date_sub,Column,numeric-method
#' @export
#' @note date_sub since 1.5.0
setMethod("date_sub", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_sub", y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{format_number}: Formats numeric column y to a format like '#,###,###.##',
#' rounded to x decimal places with HALF_EVEN round mode, and returns the result
#' as a string column.
#' If x is 0, the result has no decimal point or fractional part.
#' If x < 0, the result will be null.
#'
#' @rdname column_string_functions
#' @aliases format_number format_number,Column,numeric-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, v1 = df$Freq/3)
#' head(select(tmp, format_number(tmp$v1, 0), format_number(tmp$v1, 2)), 10)}
#' @note format_number since 1.5.0
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
#' @param y column to compute SHA-2 on.
#' @param x one of 224, 256, 384, or 512.
#' @family misc functions
#' @rdname sha2
#' @name sha2
#' @aliases sha2,Column,numeric-method
#' @export
#' @examples \dontrun{sha2(df$c, 256)}
#' @note sha2 since 1.5.0
setMethod("sha2", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sha2", y@jc, as.integer(x))
            column(jc)
          })

#' shiftLeft
#'
#' Shift the given value numBits left. If the given value is a long value, this function
#' will return a long value else it will return an integer value.
#'
#' @param y column to compute on.
#' @param x number of bits to shift.
#'
#' @family math functions
#' @rdname shiftLeft
#' @name shiftLeft
#' @aliases shiftLeft,Column,numeric-method
#' @export
#' @examples \dontrun{shiftLeft(df$c, 1)}
#' @note shiftLeft since 1.5.0
setMethod("shiftLeft", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftLeft",
                              y@jc, as.integer(x))
            column(jc)
          })

#' shiftRight
#'
#' (Signed) shift the given value numBits right. If the given value is a long value, it will return
#' a long value else it will return an integer value.
#'
#' @param y column to compute on.
#' @param x number of bits to shift.
#'
#' @family math functions
#' @rdname shiftRight
#' @name shiftRight
#' @aliases shiftRight,Column,numeric-method
#' @export
#' @examples \dontrun{shiftRight(df$c, 1)}
#' @note shiftRight since 1.5.0
setMethod("shiftRight", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftRight",
                              y@jc, as.integer(x))
            column(jc)
          })

#' shiftRightUnsigned
#'
#' Unsigned shift the given value numBits right. If the given value is a long value,
#' it will return a long value else it will return an integer value.
#'
#' @param y column to compute on.
#' @param x number of bits to shift.
#'
#' @family math functions
#' @rdname shiftRightUnsigned
#' @name shiftRightUnsigned
#' @aliases shiftRightUnsigned,Column,numeric-method
#' @export
#' @examples \dontrun{shiftRightUnsigned(df$c, 1)}
#' @note shiftRightUnsigned since 1.5.0
setMethod("shiftRightUnsigned", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftRightUnsigned",
                              y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{concat_ws}: Concatenates multiple input string columns together into a single
#' string column, using the given separator.
#'
#' @param sep separator to use.
#' @rdname column_string_functions
#' @aliases concat_ws concat_ws,character,Column-method
#' @export
#' @note concat_ws since 1.5.0
setMethod("concat_ws", signature(sep = "character", x = "Column"),
          function(sep, x, ...) {
            jcols <- lapply(list(x, ...), function(x) { x@jc })
            jc <- callJStatic("org.apache.spark.sql.functions", "concat_ws", sep, jcols)
            column(jc)
          })

#' conv
#'
#' Convert a number in a string column from one base to another.
#'
#' @param x column to convert.
#' @param fromBase base to convert from.
#' @param toBase base to convert to.
#'
#' @family math functions
#' @rdname conv
#' @aliases conv,Column,numeric,numeric-method
#' @name conv
#' @export
#' @examples \dontrun{conv(df$n, 2, 16)}
#' @note conv since 1.5.0
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
#' SparkDataFrame.selectExpr
#'
#' @param x an expression character object to be parsed.
#' @family non-aggregate functions
#' @rdname expr
#' @aliases expr,character-method
#' @name expr
#' @export
#' @examples \dontrun{expr('length(name)')}
#' @note expr since 1.5.0
setMethod("expr", signature(x = "character"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "expr", x)
            column(jc)
          })

#' @details
#' \code{format_string}: Formats the arguments in printf-style and returns the result
#' as a string column.
#'
#' @param format a character object of format strings.
#' @rdname column_string_functions
#' @aliases format_string format_string,character,Column-method
#' @export
#' @note format_string since 1.5.0
setMethod("format_string", signature(format = "character", x = "Column"),
          function(format, x, ...) {
            jcols <- lapply(list(x, ...), function(arg) { arg@jc })
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "format_string",
                              format, jcols)
            column(jc)
          })

#' @details
#' \code{from_unixtime}: Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a
#' string representing the timestamp of that moment in the current system time zone in the JVM in the
#' given format. See \href{http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html}{
#' Customizing Formats} for available options.
#'
#' @rdname column_datetime_functions
#'
#' @aliases from_unixtime from_unixtime,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, to_unix = unix_timestamp(df$time),
#'                   to_unix2 = unix_timestamp(df$time, 'yyyy-MM-dd HH'),
#'                   from_unix = from_unixtime(unix_timestamp(df$time)),
#'                   from_unix2 = from_unixtime(unix_timestamp(df$time), 'yyyy-MM-dd HH:mm'))
#' head(tmp)}
#' @note from_unixtime since 1.5.0
setMethod("from_unixtime", signature(x = "Column"),
          function(x, format = "yyyy-MM-dd HH:mm:ss") {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_unixtime",
                              x@jc, format)
            column(jc)
          })

#' window
#'
#' Bucketize rows into one or more time windows given a timestamp specifying column. Window
#' starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
#' [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
#' the order of months are not supported.
#'
#' @param x a time Column. Must be of TimestampType.
#' @param windowDuration a string specifying the width of the window, e.g. '1 second',
#'                       '1 day 12 hours', '2 minutes'. Valid interval strings are 'week',
#'                       'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond'. Note that
#'                       the duration is a fixed length of time, and does not vary over time
#'                       according to a calendar. For example, '1 day' always means 86,400,000
#'                       milliseconds, not a calendar day.
#' @param slideDuration a string specifying the sliding interval of the window. Same format as
#'                      \code{windowDuration}. A new window will be generated every
#'                      \code{slideDuration}. Must be less than or equal to
#'                      the \code{windowDuration}. This duration is likewise absolute, and does not
#'                      vary according to a calendar.
#' @param startTime the offset with respect to 1970-01-01 00:00:00 UTC with which to start
#'                  window intervals. For example, in order to have hourly tumbling windows
#'                  that start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
#'                  \code{startTime} as \code{"15 minutes"}.
#' @param ... further arguments to be passed to or from other methods.
#' @return An output column of struct called 'window' by default with the nested columns 'start'
#'         and 'end'.
#' @family date time functions
#' @rdname window
#' @name window
#' @aliases window,Column-method
#' @export
#' @examples
#'\dontrun{
#'   # One minute windows every 15 seconds 10 seconds after the minute, e.g. 09:00:10-09:01:10,
#'   # 09:00:25-09:01:25, 09:00:40-09:01:40, ...
#'   window(df$time, "1 minute", "15 seconds", "10 seconds")
#'
#'   # One minute tumbling windows 15 seconds after the minute, e.g. 09:00:15-09:01:15,
#'    # 09:01:15-09:02:15...
#'   window(df$time, "1 minute", startTime = "15 seconds")
#'
#'   # Thirty-second windows every 10 seconds, e.g. 09:00:00-09:00:30, 09:00:10-09:00:40, ...
#'   window(df$time, "30 seconds", "10 seconds")
#'}
#' @note window since 2.0.0
setMethod("window", signature(x = "Column"),
          function(x, windowDuration, slideDuration = NULL, startTime = NULL) {
            stopifnot(is.character(windowDuration))
            if (!is.null(slideDuration) && !is.null(startTime)) {
              stopifnot(is.character(slideDuration) && is.character(startTime))
              jc <- callJStatic("org.apache.spark.sql.functions",
                                "window",
                                x@jc, windowDuration, slideDuration, startTime)
            } else if (!is.null(slideDuration)) {
              stopifnot(is.character(slideDuration))
              jc <- callJStatic("org.apache.spark.sql.functions",
                                "window",
                                x@jc, windowDuration, slideDuration)
            } else if (!is.null(startTime)) {
              stopifnot(is.character(startTime))
              jc <- callJStatic("org.apache.spark.sql.functions",
                                "window",
                                x@jc, windowDuration, windowDuration, startTime)
            } else {
              jc <- callJStatic("org.apache.spark.sql.functions",
                                "window",
                                x@jc, windowDuration)
            }
            column(jc)
          })

#' @details
#' \code{locate}: Locates the position of the first occurrence of substr.
#' Note: The position is not zero based, but 1 based index. Returns 0 if substr
#' could not be found in str.
#'
#' @param substr a character string to be matched.
#' @param str a Column where matches are sought for each entry.
#' @param pos start position of search.
#' @rdname column_string_functions
#' @aliases locate locate,character,Column-method
#' @export
#' @note locate since 1.5.0
setMethod("locate", signature(substr = "character", str = "Column"),
          function(substr, str, pos = 1) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "locate",
                              substr, str@jc, as.integer(pos))
            column(jc)
          })

#' @details
#' \code{lpad}: Left-padded with pad to a length of len.
#'
#' @param len maximum length of each output result.
#' @param pad a character string to be padded with.
#' @rdname column_string_functions
#' @aliases lpad lpad,Column,numeric,character-method
#' @export
#' @note lpad since 1.5.0
setMethod("lpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' rand
#'
#' Generate a random column with independent and identically distributed (i.i.d.) samples
#' from U[0.0, 1.0].
#'
#' @param seed a random seed. Can be missing.
#' @family non-aggregate functions
#' @rdname rand
#' @name rand
#' @aliases rand,missing-method
#' @export
#' @examples \dontrun{rand()}
#' @note rand since 1.5.0
setMethod("rand", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand")
            column(jc)
          })

#' @rdname rand
#' @name rand
#' @aliases rand,numeric-method
#' @export
#' @note rand(numeric) since 1.5.0
setMethod("rand", signature(seed = "numeric"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand", as.integer(seed))
            column(jc)
          })

#' randn
#'
#' Generate a column with independent and identically distributed (i.i.d.) samples from
#' the standard normal distribution.
#'
#' @param seed a random seed. Can be missing.
#' @family non-aggregate functions
#' @rdname randn
#' @name randn
#' @aliases randn,missing-method
#' @export
#' @examples \dontrun{randn()}
#' @note randn since 1.5.0
setMethod("randn", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "randn")
            column(jc)
          })

#' @rdname randn
#' @name randn
#' @aliases randn,numeric-method
#' @export
#' @note randn(numeric) since 1.5.0
setMethod("randn", signature(seed = "numeric"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "randn", as.integer(seed))
            column(jc)
          })

#' @details
#' \code{regexp_extract}: Extracts a specific \code{idx} group identified by a Java regex,
#' from the specified string column. If the regex did not match, or the specified group did
#' not match, an empty string is returned.
#'
#' @param pattern a regular expression.
#' @param idx a group index.
#' @rdname column_string_functions
#' @aliases regexp_extract regexp_extract,Column,character,numeric-method
#' @export
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, s1 = regexp_extract(df$Class, "(\\d+)\\w+", 1),
#'                   s2 = regexp_extract(df$Sex, "^(\\w)\\w+", 1),
#'                   s3 = regexp_replace(df$Class, "\\D+", ""),
#'                   s4 = substring_index(df$Sex, "a", 1),
#'                   s5 = substring_index(df$Sex, "a", -1),
#'                   s6 = translate(df$Sex, "ale", ""),
#'                   s7 = translate(df$Sex, "a", "-"))
#' head(tmp)}
#' @note regexp_extract since 1.5.0
setMethod("regexp_extract",
          signature(x = "Column", pattern = "character", idx = "numeric"),
          function(x, pattern, idx) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "regexp_extract",
                              x@jc, pattern, as.integer(idx))
            column(jc)
          })

#' @details
#' \code{regexp_replace}: Replaces all substrings of the specified string value that
#' match regexp with rep.
#'
#' @param replacement a character string that a matched \code{pattern} is replaced with.
#' @rdname column_string_functions
#' @aliases regexp_replace regexp_replace,Column,character,character-method
#' @export
#' @note regexp_replace since 1.5.0
setMethod("regexp_replace",
          signature(x = "Column", pattern = "character", replacement = "character"),
          function(x, pattern, replacement) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "regexp_replace",
                              x@jc, pattern, replacement)
            column(jc)
          })

#' @details
#' \code{rpad}: Right-padded with pad to a length of len.
#'
#' @rdname column_string_functions
#' @aliases rpad rpad,Column,numeric,character-method
#' @export
#' @note rpad since 1.5.0
setMethod("rpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "rpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' @details
#' \code{substring_index}: Returns the substring from string str before count occurrences of
#' the delimiter delim. If count is positive, everything the left of the final delimiter
#' (counting from left) is returned. If count is negative, every to the right of the final
#' delimiter (counting from the right) is returned. substring_index performs a case-sensitive
#' match when searching for delim.
#'
#' @param delim a delimiter string.
#' @param count number of occurrences of \code{delim} before the substring is returned.
#'              A positive number means counting from the left, while negative means
#'              counting from the right.
#' @rdname column_string_functions
#' @aliases substring_index substring_index,Column,character,numeric-method
#' @export
#' @note substring_index since 1.5.0
setMethod("substring_index",
          signature(x = "Column", delim = "character", count = "numeric"),
          function(x, delim, count) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "substring_index",
                              x@jc, delim, as.integer(count))
            column(jc)
          })

#' @details
#' \code{translate}: Translates any character in the src by a character in replaceString.
#' The characters in replaceString is corresponding to the characters in matchingString.
#' The translate will happen when any character in the string matching with the character
#' in the matchingString.
#'
#' @param matchingString a source string where each character will be translated.
#' @param replaceString a target string where each \code{matchingString} character will
#'                      be replaced by the character in \code{replaceString}
#'                      at the same location, if any.
#' @rdname column_string_functions
#' @aliases translate translate,Column,character,character-method
#' @export
#' @note translate since 1.5.0
setMethod("translate",
          signature(x = "Column", matchingString = "character", replaceString = "character"),
          function(x, matchingString, replaceString) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "translate", x@jc, matchingString, replaceString)
            column(jc)
          })

#' @details
#' \code{unix_timestamp}: Gets current Unix timestamp in seconds.
#'
#' @rdname column_datetime_functions
#' @aliases unix_timestamp unix_timestamp,missing,missing-method
#' @export
#' @note unix_timestamp since 1.5.0
setMethod("unix_timestamp", signature(x = "missing", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp")
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases unix_timestamp,Column,missing-method
#' @export
#' @note unix_timestamp(Column) since 1.5.0
setMethod("unix_timestamp", signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp", x@jc)
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases unix_timestamp,Column,character-method
#' @export
#' @note unix_timestamp(Column, character) since 1.5.0
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
#' @param condition the condition to test on. Must be a Column expression.
#' @param value result expression.
#' @family non-aggregate functions
#' @rdname when
#' @name when
#' @aliases when,Column-method
#' @seealso \link{ifelse}
#' @export
#' @examples \dontrun{when(df$age == 2, df$age + 1)}
#' @note when since 1.5.0
setMethod("when", signature(condition = "Column", value = "ANY"),
          function(condition, value) {
              condition <- condition@jc
              value <- if (class(value) == "Column") { value@jc } else { value }
              jc <- callJStatic("org.apache.spark.sql.functions", "when", condition, value)
              column(jc)
          })

#' ifelse
#'
#' Evaluates a list of conditions and returns \code{yes} if the conditions are satisfied.
#' Otherwise \code{no} is returned for unmatched conditions.
#'
#' @param test a Column expression that describes the condition.
#' @param yes return values for \code{TRUE} elements of test.
#' @param no return values for \code{FALSE} elements of test.
#' @family non-aggregate functions
#' @rdname ifelse
#' @name ifelse
#' @aliases ifelse,Column-method
#' @seealso \link{when}
#' @export
#' @examples
#' \dontrun{
#' ifelse(df$a > 1 & df$b > 2, 0, 1)
#' ifelse(df$a > 1, df$a, 1)
#' }
#' @note ifelse since 1.5.0
setMethod("ifelse",
          signature(test = "Column", yes = "ANY", no = "ANY"),
          function(test, yes, no) {
              test <- test@jc
              yes <- if (class(yes) == "Column") { yes@jc } else { yes }
              no <- if (class(no) == "Column") { no@jc } else { no }
              jc <- callJMethod(callJStatic("org.apache.spark.sql.functions",
                                            "when",
                                            test, yes),
                                "otherwise", no)
              column(jc)
          })

###################### Window functions######################

#' cume_dist
#'
#' Window function: returns the cumulative distribution of values within a window partition,
#' i.e. the fraction of rows that are below the current row.
#'
#'   N = total number of rows in the partition
#'   cume_dist(x) = number of values before (and including) x / N
#'
#' This is equivalent to the \code{CUME_DIST} function in SQL.
#'
#' @rdname cume_dist
#' @name cume_dist
#' @family window functions
#' @aliases cume_dist,missing-method
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'   out <- select(df, over(cume_dist(), ws), df$hp, df$am)
#' }
#' @note cume_dist since 1.6.0
setMethod("cume_dist",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "cume_dist")
            column(jc)
          })

#' dense_rank
#'
#' Window function: returns the rank of rows within a window partition, without any gaps.
#' The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
#' sequence when there are ties. That is, if you were ranking a competition using dense_rank
#' and had three people tie for second place, you would say that all three were in second
#' place and that the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as coming in fifth.
#'
#' This is equivalent to the \code{DENSE_RANK} function in SQL.
#'
#' @rdname dense_rank
#' @name dense_rank
#' @family window functions
#' @aliases dense_rank,missing-method
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'   out <- select(df, over(dense_rank(), ws), df$hp, df$am)
#' }
#' @note dense_rank since 1.6.0
setMethod("dense_rank",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "dense_rank")
            column(jc)
          })

#' lag
#'
#' Window function: returns the value that is \code{offset} rows before the current row, and
#' \code{defaultValue} if there is less than \code{offset} rows before the current row. For example,
#' an \code{offset} of one will return the previous row at any given point in the window partition.
#'
#' This is equivalent to the \code{LAG} function in SQL.
#'
#' @param x the column as a character string or a Column to compute on.
#' @param offset the number of rows back from the current row from which to obtain a value.
#'               If not specified, the default is 1.
#' @param defaultValue (optional) default to use when the offset row does not exist.
#' @param ... further arguments to be passed to or from other methods.
#' @rdname lag
#' @name lag
#' @aliases lag,characterOrColumn-method
#' @family window functions
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'
#'   # Partition by am (transmission) and order by hp (horsepower)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'
#'   # Lag mpg values by 1 row on the partition-and-ordered table
#'   out <- select(df, over(lag(df$mpg), ws), df$mpg, df$hp, df$am)
#' }
#' @note lag since 1.6.0
setMethod("lag",
          signature(x = "characterOrColumn"),
          function(x, offset = 1, defaultValue = NULL) {
            col <- if (class(x) == "Column") {
              x@jc
            } else {
              x
            }

            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lag", col, as.integer(offset), defaultValue)
            column(jc)
          })

#' lead
#'
#' Window function: returns the value that is \code{offset} rows after the current row, and
#' \code{defaultValue} if there is less than \code{offset} rows after the current row.
#' For example, an \code{offset} of one will return the next row at any given point
#' in the window partition.
#'
#' This is equivalent to the \code{LEAD} function in SQL.
#'
#' @param x the column as a character string or a Column to compute on.
#' @param offset the number of rows after the current row from which to obtain a value.
#'               If not specified, the default is 1.
#' @param defaultValue (optional) default to use when the offset row does not exist.
#'
#' @rdname lead
#' @name lead
#' @family window functions
#' @aliases lead,characterOrColumn,numeric-method
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'
#'   # Partition by am (transmission) and order by hp (horsepower)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'
#'   # Lead mpg values by 1 row on the partition-and-ordered table
#'   out <- select(df, over(lead(df$mpg), ws), df$mpg, df$hp, df$am)
#' }
#' @note lead since 1.6.0
setMethod("lead",
          signature(x = "characterOrColumn", offset = "numeric", defaultValue = "ANY"),
          function(x, offset = 1, defaultValue = NULL) {
            col <- if (class(x) == "Column") {
              x@jc
            } else {
              x
            }

            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lead", col, as.integer(offset), defaultValue)
            column(jc)
          })

#' ntile
#'
#' Window function: returns the ntile group id (from 1 to n inclusive) in an ordered window
#' partition. For example, if n is 4, the first quarter of the rows will get value 1, the second
#' quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
#'
#' This is equivalent to the \code{NTILE} function in SQL.
#'
#' @param x Number of ntile groups
#'
#' @rdname ntile
#' @name ntile
#' @aliases ntile,numeric-method
#' @family window functions
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'
#'   # Partition by am (transmission) and order by hp (horsepower)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'
#'   # Get ntile group id (1-4) for hp
#'   out <- select(df, over(ntile(4), ws), df$hp, df$am)
#' }
#' @note ntile since 1.6.0
setMethod("ntile",
          signature(x = "numeric"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ntile", as.integer(x))
            column(jc)
          })

#' percent_rank
#'
#' Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
#'
#' This is computed by:
#'
#'   (rank of row in its partition - 1) / (number of rows in the partition - 1)
#'
#' This is equivalent to the PERCENT_RANK function in SQL.
#'
#' @rdname percent_rank
#' @name percent_rank
#' @family window functions
#' @aliases percent_rank,missing-method
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'   out <- select(df, over(percent_rank(), ws), df$hp, df$am)
#' }
#' @note percent_rank since 1.6.0
setMethod("percent_rank",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "percent_rank")
            column(jc)
          })

#' rank
#'
#' Window function: returns the rank of rows within a window partition.
#'
#' The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
#' sequence when there are ties. That is, if you were ranking a competition using dense_rank
#' and had three people tie for second place, you would say that all three were in second
#' place and that the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as coming in fifth.
#'
#' This is equivalent to the RANK function in SQL.
#'
#' @rdname rank
#' @name rank
#' @family window functions
#' @aliases rank,missing-method
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'   out <- select(df, over(rank(), ws), df$hp, df$am)
#' }
#' @note rank since 1.6.0
setMethod("rank",
          signature(x = "missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "rank")
            column(jc)
          })

# Expose rank() in the R base package
#' @param x a numeric, complex, character or logical vector.
#' @param ... additional argument(s) passed to the method.
#' @name rank
#' @rdname rank
#' @aliases rank,ANY-method
#' @export
setMethod("rank",
          signature(x = "ANY"),
          function(x, ...) {
            base::rank(x, ...)
          })

#' row_number
#'
#' Window function: returns a sequential number starting at 1 within a window partition.
#'
#' This is equivalent to the ROW_NUMBER function in SQL.
#'
#' @rdname row_number
#' @name row_number
#' @aliases row_number,missing-method
#' @family window functions
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'   out <- select(df, over(row_number(), ws), df$hp, df$am)
#' }
#' @note row_number since 1.6.0
setMethod("row_number",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "row_number")
            column(jc)
          })

###################### Collection functions######################

#' array_contains
#'
#' Returns null if the array is null, true if the array contains the value, and false otherwise.
#'
#' @param x A Column
#' @param value A value to be checked if contained in the column
#' @rdname array_contains
#' @aliases array_contains,Column-method
#' @name array_contains
#' @family collection functions
#' @export
#' @examples \dontrun{array_contains(df$c, 1)}
#' @note array_contains since 1.6.0
setMethod("array_contains",
          signature(x = "Column", value = "ANY"),
          function(x, value) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_contains", x@jc, value)
            column(jc)
          })

#' explode
#'
#' Creates a new row for each element in the given array or map column.
#'
#' @param x Column to compute on
#'
#' @rdname explode
#' @name explode
#' @family collection functions
#' @aliases explode,Column-method
#' @export
#' @examples \dontrun{explode(df$c)}
#' @note explode since 1.5.0
setMethod("explode",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "explode", x@jc)
            column(jc)
          })

#' size
#'
#' Returns length of array or map.
#'
#' @param x Column to compute on
#'
#' @rdname size
#' @name size
#' @aliases size,Column-method
#' @family collection functions
#' @export
#' @examples \dontrun{size(df$c)}
#' @note size since 1.5.0
setMethod("size",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "size", x@jc)
            column(jc)
          })

#' sort_array
#'
#' Sorts the input array in ascending or descending order according
#' to the natural ordering of the array elements.
#'
#' @param x A Column to sort
#' @param asc A logical flag indicating the sorting order.
#'            TRUE, sorting is in ascending order.
#'            FALSE, sorting is in descending order.
#' @rdname sort_array
#' @name sort_array
#' @aliases sort_array,Column-method
#' @family collection functions
#' @export
#' @examples
#' \dontrun{
#' sort_array(df$c)
#' sort_array(df$c, FALSE)
#' }
#' @note sort_array since 1.6.0
setMethod("sort_array",
          signature(x = "Column"),
          function(x, asc = TRUE) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sort_array", x@jc, asc)
            column(jc)
          })

#' posexplode
#'
#' Creates a new row for each element with position in the given array or map column.
#'
#' @param x Column to compute on
#'
#' @rdname posexplode
#' @name posexplode
#' @family collection functions
#' @aliases posexplode,Column-method
#' @export
#' @examples \dontrun{posexplode(df$c)}
#' @note posexplode since 2.1.0
setMethod("posexplode",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "posexplode", x@jc)
            column(jc)
          })

#' create_array
#'
#' Creates a new array column. The input columns must all have the same data type.
#'
#' @param x Column to compute on
#' @param ... additional Column(s).
#'
#' @family non-aggregate functions
#' @rdname create_array
#' @name create_array
#' @aliases create_array,Column-method
#' @export
#' @examples \dontrun{create_array(df$x, df$y, df$z)}
#' @note create_array since 2.3.0
setMethod("create_array",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "array", jcols)
            column(jc)
          })

#' create_map
#'
#' Creates a new map column. The input columns must be grouped as key-value pairs,
#' e.g. (key1, value1, key2, value2, ...).
#' The key columns must all have the same data type, and can't be null.
#' The value columns must all have the same data type.
#'
#' @param x Column to compute on
#' @param ... additional Column(s).
#'
#' @family non-aggregate functions
#' @rdname create_map
#' @name create_map
#' @aliases create_map,Column-method
#' @export
#' @examples \dontrun{create_map(lit("x"), lit(1.0), lit("y"), lit(-1.0))}
#' @note create_map since 2.3.0
setMethod("create_map",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "map", jcols)
            column(jc)
          })

#' @details
#' \code{collect_list}: Creates a list of objects with duplicates.
#'
#' @rdname column_aggregate_functions
#' @aliases collect_list collect_list,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' df2 = df[df$mpg > 20, ]
#' collect(select(df2, collect_list(df2$gear)))
#' collect(select(df2, collect_set(df2$gear)))}
#' @note collect_list since 2.3.0
setMethod("collect_list",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "collect_list", x@jc)
            column(jc)
          })

#' @details
#' \code{collect_set}: Creates a list of objects with duplicate elements eliminated.
#'
#' @rdname column_aggregate_functions
#' @aliases collect_set collect_set,Column-method
#' @export
#' @note collect_set since 2.3.0
setMethod("collect_set",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "collect_set", x@jc)
            column(jc)
          })

#' @details
#' \code{split_string}: Splits string on regular expression.
#' Equivalent to \code{split} SQL function.
#'
#' @rdname column_string_functions
#' @aliases split_string split_string,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, split_string(df$Sex, "a")))
#' head(select(df, split_string(df$Class, "\\d")))
#' # This is equivalent to the following SQL expression
#' head(selectExpr(df, "split(Class, '\\\\d')"))}
#' @note split_string 2.3.0
setMethod("split_string",
          signature(x = "Column", pattern = "character"),
          function(x, pattern) {
            jc <- callJStatic("org.apache.spark.sql.functions", "split", x@jc, pattern)
            column(jc)
          })

#' @details
#' \code{repeat_string}: Repeats string n times.
#' Equivalent to \code{repeat} SQL function.
#'
#' @param n Number of repetitions
#' @rdname column_string_functions
#' @aliases repeat_string repeat_string,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, repeat_string(df$Class, 3)))
#' # This is equivalent to the following SQL expression
#' head(selectExpr(df, "repeat(Class, 3)"))}
#' @note repeat_string since 2.3.0
setMethod("repeat_string",
          signature(x = "Column", n = "numeric"),
          function(x, n) {
            jc <- callJStatic("org.apache.spark.sql.functions", "repeat", x@jc, numToInt(n))
            column(jc)
          })

#' explode_outer
#'
#' Creates a new row for each element in the given array or map column.
#' Unlike \code{explode}, if the array/map is \code{null} or empty
#' then \code{null} is produced.
#'
#' @param x Column to compute on
#'
#' @rdname explode_outer
#' @name explode_outer
#' @family collection functions
#' @aliases explode_outer,Column-method
#' @export
#' @examples
#' \dontrun{
#' df <- createDataFrame(data.frame(
#'   id = c(1, 2, 3), text = c("a,b,c", NA, "d,e")
#' ))
#'
#' head(select(df, df$id, explode_outer(split_string(df$text, ","))))
#' }
#' @note explode_outer since 2.3.0
setMethod("explode_outer",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "explode_outer", x@jc)
            column(jc)
          })

#' posexplode_outer
#'
#' Creates a new row for each element with position in the given array or map column.
#' Unlike \code{posexplode}, if the array/map is \code{null} or empty
#' then the row (\code{null}, \code{null}) is produced.
#'
#' @param x Column to compute on
#'
#' @rdname posexplode_outer
#' @name posexplode_outer
#' @family collection functions
#' @aliases posexplode_outer,Column-method
#' @export
#' @examples
#' \dontrun{
#' df <- createDataFrame(data.frame(
#'   id = c(1, 2, 3), text = c("a,b,c", NA, "d,e")
#' ))
#'
#' head(select(df, df$id, posexplode_outer(split_string(df$text, ","))))
#' }
#' @note posexplode_outer since 2.3.0
setMethod("posexplode_outer",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "posexplode_outer", x@jc)
            column(jc)
          })

#' not
#'
#' Inversion of boolean expression.
#'
#' \code{not} and \code{!} cannot be applied directly to numerical column.
#' To achieve R-like truthiness column has to be casted to \code{BooleanType}.
#'
#' @param x Column to compute on
#' @rdname not
#' @name not
#' @aliases not,Column-method
#' @family non-aggregate functions
#' @export
#' @examples
#' \dontrun{
#' df <- createDataFrame(data.frame(
#'   is_true = c(TRUE, FALSE, NA),
#'   flag = c(1, 0,  1)
#' ))
#'
#' head(select(df, not(df$is_true)))
#'
#' # Explicit cast is required when working with numeric column
#' head(select(df, not(cast(df$flag, "boolean"))))
#' }
#' @note not since 2.3.0
setMethod("not",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "not", x@jc)
            column(jc)
          })

#' @details
#' \code{grouping_bit}: Indicates whether a specified column in a GROUP BY list is aggregated or not,
#' returns 1 for aggregated or 0 for not aggregated in the result set. Same as \code{GROUPING} in SQL
#' and \code{grouping} function in Scala.
#'
#' @rdname column_aggregate_functions
#' @aliases grouping_bit grouping_bit,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' # With cube
#' agg(
#'   cube(df, "cyl", "gear", "am"),
#'   mean(df$mpg),
#'   grouping_bit(df$cyl), grouping_bit(df$gear), grouping_bit(df$am)
#' )
#'
#' # With rollup
#' agg(
#'   rollup(df, "cyl", "gear", "am"),
#'   mean(df$mpg),
#'   grouping_bit(df$cyl), grouping_bit(df$gear), grouping_bit(df$am)
#' )}
#' @note grouping_bit since 2.3.0
setMethod("grouping_bit",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "grouping", x@jc)
            column(jc)
          })

#' @details
#' \code{grouping_id}: Returns the level of grouping.
#' Equals to \code{
#' grouping_bit(c1) * 2^(n - 1) + grouping_bit(c2) * 2^(n - 2)  + ... + grouping_bit(cn)
#' }
#'
#' @rdname column_aggregate_functions
#' @aliases grouping_id grouping_id,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' # With cube
#' agg(
#'   cube(df, "cyl", "gear", "am"),
#'   mean(df$mpg),
#'   grouping_id(df$cyl, df$gear, df$am)
#' )
#'
#' # With rollup
#' agg(
#'   rollup(df, "cyl", "gear", "am"),
#'   mean(df$mpg),
#'   grouping_id(df$cyl, df$gear, df$am)
#' )}
#' @note grouping_id since 2.3.0
setMethod("grouping_id",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function (x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "grouping_id", jcols)
            column(jc)
          })

#' input_file_name
#'
#' Creates a string column with the input file name for a given row
#'
#' @rdname input_file_name
#' @name input_file_name
#' @family non-aggregate functions
#' @aliases input_file_name,missing-method
#' @export
#' @examples
#' \dontrun{
#' df <- read.text("README.md")
#'
#' head(select(df, input_file_name()))
#' }
#' @note input_file_name since 2.3.0
setMethod("input_file_name", signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "input_file_name")
            column(jc)
          })

#' @details
#' \code{trunc}: Returns date truncated to the unit specified by the format.
#'
#' @rdname column_datetime_functions
#' @aliases trunc trunc,Column-method
#' @export
#' @examples
#'
#' \dontrun{
#' head(select(df, df$time, trunc(df$time, "year"), trunc(df$time, "yy"),
#'            trunc(df$time, "month"), trunc(df$time, "mon")))}
#' @note trunc since 2.3.0
setMethod("trunc",
          signature(x = "Column"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "trunc",
                              x@jc, as.character(format))
            column(jc)
          })

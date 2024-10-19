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
#' @param x Column to compute on. In \code{window}, it must be a time Column of
#'          \code{TimestampType}. This is not used with \code{current_date} and
#'          \code{current_timestamp}
#' @param y Column to compute on.
#' @param z Column to compute on.
#' @param format The format for the given dates or timestamps in Column \code{x}. See the
#'               format used in the following methods:
#'               \itemize{
#'               \item \code{to_date} and \code{to_timestamp}: it is the string to use to parse
#'                    Column \code{x} to DateType or TimestampType.
#'               \item \code{trunc}: it is the string to use to specify the truncation method.
#'                    'year', 'yyyy', 'yy' to truncate by year,
#'                    or 'month', 'mon', 'mm' to truncate by month
#'                    Other options are: 'week', 'quarter'
#'               \item \code{date_trunc}: it is similar with \code{trunc}'s but additionally
#'                    supports
#'                    'day', 'dd' to truncate by day,
#'                    'microsecond', 'millisecond', 'second', 'minute' and 'hour'
#'               }
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
#'          \item \code{from_utc_timestamp}, \code{to_utc_timestamp}: A string detailing
#'            the time zone ID that the input should be adjusted to. It should be in the format
#'            of either region-based zone IDs or zone offsets. Region IDs must have the form
#'            'area/city', such as 'America/Los_Angeles'. Zone offsets must be in the format
#'            (+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are supported
#'            as aliases of '+00:00'. Other short names are not recommended to use
#'            because they can be ambiguous.
#'          \item \code{next_day}: day of the week string.
#'          }
#' @param ... additional argument(s).
#'          \itemize{
#'          \item \code{months_between}, this contains an optional parameter to specify the
#'              the result is rounded off to 8 digits.
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

#' Math functions for Column operations
#'
#' Math functions defined for \code{Column}.
#'
#' @param x Column to compute on. In \code{shiftLeft}, \code{shiftRight} and
#'          \code{shiftRightUnsigned}, this is the number of bits to shift.
#' @param y Column to compute on.
#' @param ... additional argument(s).
#' @name column_math_functions
#' @rdname column_math_functions
#' @family math functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' tmp <- mutate(df, v1 = log(df$mpg), v2 = cbrt(df$disp),
#'                   v3 = bround(df$wt, 1), v4 = bin(df$cyl),
#'                   v5 = hex(df$wt), v6 = degrees(df$gear),
#'                   v7 = atan2(df$cyl, df$am), v8 = hypot(df$cyl, df$am),
#'                   v9 = pmod(df$hp, df$cyl), v10 = shiftLeft(df$disp, 1),
#'                   v11 = conv(df$hp, 10, 16), v12 = sign(df$vs - 0.5),
#'                   v13 = sqrt(df$disp), v14 = ceil(df$wt))
#' head(tmp)}
NULL

#' String functions for Column operations
#'
#' String functions defined for \code{Column}.
#'
#' @param x Column to compute on except in the following methods:
#'      \itemize{
#'      \item \code{instr}: \code{character}, the substring to check. See 'Details'.
#'      \item \code{format_number}: \code{numeric}, the number of decimal place to
#'           format to. See 'Details'.
#'      }
#' @param y Column to compute on.
#' @param pos In \itemize{
#'                \item \code{locate}: a start position of search.
#'                \item \code{overlay}: a start position for replacement.
#'                }
#' @param len In \itemize{
#'               \item \code{lpad} the maximum length of each output result.
#'               \item \code{overlay} a number of bytes to replace.
#'               }
#' @param ... additional Columns.
#' @name column_string_functions
#' @rdname column_string_functions
#' @family string functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(as.data.frame(Titanic, stringsAsFactors = FALSE))}
NULL

#' Non-aggregate functions for Column operations
#'
#' Non-aggregate functions defined for \code{Column}.
#'
#' @param x Column to compute on. In \code{lit}, it is a literal value or a Column.
#'          In \code{expr}, it contains an expression character object to be parsed.
#' @param y Column to compute on.
#' @param ... additional Columns.
#' @name column_nonaggregate_functions
#' @rdname column_nonaggregate_functions
#' @seealso coalesce,SparkDataFrame-method
#' @family non-aggregate functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))}
NULL

#' Miscellaneous functions for Column operations
#'
#' Miscellaneous functions defined for \code{Column}.
#'
#' @param x Column to compute on. In \code{sha2}, it is one of 224, 256, 384, or 512.
#' @param y Column to compute on.
#' @param ... additional Columns.
#' @name column_misc_functions
#' @rdname column_misc_functions
#' @family misc functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars)[, 1:2])
#' tmp <- mutate(df, v1 = crc32(df$model), v2 = hash(df$model),
#'                   v3 = hash(df$model, df$mpg), v4 = md5(df$model),
#'                   v5 = sha1(df$model), v6 = sha2(df$model, 256))
#' head(tmp)}
NULL

#' Collection functions for Column operations
#'
#' Collection functions defined for \code{Column}.
#'
#' @param x Column to compute on. Note the difference in the following methods:
#'          \itemize{
#'          \item \code{to_json}: it is the column containing the struct, array of the structs,
#'              the map or array of maps.
#'          \item \code{to_csv}: it is the column containing the struct.
#'          \item \code{from_json}: it is the column containing the JSON string.
#'          \item \code{from_csv}: it is the column containing the CSV string.
#'          }
#' @param y Column to compute on.
#' @param value A value to compute on.
#'          \itemize{
#'          \item \code{array_contains}: a value to be checked if contained in the column.
#'          \item \code{array_position}: a value to locate in the given array.
#'          \item \code{array_remove}: a value to remove in the given array.
#'          }
#' @param schema
#'          \itemize{
#'          \item \code{from_json}: a structType object to use as the schema to use
#'              when parsing the JSON string. Since Spark 2.3, the DDL-formatted string is
#'              also supported for the schema. Since Spark 3.0, \code{schema_of_json} or
#'              the DDL-formatted string literal can also be accepted.
#'          \item \code{from_csv}: a structType object, DDL-formatted string or \code{schema_of_csv}
#'          }
#'
#' @param f a \code{function} mapping from \code{Column(s)} to \code{Column}.
#'          \itemize{
#'          \item \code{array_exists}
#'          \item \code{array_filter} the Boolean \code{function} used to filter the data.
#'            Either unary or binary. In the latter case the second argument
#'            is the index in the array (0-based).
#'          \item \code{array_forall} the Boolean unary \code{function} used to filter the data.
#'          \item \code{array_transform} a \code{function} used to transform the data.
#'            Either unary or binary. In the latter case the second argument
#'            is the index in the array (0-based).
#'          \item \code{arrays_zip_with}
#'          \item \code{map_zip_with}
#'          \item \code{map_filter} the Boolean binary \code{function} used to filter the data.
#'            The first argument is the key, the second argument is the value.
#'          \item \code{transform_keys} a binary \code{function}
#'            used to transform the data.  The first argument is the key, the second argument
#'            is the value.
#'          \item \code{transform_values} a binary \code{function}
#'            used to transform the data.  The first argument is the key, the second argument
#'            is the value.
#'          }
#' @param initialValue a \code{Column} used as the initial value in \code{array_aggregate}
#' @param merge a \code{function} a binary function \code{(Column, Column) -> Column}
#'          used in \code{array_aggregate}to merge values (the second argument)
#'          into accumulator (the first argument).
#' @param finish an unary \code{function} \code{(Column) -> Column} used to
#'          apply final transformation on the accumulated data in \code{array_aggregate}.
#' @param comparator an optional binary (\code{(Column, Column) -> Column}) \code{function}
#'          which is used to compare the elements of the array.
#'          The comparator will take two
#'          arguments representing two elements of the array. It returns a negative integer,
#'          0, or a positive integer as the first element is less than, equal to,
#'          or greater than the second element.
#'          If the comparator function returns null, the function will fail and raise an error.
#' @param ... additional argument(s).
#'          \itemize{
#'          \item \code{to_json}, \code{from_json} and \code{schema_of_json}: this contains
#'              additional named properties to control how it is converted and accepts the
#'              same options as the JSON data source.
#'              You can find the JSON-specific options for reading/writing JSON files in
# nolint start
#'              \url{https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option}{Data Source Option}
# nolint end
#'              in the version you use.
#'          \item \code{to_json}: it supports the "pretty" option which enables pretty
#'              JSON generation.
#'          \item \code{to_csv}, \code{from_csv} and \code{schema_of_csv}: this contains
#'              additional named properties to control how it is converted and accepts the
#'              same options as the CSV data source.
#'              You can find the CSV-specific options for reading/writing CSV files in
# nolint start
#'              \url{https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option}{Data Source Option}
# nolint end
#'              in the version you use.
#'          \item \code{arrays_zip}, this contains additional Columns of arrays to be merged.
#'          \item \code{map_concat}, this contains additional Columns of maps to be unioned.
#'          }
#' @name column_collection_functions
#' @rdname column_collection_functions
#' @family collection functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' tmp <- mutate(df, v1 = create_array(df$mpg, df$cyl, df$hp))
#' head(select(tmp, array_contains(tmp$v1, 21), size(tmp$v1), shuffle(tmp$v1)))
#' head(select(tmp, array_max(tmp$v1), array_min(tmp$v1), array_distinct(tmp$v1)))
#' head(select(tmp, array_position(tmp$v1, 21), array_repeat(df$mpg, 3), array_sort(tmp$v1)))
#' head(select(tmp, array_sort(tmp$v1, function(x, y) coalesce(cast(y - x, "integer"), lit(0L)))))
#' head(select(tmp, reverse(tmp$v1), array_remove(tmp$v1, 21)))
#' head(select(tmp, array_transform("v1", function(x) x * 10)))
#' head(select(tmp, array_exists("v1", function(x) x > 120)))
#' head(select(tmp, array_forall("v1", function(x) x >= 8.0)))
#' head(select(tmp, array_filter("v1", function(x) x < 10)))
#' head(select(tmp, array_aggregate("v1", lit(0), function(acc, y) acc + y)))
#' head(select(
#'   tmp,
#'   array_aggregate("v1", lit(0), function(acc, y) acc + y, function(acc) acc / 10)))
#' tmp2 <- mutate(tmp, v2 = explode(tmp$v1))
#' head(tmp2)
#' head(select(tmp, posexplode(tmp$v1)))
#' head(select(tmp, slice(tmp$v1, 2L, 2L)))
#' head(select(tmp, sort_array(tmp$v1)))
#' head(select(tmp, sort_array(tmp$v1, asc = FALSE)))
#' tmp3 <- mutate(df, v3 = create_map(df$model, df$cyl))
#' head(select(tmp3, map_entries(tmp3$v3), map_keys(tmp3$v3), map_values(tmp3$v3)))
#' head(select(tmp3, element_at(tmp3$v3, "Valiant"), map_concat(tmp3$v3, tmp3$v3)))
#' head(select(tmp3, transform_keys("v3", function(k, v) upper(k))))
#' head(select(tmp3, transform_values("v3", function(k, v) v * 10)))
#' head(select(tmp3, map_filter("v3", function(k, v) v < 42)))
#' tmp4 <- mutate(df, v4 = create_array(df$mpg, df$cyl), v5 = create_array(df$cyl, df$hp))
#' head(select(tmp4, concat(tmp4$v4, tmp4$v5), arrays_overlap(tmp4$v4, tmp4$v5)))
#' head(select(tmp4, array_except(tmp4$v4, tmp4$v5), array_intersect(tmp4$v4, tmp4$v5)))
#' head(select(tmp4, array_union(tmp4$v4, tmp4$v5)))
#' head(select(tmp4, arrays_zip(tmp4$v4, tmp4$v5)))
#' head(select(tmp, concat(df$mpg, df$cyl, df$hp)))
#' head(select(tmp4, arrays_zip_with(tmp4$v4, tmp4$v5, function(x, y) x * y)))
#' tmp5 <- mutate(df, v6 = create_array(df$model, df$model))
#' head(select(tmp5, array_join(tmp5$v6, "#"), array_join(tmp5$v6, "#", "NULL")))
#' tmp6 <- mutate(df, v7 = create_array(create_array(df$model, df$model)))
#' head(select(tmp6, flatten(tmp6$v7)))
#' tmp7 <- mutate(df, v8 = create_array(df$model, df$cyl), v9 = create_array(df$model, df$hp))
#' head(select(tmp7, arrays_zip_with("v8", "v9", function(x, y) (x * y) %% 3)))
#' head(select(tmp7, map_from_arrays(tmp7$v8, tmp7$v9)))
#' tmp8 <- mutate(df, v10 = create_array(struct(df$model, df$cyl)))
#' head(select(tmp8, map_from_entries(tmp8$v10)))}
NULL

#' Window functions for Column operations
#'
#' Window functions defined for \code{Column}.
#'
#' @param x In \code{lag} and \code{lead}, it is the column as a character string or a Column
#'          to compute on. In \code{ntile}, it is the number of ntile groups.
#' @param offset In \code{lag}, the number of rows back from the current row from which to obtain
#'               a value. In \code{lead}, the number of rows after the current row from which to
#'               obtain a value. If not specified, the default is 1.
#' @param defaultValue (optional) default to use when the offset row does not exist.
#' @param ... additional argument(s).
#' @name column_window_functions
#' @rdname column_window_functions
#' @family window functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' ws <- orderBy(windowPartitionBy("am"), "hp")
#' tmp <- mutate(df, dist = over(cume_dist(), ws), dense_rank = over(dense_rank(), ws),
#'               lag = over(lag(df$mpg), ws), lead = over(lead(df$mpg, 1), ws),
#'               percent_rank = over(percent_rank(), ws),
#'               rank = over(rank(), ws), row_number = over(row_number(), ws),
#'               nth_value = over(nth_value(df$mpg, 3), ws))
#' # Get ntile group id (1-4) for hp
#' tmp <- mutate(tmp, ntile = over(ntile(4), ws))
#' head(tmp)}
NULL

#' ML functions for Column operations
#'
#' ML functions defined for \code{Column}.
#'
#' @param x Column to compute on.
#' @param ... additional argument(s).
#' @name column_ml_functions
#' @rdname column_ml_functions
#' @family ml functions
#' @examples
#' \dontrun{
#' df <- read.df("data/mllib/sample_libsvm_data.txt", source = "libsvm")
#' head(
#'   withColumn(
#'     withColumn(df, "array", vector_to_array(df$features)),
#'     "vector",
#'     array_to_vector(column("array"))
#'   )
#' )
#' }
NULL

#' Avro processing functions for Column operations
#'
#' Avro processing functions defined for \code{Column}.
#'
#' @param x Column to compute on.
#' @param jsonFormatSchema character Avro schema in JSON string format
#' @param ... additional argument(s) passed as parser options.
#' @name column_avro_functions
#' @rdname column_avro_functions
#' @family avro functions
#' @note Avro is built-in but external data source module since Spark 2.4.
#'   Please deploy the application as per
#'   \href{https://spark.apache.org/docs/latest/sql-data-sources-avro.html#deploying}{
#'     the deployment section
#'   } of "Apache Avro Data Source Guide".
#' @examples
#' \dontrun{
#' df <- createDataFrame(iris)
#' schema <- paste(
#'   c(
#'     '{"type": "record", "namespace": "example.avro", "name": "Iris", "fields": [',
#'     '{"type": ["double", "null"], "name": "Sepal_Length"},',
#'     '{"type": ["double", "null"], "name": "Sepal_Width"},',
#'     '{"type": ["double", "null"], "name": "Petal_Length"},',
#'     '{"type": ["double", "null"], "name": "Petal_Width"},',
#'     '{"type": ["string", "null"], "name": "Species"}]}'
#'   ),
#'   collapse="\\n"
#' )
#'
#' df_serialized <- select(
#'   df,
#'   alias(to_avro(alias(struct(column("*")), "fields")), "payload")
#' )
#'
#' df_deserialized <- select(
#'   df_serialized,
#'   from_avro(df_serialized$payload, schema)
#' )
#'
#' head(df_deserialized)
#' }
NULL

#' @details
#' \code{lit}: A new Column is created to represent the literal value.
#' If the parameter is a Column, it is returned unchanged.
#'
#' @rdname column_nonaggregate_functions
#' @aliases lit lit,ANY-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, v1 = lit(df$mpg), v2 = lit("x"), v3 = lit("2015-01-01"),
#'                   v4 = negate(df$mpg), v5 = expr('length(model)'),
#'                   v6 = greatest(df$vs, df$am), v7 = least(df$vs, df$am),
#'                   v8 = column("mpg"))
#' head(tmp)}
#' @note lit since 1.5.0
setMethod("lit", signature("ANY"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lit",
                              if (inherits(x, "Column")) { x@jc } else { x })
            column(jc)
          })

#' @details
#' \code{abs}: Computes the absolute value.
#'
#' @rdname column_math_functions
#' @aliases abs abs,Column-method
#' @note abs since 1.5.0
setMethod("abs",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "abs", x@jc)
            column(jc)
          })

#' @details
#' \code{acos}: Returns the inverse cosine of the given value,
#' as if computed by \code{java.lang.Math.acos()}
#'
#' @rdname column_math_functions
#' @aliases acos acos,Column-method
#' @note acos since 1.5.0
setMethod("acos",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "acos", x@jc)
            column(jc)
          })

#' @details
#' \code{acosh}: Computes inverse hyperbolic cosine of the input column.
#'
#' @rdname column_math_functions
#' @aliases acosh acosh,Column-method
#' @note acosh since 3.1.0
setMethod("acosh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "acosh", x@jc)
            column(jc)
          })

#' @details
#' \code{approx_count_distinct}: Returns the approximate number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases approx_count_distinct approx_count_distinct,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, approx_count_distinct(df$gear)))
#' head(select(df, approx_count_distinct(df$gear, 0.02)))
#' head(select(df, count_distinct(df$gear, df$cyl)))
#' head(select(df, n_distinct(df$gear)))
#' head(distinct(select(df, "gear")))}
#' @note approx_count_distinct(Column) since 3.0.0
setMethod("approx_count_distinct",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approx_count_distinct", x@jc)
            column(jc)
          })

#' @details
#' \code{approxCountDistinct}: Returns the approximate number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases approxCountDistinct approxCountDistinct,Column-method
#' @note approxCountDistinct(Column) since 1.4.0
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x) {
            .Deprecated("approx_count_distinct")
            jc <- callJStatic("org.apache.spark.sql.functions", "approx_count_distinct", x@jc)
            column(jc)
          })

#' @details
#' \code{ascii}: Computes the numeric value of the first character of the string column,
#' and returns the result as an int column.
#'
#' @rdname column_string_functions
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

#' @details
#' \code{asin}: Returns the inverse sine of the given value,
#' as if computed by \code{java.lang.Math.asin()}
#'
#' @rdname column_math_functions
#' @aliases asin asin,Column-method
#' @note asin since 1.5.0
setMethod("asin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "asin", x@jc)
            column(jc)
          })

#' @details
#' \code{asinh}: Computes inverse hyperbolic sine of the input column.
#'
#' @rdname column_math_functions
#' @aliases asinh asinh,Column-method
#' @note asinh since 3.1.0
setMethod("asinh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "asinh", x@jc)
            column(jc)
          })

#' @details
#' \code{atan}: Returns the inverse tangent of the given value,
#' as if computed by \code{java.lang.Math.atan()}
#'
#' @rdname column_math_functions
#' @aliases atan atan,Column-method
#' @note atan since 1.5.0
setMethod("atan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "atan", x@jc)
            column(jc)
          })

#' @details
#' \code{atanh}: Computes inverse hyperbolic tangent of the input column.
#'
#' @rdname column_math_functions
#' @aliases atanh atanh,Column-method
#' @note atanh since 3.1.0
setMethod("atanh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "atanh", x@jc)
            column(jc)
          })

#' avg
#'
#' Aggregate function: returns the average of the values in a group.
#'
#' @rdname avg
#' @name avg
#' @family aggregate functions
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
#' @aliases base64 base64,Column-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, s1 = encode(df$Class, "UTF-8"))
#' str(tmp)
#' tmp2 <- mutate(tmp, s2 = base64(tmp$s1), s3 = decode(tmp$s1, "UTF-8"),
#'                     s4 = soundex(tmp$Sex))
#' head(tmp2)
#' head(select(tmp2, unbase64(tmp2$s2)))}
#' @note base64 since 1.5.0
setMethod("base64",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "base64", x@jc)
            column(jc)
          })

#' @details
#' \code{bin}: Returns the string representation of the binary value
#' of the given long column. For example, bin("12") returns "1100".
#'
#' @rdname column_math_functions
#' @aliases bin bin,Column-method
#' @note bin since 1.5.0
setMethod("bin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bin", x@jc)
            column(jc)
          })

#' @details
#' \code{bit_length}: Calculates the bit length for the specified string column.
#'
#' @rdname column_string_functions
#' @aliases bit_length bit_length,Column-method
#' @note length since 3.3.0
setMethod("bit_length",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bit_length", x@jc)
            column(jc)
          })

#' @details
#' \code{bitwise_not}: Computes bitwise NOT.
#'
#' @rdname column_nonaggregate_functions
#' @aliases bitwise_not bitwise_not,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, bitwise_not(cast(df$vs, "int"))))}
#' @note bitwise_not since 3.2.0
setMethod("bitwise_not",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bitwise_not", x@jc)
            column(jc)
          })

#' @details
#' \code{bitwiseNOT}: Computes bitwise NOT.
#'
#' @rdname column_nonaggregate_functions
#' @aliases bitwiseNOT bitwiseNOT,Column-method
#' @note bitwiseNOT since 1.5.0
setMethod("bitwiseNOT",
          signature(x = "Column"),
          function(x) {
            .Deprecated("bitwise_not")
            bitwise_not(x)
          })

#' @details
#' \code{cbrt}: Computes the cube-root of the given value.
#'
#' @rdname column_math_functions
#' @aliases cbrt cbrt,Column-method
#' @note cbrt since 1.4.0
setMethod("cbrt",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cbrt", x@jc)
            column(jc)
          })

#' @details
#' \code{ceil}: Computes the ceiling of the given value.
#'
#' @rdname column_math_functions
#' @aliases ceil ceil,Column-method
#' @note ceil since 1.5.0
setMethod("ceil",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ceil", x@jc)
            column(jc)
          })

#' @details
#' \code{ceiling}: Alias for \code{ceil}.
#'
#' @rdname column_math_functions
#' @aliases ceiling ceiling,Column-method
#' @note ceiling since 1.5.0
setMethod("ceiling",
          signature(x = "Column"),
          function(x) {
            ceil(x)
          })

#' @details
#' \code{coalesce}: Returns the first column that is not NA, or NA if all inputs are.
#'
#' @rdname column_nonaggregate_functions
#' @aliases coalesce,Column-method
#' @note coalesce(Column) since 2.1.1
setMethod("coalesce",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
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

#' @details
#' \code{cos}: Returns the cosine of the given value,
#' as if computed by \code{java.lang.Math.cos()}. Units in radians.
#'
#' @rdname column_math_functions
#' @aliases cos cos,Column-method
#' @note cos since 1.5.0
setMethod("cos",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cos", x@jc)
            column(jc)
          })

#' @details
#' \code{cosh}: Returns the hyperbolic cosine of the given value,
#' as if computed by \code{java.lang.Math.cosh()}.
#'
#' @rdname column_math_functions
#' @aliases cosh cosh,Column-method
#' @note cosh since 1.5.0
setMethod("cosh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cosh", x@jc)
            column(jc)
          })

#' @details
#' \code{cot}: Returns the cotangent of the given value.
#'
#' @rdname column_math_functions
#' @aliases cot cot,Column-method
#' @note cot since 3.3.0
setMethod("cot",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "cot", x@jc)
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
#' @examples \dontrun{count(df$c)}
#' @note count since 1.4.0
setMethod("count",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "count", x@jc)
            column(jc)
          })

#' @details
#' \code{crc32}: Calculates the cyclic redundancy check value  (CRC32) of a binary column
#' and returns the value as a bigint.
#'
#' @rdname column_misc_functions
#' @aliases crc32 crc32,Column-method
#' @note crc32 since 1.5.0
setMethod("crc32",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "crc32", x@jc)
            column(jc)
          })

#' @details
#' \code{csc}: Returns the cosecant of the given value.
#'
#' @rdname column_math_functions
#' @aliases csc csc,Column-method
#' @note csc since 3.3.0
setMethod("csc",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "csc", x@jc)
            column(jc)
          })

#' @details
#' \code{hash}: Calculates the hash code of given columns, and returns the result
#' as an int column.
#'
#' @rdname column_misc_functions
#' @aliases hash hash,Column-method
#' @note hash since 2.0.0
setMethod("hash",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "hash", jcols)
            column(jc)
          })

#' @details
#' \code{xxhash64}: Calculates the hash code of given columns using the 64-bit
#' variant of the xxHash algorithm, and returns the result as a long
#' column. The hash computation uses an initial seed of 42.
#'
#' @rdname column_misc_functions
#' @aliases xxhash64 xxhash64,Column-method
#' @note xxhash64 since 3.0.0
setMethod("xxhash64",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "xxhash64", jcols)
            column(jc)
          })

#' @details
#' \code{assert_true}: Returns null if the input column is true; throws an exception
#' with the provided error message otherwise.
#'
#' @param errMsg (optional) The error message to be thrown.
#'
#' @rdname column_misc_functions
#' @aliases assert_true assert_true,Column-method
#' @examples
#' \dontrun{
#' tmp <- mutate(df, v1 = assert_true(df$vs < 2),
#'                   v2 = assert_true(df$vs < 2, "custom error message"),
#'                   v3 = assert_true(df$vs < 2, df$vs))
#' head(tmp)}
#' @note assert_true since 3.1.0
setMethod("assert_true",
          signature(x = "Column"),
          function(x, errMsg = NULL) {
            jc <- if (is.null(errMsg)) {
              callJStatic("org.apache.spark.sql.functions", "assert_true", x@jc)
            } else {
              if (is.character(errMsg)) {
                stopifnot(length(errMsg) == 1)
                errMsg <- lit(errMsg)
              }
              callJStatic("org.apache.spark.sql.functions", "assert_true", x@jc, errMsg@jc)
            }
            column(jc)
          })

#' @details
#' \code{raise_error}: Throws an exception with the provided error message.
#'
#' @rdname column_misc_functions
#' @aliases raise_error raise_error,characterOrColumn-method
#' @examples
#' \dontrun{
#' tmp <- mutate(df, v1 = raise_error("error message"))
#' head(tmp)}
#' @note raise_error since 3.1.0
setMethod("raise_error",
          signature(x = "characterOrColumn"),
          function(x) {
            if (is.character(x)) {
              stopifnot(length(x) == 1)
              x <- lit(x)
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "raise_error", x@jc)
            column(jc)
          })

#' @details
#' \code{dayofmonth}: Extracts the day of the month as an integer from a
#' given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases dayofmonth dayofmonth,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, df$time, year(df$time), quarter(df$time), month(df$time),
#'             dayofmonth(df$time), dayofweek(df$time), dayofyear(df$time), weekofyear(df$time)))
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
#' \code{dayofweek}: Extracts the day of the week as an integer from a
#' given date/timestamp/string.
#' Ranges from 1 for a Sunday through to 7 for a Saturday
#'
#' @rdname column_datetime_functions
#' @aliases dayofweek dayofweek,Column-method
#' @note dayofweek since 2.3.0
setMethod("dayofweek",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "dayofweek", x@jc)
            column(jc)
          })

#' @details
#' \code{dayofyear}: Extracts the day of the year as an integer from a
#' given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases dayofyear dayofyear,Column-method
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
#' @param charset character set to use (one of "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE",
#'                "UTF-16LE", "UTF-16").
#'
#' @rdname column_string_functions
#' @aliases decode decode,Column,character-method
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
#' @note encode since 1.6.0
setMethod("encode",
          signature(x = "Column", charset = "character"),
          function(x, charset) {
            jc <- callJStatic("org.apache.spark.sql.functions", "encode", x@jc, charset)
            column(jc)
          })

#' @details
#' \code{exp}: Computes the exponential of the given value.
#'
#' @rdname column_math_functions
#' @aliases exp exp,Column-method
#' @note exp since 1.5.0
setMethod("exp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "exp", x@jc)
            column(jc)
          })

#' @details
#' \code{expm1}: Computes the exponential of the given value minus one.
#'
#' @rdname column_math_functions
#' @aliases expm1 expm1,Column-method
#' @note expm1 since 1.5.0
setMethod("expm1",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "expm1", x@jc)
            column(jc)
          })

#' @details
#' \code{factorial}: Computes the factorial of the given value.
#'
#' @rdname column_math_functions
#' @aliases factorial factorial,Column-method
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
#' Note: the function is non-deterministic because its results depends on the order of the rows
#' which may be non-deterministic after a shuffle.
#'
#' @param na.rm a logical value indicating whether NA values should be stripped
#'        before the computation proceeds.
#'
#' @rdname first
#' @name first
#' @aliases first,characterOrColumn-method
#' @family aggregate functions
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

#' @details
#' \code{floor}: Computes the floor of the given value.
#'
#' @rdname column_math_functions
#' @aliases floor floor,Column-method
#' @note floor since 1.5.0
setMethod("floor",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "floor", x@jc)
            column(jc)
          })

#' @details
#' \code{hex}: Computes hex value of the given column.
#'
#' @rdname column_math_functions
#' @aliases hex hex,Column-method
#' @note hex since 1.5.0
setMethod("hex",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "hex", x@jc)
            column(jc)
          })

#' @details
#' \code{hour}: Extracts the hour as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases hour hour,Column-method
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
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, sex_lower = lower(df$Sex), age_upper = upper(df$age),
#'                   sex_age = concat_ws(" ", lower(df$sex), lower(df$age)))
#' head(tmp)
#' tmp2 <- mutate(tmp, s1 = initcap(tmp$sex_lower), s2 = initcap(tmp$sex_age),
#'                     s3 = reverse(df$Sex))
#' head(tmp2)}
#' @note initcap since 1.5.0
setMethod("initcap",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "initcap", x@jc)
            column(jc)
          })

#' @details
#' \code{isnan}: Returns true if the column is NaN.
#' @rdname column_nonaggregate_functions
#' @aliases isnan isnan,Column-method
#' @note isnan since 2.0.0
setMethod("isnan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "isnan", x@jc)
            column(jc)
          })

#' @details
#' \code{is.nan}: Alias for \link{isnan}.
#'
#' @rdname column_nonaggregate_functions
#' @aliases is.nan is.nan,Column-method
#' @note is.nan since 2.0.0
setMethod("is.nan",
          signature(x = "Column"),
          function(x) {
            isnan(x)
          })

#' @details
#' \code{kurtosis}: Returns the kurtosis of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases kurtosis kurtosis,Column-method
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
#' Note: the function is non-deterministic because its results depends on the order of the rows
#' which may be non-deterministic after a shuffle.
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
#' \code{length}: Computes the character length of a string data or number of bytes
#' of a binary data. The length of string data includes the trailing spaces.
#' The length of binary data includes binary zeros.
#'
#' @rdname column_string_functions
#' @aliases length length,Column-method
#' @note length since 1.5.0
setMethod("length",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "length", x@jc)
            column(jc)
          })

#' @details
#' \code{log}: Computes the natural logarithm of the given value.
#'
#' @rdname column_math_functions
#' @aliases log log,Column-method
#' @note log since 1.5.0
setMethod("log",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log", x@jc)
            column(jc)
          })

#' @details
#' \code{ln}: Alias for \code{log}.
#'
#' @rdname column_math_functions
#' @aliases ln ln,Column-method
#' @note ln since 3.5.0
setMethod("ln",
          signature(x = "Column"),
          function(x) {
            log(x)
          })

#' @details
#' \code{log10}: Computes the logarithm of the given value in base 10.
#'
#' @rdname column_math_functions
#' @aliases log10 log10,Column-method
#' @note log10 since 1.5.0
setMethod("log10",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log10", x@jc)
            column(jc)
          })

#' @details
#' \code{log1p}: Computes the natural logarithm of the given value plus one.
#'
#' @rdname column_math_functions
#' @aliases log1p log1p,Column-method
#' @note log1p since 1.5.0
setMethod("log1p",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "log1p", x@jc)
            column(jc)
          })

#' @details
#' \code{log2}: Computes the logarithm of the given column in base 2.
#'
#' @rdname column_math_functions
#' @aliases log2 log2,Column-method
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
#' @note lower since 1.4.0
setMethod("lower",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "lower", x@jc)
            column(jc)
          })

#' @details
#' \code{ltrim}: Trims the spaces from left end for the specified string value. Optionally a
#' \code{trimString} can be specified.
#'
#' @rdname column_string_functions
#' @aliases ltrim ltrim,Column,missing-method
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
          signature(x = "Column", trimString = "missing"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ltrim", x@jc)
            column(jc)
          })

#' @param trimString a character string to trim with
#' @rdname column_string_functions
#' @aliases ltrim,Column,character-method
#' @note ltrim(Column, character) since 2.3.0
setMethod("ltrim",
          signature(x = "Column", trimString = "character"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ltrim", x@jc, trimString)
            column(jc)
          })

#' @details
#' \code{make_date}: Create date from year, month and day fields.
#'
#' @rdname column_datetime_functions
#' @aliases make_date make_date,Column-method
#' @note make_date since 3.3.0
#' @examples
#'
#' \dontrun{
#' df <- createDataFrame(
#'   list(list(2021, 10, 22), list(2021, 13, 1),
#'        list(2021, 2, 29), list(2020, 2, 29)),
#'   list("year", "month", "day")
#' )
#' tmp <- head(select(df, make_date(df$year, df$month, df$day)))
#' head(tmp)}
setMethod("make_date",
          signature(x = "Column", y = "Column", z = "Column"),
          function(x, y, z) {
            jc <- callJStatic("org.apache.spark.sql.functions", "make_date",
                              x@jc, y@jc, z@jc)
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

#' @details
#' \code{max_by}: Returns the value associated with the maximum value of ord.
#'
#' @rdname column_aggregate_functions
#' @aliases max_by max_by,Column-method
#' @note max_by since 3.3.0
#' @examples
#'
#' \dontrun{
#' df <- createDataFrame(
#'   list(list("Java", 2012, 20000), list("dotNET", 2012, 5000),
#'        list("dotNET", 2013, 48000), list("Java", 2013, 30000)),
#'   list("course", "year", "earnings")
#' )
#' tmp <- agg(groupBy(df, df$"course"), "max_by" = max_by(df$"year", df$"earnings"))
#' head(tmp)}
setMethod("max_by",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "max_by", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{md5}: Calculates the MD5 digest of a binary column and returns the value
#' as a 32 character hex string.
#'
#' @rdname column_misc_functions
#' @aliases md5 md5,Column-method
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
#' @note min since 1.5.0
setMethod("min",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "min", x@jc)
            column(jc)
          })

#' @details
#' \code{min_by}: Returns the value associated with the minimum value of ord.
#'
#' @rdname column_aggregate_functions
#' @aliases min_by min_by,Column-method
#' @note min_by since 3.3.0
#' @examples
#'
#' \dontrun{
#' df <- createDataFrame(
#'   list(list("Java", 2012, 20000), list("dotNET", 2012, 5000),
#'        list("dotNET", 2013, 48000), list("Java", 2013, 30000)),
#'   list("course", "year", "earnings")
#' )
#' tmp <- agg(groupBy(df, df$"course"), "min_by" = min_by(df$"year", df$"earnings"))
#' head(tmp)}
setMethod("min_by",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "min_by", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{minute}: Extracts the minute as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases minute minute,Column-method
#' @note minute since 1.5.0
setMethod("minute",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "minute", x@jc)
            column(jc)
          })

#' @details
#' \code{monotonically_increasing_id}: Returns a column that generates monotonically increasing
#' 64-bit integers. The generated ID is guaranteed to be monotonically increasing and unique,
#' but not consecutive. The current implementation puts the partition ID in the upper 31 bits,
#' and the record number within each partition in the lower 33 bits. The assumption is that the
#' SparkDataFrame has less than 1 billion partitions, and each partition has less than 8 billion
#' records. As an example, consider a SparkDataFrame with two partitions, each with 3 records.
#' This expression would return the following IDs:
#' 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
#' This is equivalent to the MONOTONICALLY_INCREASING_ID function in SQL.
#' The method should be used with no argument.
#' Note: the function is non-deterministic because its result depends on partition IDs.
#'
#' @rdname column_nonaggregate_functions
#' @aliases monotonically_increasing_id monotonically_increasing_id,missing-method
#' @examples
#'
#' \dontrun{head(select(df, monotonically_increasing_id()))}
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
#' @note month since 1.5.0
setMethod("month",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "month", x@jc)
            column(jc)
          })

#' @details
#' \code{negate}: Unary minus, i.e. negate the expression.
#'
#' @rdname column_nonaggregate_functions
#' @aliases negate negate,Column-method
#' @note negate since 1.5.0
setMethod("negate",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "negate", x@jc)
            column(jc)
          })

#' @details
#' \code{negative}: Alias for \code{negate}.
#'
#' @rdname column_nonaggregate_functions
#' @aliases negative negative,Column-method
#' @note negative since 3.5.0
setMethod("negative",
          signature(x = "Column"),
          function(x) {
            negate(x)
          })

#' @details
#' \code{positive}: Unary plus, i.e. return the expression.
#'
#' @rdname column_nonaggregate_functions
#' @aliases positive positive,Column-method
#' @note positive since 3.5.0
setMethod("positive",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "positive", x@jc)
            column(jc)
          })

#' @details
#' \code{width_bucket} Returns the bucket number into which the value of this expression would
#' fall after being evaluated. Note that input arguments must follow conditions listed below;
#' otherwise, the method will return null.
#'
#' @param v value to compute a bucket number in the histogram.
#' @param min minimum value of the histogram
#' @param max maximum value of the histogram
#' @param numBucket the number of buckets
#'
#' @rdname column_math_functions
#' @aliases width_bucket width_bucket,Column-method
#' @note width_bucket since 3.5.0
setMethod("width_bucket",
          signature(v = "Column", min = "Column", max = "Column", numBucket = "Column"),
          function(v, min, max, numBucket) {
            jc <- callJStatic(
              "org.apache.spark.sql.functions", "width_bucket",
              v@jc, min@jc, max@jc, numBucket@jc
            )
            column(jc)
          })

#' @details
#' \code{octet_length}: Calculates the byte length for the specified string column.
#'
#' @rdname column_string_functions
#' @aliases octet_length octet_length,Column-method
#' @note length since 3.3.0
setMethod("octet_length",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "octet_length", x@jc)
            column(jc)
          })

#' @details
#' \code{overlay}: Overlay the specified portion of \code{x} with \code{replace},
#' starting from byte position \code{pos} of \code{src} and proceeding for
#' \code{len} bytes.
#'
#' @param replace a Column with replacement.
#'
#' @rdname column_string_functions
#' @aliases overlay overlay,Column-method,numericOrColumn-method
#' @note overlay since 3.0.0
setMethod("overlay",
  signature(x = "Column", replace = "Column", pos = "numericOrColumn"),
  function(x, replace, pos, len = -1) {
    if (is.numeric(pos)) {
      pos <- lit(as.integer(pos))
    }

    if (is.numeric(len)) {
      len <- lit(as.integer(len))
    }

    jc <- callJStatic(
      "org.apache.spark.sql.functions", "overlay",
      x@jc, replace@jc, pos@jc, len@jc
    )

    column(jc)
  })

#' @details
#' \code{product}: Returns the product of the values in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases product product,Column-method
#' @note product since 3.2.0
setMethod("product",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "product", x@jc)
            column(jc)
          })

#' @details
#' \code{quarter}: Extracts the quarter as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases quarter quarter,Column-method
#' @note quarter since 1.5.0
setMethod("quarter",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "quarter", x@jc)
            column(jc)
          })

#' @details
#' \code{percentile_approx} Returns the approximate \code{percentile} of the numeric column
#' \code{col} which is the smallest value in the ordered \code{col} values (sorted from least to
#' greatest) such that no more than \code{percentage} of \code{col} values is less than the value
#' or equal to that value.
#'
#' @param percentage Numeric percentage at which percentile should be computed
#'                   All values should be between 0 and 1.
#'                   If length equals to 1 resulting column is of type double,
#'                   otherwise, array type of double.
#' @param accuracy A positive numeric literal (default: 10000) which
#'                 controls approximation accuracy at the cost of memory.
#'                 Higher value of accuracy yields better accuracy, 1.0/accuracy
#'                 is the relative error of the approximation.
#'
#' @rdname column_aggregate_functions
#' @aliases percentile_approx percentile_approx,Column-method
#' @note percentile_approx since 3.1.0
setMethod("percentile_approx",
          signature(x = "characterOrColumn", percentage = "numericOrColumn"),
          function(x, percentage, accuracy = 10000) {
            col <- if (class(x) == "Column") {
              x@jc
            } else {
              column(x)@jc
            }

            percentage <- if (class(percentage) == "Column") {
              percentage@jc
            } else if (length(percentage) > 1) {
              do.call(create_array, lapply(percentage, lit))@jc
            } else {
              lit(percentage)@jc
            }

            accuracy <- if (class(accuracy) == "Column") {
              accuracy@jc
            } else {
              lit(as.integer(accuracy))@jc
            }

            jc <- callJStatic(
              "org.apache.spark.sql.functions", "percentile_approx",
              col, percentage, accuracy
            )
            column(jc)
          })

#' @details
#' \code{reverse}: Returns a reversed string or an array with reverse order of elements.
#'
#' @rdname column_collection_functions
#' @aliases reverse reverse,Column-method
#' @note reverse since 1.5.0
setMethod("reverse",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "reverse", x@jc)
            column(jc)
          })

#' @details
#' \code{rint}: Returns the double value that is closest in value to the argument and
#' is equal to a mathematical integer.
#'
#' @rdname column_math_functions
#' @aliases rint rint,Column-method
#' @note rint since 1.5.0
setMethod("rint",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rint", x@jc)
            column(jc)
          })

#' @details
#' \code{round}: Returns the value of the column rounded to 0 decimal places
#' using HALF_UP rounding mode.
#'
#' @rdname column_math_functions
#' @aliases round round,Column-method
#' @note round since 1.5.0
setMethod("round",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "round", x@jc)
            column(jc)
          })

#' @details
#' \code{bround}: Returns the value of the column \code{e} rounded to \code{scale} decimal places
#' using HALF_EVEN rounding mode if \code{scale} >= 0 or at integer part when \code{scale} < 0.
#' Also known as Gaussian rounding or bankers' rounding that rounds to the nearest even number.
#' bround(2.5, 0) = 2, bround(3.5, 0) = 4.
#'
#' @param scale round to \code{scale} digits to the right of the decimal point when
#'        \code{scale} > 0, the nearest even number when \code{scale} = 0, and \code{scale} digits
#'        to the left of the decimal point when \code{scale} < 0.
#' @rdname column_math_functions
#' @aliases bround bround,Column-method
#' @note bround since 2.0.0
setMethod("bround",
          signature(x = "Column"),
          function(x, scale = 0) {
            jc <- callJStatic("org.apache.spark.sql.functions", "bround", x@jc, as.integer(scale))
            column(jc)
          })

#' @details
#' \code{rtrim}: Trims the spaces from right end for the specified string value. Optionally a
#' \code{trimString} can be specified.
#'
#' @rdname column_string_functions
#' @aliases rtrim rtrim,Column,missing-method
#' @note rtrim since 1.5.0
setMethod("rtrim",
          signature(x = "Column", trimString = "missing"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rtrim", x@jc)
            column(jc)
          })

#' @rdname column_string_functions
#' @aliases rtrim,Column,character-method
#' @note rtrim(Column, character) since 2.3.0
setMethod("rtrim",
          signature(x = "Column", trimString = "character"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rtrim", x@jc, trimString)
            column(jc)
          })

#' @details
#' \code{sd}: Alias for \code{stddev_samp}.
#'
#' @rdname column_aggregate_functions
#' @aliases sd sd,Column-method
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
#' \code{second}: Extracts the second as an integer from a given date/timestamp/string.
#'
#' @rdname column_datetime_functions
#' @aliases second second,Column-method
#' @note second since 1.5.0
setMethod("second",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "second", x@jc)
            column(jc)
          })

#' @details
#' \code{sha1}: Calculates the SHA-1 digest of a binary column and returns the value
#' as a 40 character hex string.
#'
#' @rdname column_misc_functions
#' @aliases sha1 sha1,Column-method
#' @note sha1 since 1.5.0
setMethod("sha1",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sha1", x@jc)
            column(jc)
          })

#' @details
#' \code{signum}: Computes the signum of the given value.
#'
#' @rdname column_math_functions
#' @aliases signum signum,Column-method
#' @note signum since 1.5.0
setMethod("signum",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "signum", x@jc)
            column(jc)
          })

#' @details
#' \code{sign}: Alias for \code{signum}.
#'
#' @rdname column_math_functions
#' @aliases sign sign,Column-method
#' @note sign since 1.5.0
setMethod("sign", signature(x = "Column"),
          function(x) {
            signum(x)
          })

#' @details
#' \code{sin}: Returns the sine of the given value,
#' as if computed by \code{java.lang.Math.sin()}. Units in radians.
#'
#' @rdname column_math_functions
#' @aliases sin sin,Column-method
#' @note sin since 1.5.0
setMethod("sin",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sin", x@jc)
            column(jc)
          })

#' @details
#' \code{sinh}: Returns the hyperbolic sine of the given value,
#' as if computed by \code{java.lang.Math.sinh()}.
#'
#' @rdname column_math_functions
#' @aliases sinh sinh,Column-method
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
#' @note soundex since 1.5.0
setMethod("soundex",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "soundex", x@jc)
            column(jc)
          })

#' @details
#' \code{spark_partition_id}: Returns the partition ID as a SparkDataFrame column.
#' Note that this is nondeterministic because it depends on data partitioning and
#' task scheduling.
#' This is equivalent to the \code{SPARK_PARTITION_ID} function in SQL.
#'
#' @rdname column_nonaggregate_functions
#' @aliases spark_partition_id spark_partition_id,missing-method
#' @examples
#'
#' \dontrun{head(select(df, spark_partition_id()))}
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
#' \code{std}: Alias for \code{stddev}.
#'
#' @rdname column_aggregate_functions
#' @aliases std std,Column-method
#' @note std since 3.5.0
setMethod("std",
          signature(x = "Column"),
          function(x) {
            stddev(x)
          })

#' @details
#' \code{stddev_pop}: Returns the population standard deviation of the expression in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases stddev_pop stddev_pop,Column-method
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
#' @note stddev_samp since 1.6.0
setMethod("stddev_samp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "stddev_samp", x@jc)
            column(jc)
          })

#' @details
#' \code{struct}: Creates a new struct column that composes multiple input columns.
#'
#' @rdname column_nonaggregate_functions
#' @aliases struct struct,characterOrColumn-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, v1 = struct(df$mpg, df$cyl), v2 = struct("hp", "wt", "vs"),
#'                   v3 = create_array(df$mpg, df$cyl, df$hp),
#'                   v4 = create_map(lit("x"), lit(1.0), lit("y"), lit(-1.0)))
#' head(tmp)}
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

#' @details
#' \code{sqrt}: Computes the square root of the specified float value.
#'
#' @rdname column_math_functions
#' @aliases sqrt sqrt,Column-method
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
#' @note sum since 1.5.0
setMethod("sum",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sum", x@jc)
            column(jc)
          })

#' @details
#' \code{sum_distinct}: Returns the sum of distinct values in the expression.
#'
#' @rdname column_aggregate_functions
#' @aliases sum_distinct sum_distinct,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, sum_distinct(df$gear)))
#' head(distinct(select(df, "gear")))}
#' @note sum_distinct since 3.2.0
setMethod("sum_distinct",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sum_distinct", x@jc)
            column(jc)
          })

#' @details
#' \code{sumDistinct}: Returns the sum of distinct values in the expression.
#'
#' @rdname column_aggregate_functions
#' @aliases sumDistinct sumDistinct,Column-method
#' @note sumDistinct since 1.4.0
setMethod("sumDistinct",
          signature(x = "Column"),
          function(x) {
            .Deprecated("sum_distinct")
            sum_distinct(x)
          })

#' @details
#' \code{tan}: Returns the tangent of the given value,
#' as if computed by \code{java.lang.Math.tan()}.
#' Units in radians.
#'
#' @rdname column_math_functions
#' @aliases tan tan,Column-method
#' @note tan since 1.5.0
setMethod("tan",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "tan", x@jc)
            column(jc)
          })

#' @details
#' \code{tanh}: Returns the hyperbolic tangent of the given value,
#' as if computed by \code{java.lang.Math.tanh()}.
#'
#' @rdname column_math_functions
#' @aliases tanh tanh,Column-method
#' @note tanh since 1.5.0
setMethod("tanh",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "tanh", x@jc)
            column(jc)
          })

#' @details
#' \code{toDegrees}: Converts an angle measured in radians to an approximately equivalent angle
#' measured in degrees.
#'
#' @rdname column_math_functions
#' @aliases toDegrees toDegrees,Column-method
#' @note toDegrees since 1.4.0
setMethod("toDegrees",
          signature(x = "Column"),
          function(x) {
            .Deprecated("degrees")
            jc <- callJStatic("org.apache.spark.sql.functions", "degrees", x@jc)
            column(jc)
          })

#' @details
#' \code{degrees}: Converts an angle measured in radians to an approximately equivalent angle
#' measured in degrees.
#'
#' @rdname column_math_functions
#' @aliases degrees degrees,Column-method
#' @note degrees since 3.0.0
setMethod("degrees",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "degrees", x@jc)
            column(jc)
          })

#' @details
#' \code{toRadians}: Converts an angle measured in degrees to an approximately equivalent angle
#' measured in radians.
#'
#' @rdname column_math_functions
#' @aliases toRadians toRadians,Column-method
#' @note toRadians since 1.4.0
setMethod("toRadians",
          signature(x = "Column"),
          function(x) {
            .Deprecated("radians")
            jc <- callJStatic("org.apache.spark.sql.functions", "radians", x@jc)
            column(jc)
          })

#' @details
#' \code{radians}: Converts an angle measured in degrees to an approximately equivalent angle
#' measured in radians.
#'
#' @rdname column_math_functions
#' @aliases radians radians,Column-method
#' @note radians since 3.0.0
setMethod("radians",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "radians", x@jc)
            column(jc)
          })

#' @details
#' \code{to_date}: Converts the column into a DateType. You may optionally specify
#' a format according to the rules in:
#' \href{https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html}{Datetime Pattern}
#' If the string cannot be parsed according to the specified format (or default),
#' the value of the column will be null.
#' By default, it follows casting rules to a DateType if the format is omitted
#' (equivalent to \code{cast(df$x, "date")}).
#'
#' @rdname column_datetime_functions
#' @aliases to_date to_date,Column,missing-method
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
#' @note to_date(Column, character) since 2.2.0
setMethod("to_date",
          signature(x = "Column", format = "character"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_date", x@jc, format)
            column(jc)
          })

#' @details
#' \code{to_json}: Converts a column containing a \code{structType}, a \code{mapType}
#' or an \code{arrayType} into a Column of JSON string.
#' Resolving the Column can fail if an unsupported type is encountered.
#'
#' @rdname column_collection_functions
#' @aliases to_json to_json,Column-method
#' @examples
#'
#' \dontrun{
#' # Converts a struct into a JSON object
#' df2 <- sql("SELECT named_struct('date', cast('2000-01-01' as date)) as d")
#' select(df2, to_json(df2$d, dateFormat = 'dd/MM/yyyy'))
#'
#' # Converts an array of structs into a JSON array
#' df2 <- sql("SELECT array(named_struct('name', 'Bob'), named_struct('name', 'Alice')) as people")
#' df2 <- mutate(df2, people_json = to_json(df2$people))
#'
#' # Converts a map into a JSON object
#' df2 <- sql("SELECT map('name', 'Bob') as people")
#' df2 <- mutate(df2, people_json = to_json(df2$people))
#'
#' # Converts an array of maps into a JSON array
#' df2 <- sql("SELECT array(map('name', 'Bob'), map('name', 'Alice')) as people")
#' df2 <- mutate(df2, people_json = to_json(df2$people))
#'
#' # Converts a map into a pretty JSON object
#' df2 <- sql("SELECT map('name', 'Bob') as people")
#' df2 <- mutate(df2, people_json = to_json(df2$people, pretty = TRUE))}
#' @note to_json since 2.2.0
setMethod("to_json", signature(x = "Column"),
          function(x, ...) {
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions", "to_json", x@jc, options)
            column(jc)
          })

#' @details
#' \code{to_csv}: Converts a column containing a \code{structType} into a Column of CSV string.
#' Resolving the Column can fail if an unsupported type is encountered.
#'
#' @rdname column_collection_functions
#' @aliases to_csv to_csv,Column-method
#' @examples
#'
#' \dontrun{
#' # Converts a struct into a CSV string
#' df2 <- sql("SELECT named_struct('date', cast('2000-01-01' as date)) as d")
#' select(df2, to_csv(df2$d, dateFormat = 'dd/MM/yyyy'))}
#' @note to_csv since 3.0.0
setMethod("to_csv", signature(x = "Column"),
          function(x, ...) {
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions", "to_csv", x@jc, options)
            column(jc)
          })

#' @details
#' \code{to_timestamp}: Converts the column into a TimestampType. You may optionally specify
#' a format according to the rules in:
#' \href{https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html}{Datetime Pattern}
#' If the string cannot be parsed according to the specified format (or default),
#' the value of the column will be null.
#' By default, it follows casting rules to a TimestampType if the format is omitted
#' (equivalent to \code{cast(df$x, "timestamp")}).
#'
#' @rdname column_datetime_functions
#' @aliases to_timestamp to_timestamp,Column,missing-method
#' @note to_timestamp(Column) since 2.2.0
setMethod("to_timestamp",
          signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_timestamp", x@jc)
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases to_timestamp,Column,character-method
#' @note to_timestamp(Column, character) since 2.2.0
setMethod("to_timestamp",
          signature(x = "Column", format = "character"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "to_timestamp", x@jc, format)
            column(jc)
          })

#' @details
#' \code{trim}: Trims the spaces from both ends for the specified string column. Optionally a
#' \code{trimString} can be specified.
#'
#' @rdname column_string_functions
#' @aliases trim trim,Column,missing-method
#' @note trim since 1.5.0
setMethod("trim",
          signature(x = "Column", trimString = "missing"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "trim", x@jc)
            column(jc)
          })

#' @rdname column_string_functions
#' @aliases trim,Column,character-method
#' @note trim(Column, character) since 2.3.0
setMethod("trim",
          signature(x = "Column", trimString = "character"),
          function(x, trimString) {
            jc <- callJStatic("org.apache.spark.sql.functions", "trim", x@jc, trimString)
            column(jc)
          })

#' @details
#' \code{unbase64}: Decodes a BASE64 encoded string column and returns it as a binary column.
#' This is the reverse of base64.
#'
#' @rdname column_string_functions
#' @aliases unbase64 unbase64,Column-method
#' @note unbase64 since 1.5.0
setMethod("unbase64",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unbase64", x@jc)
            column(jc)
          })

#' @details
#' \code{unhex}: Inverse of hex. Interprets each pair of characters as a hexadecimal number
#' and converts to the byte representation of number.
#'
#' @rdname column_math_functions
#' @aliases unhex unhex,Column-method
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
#' @note var_samp since 1.6.0
setMethod("var_samp",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "var_samp", x@jc)
            column(jc)
          })

#' @details
#' \code{weekofyear}: Extracts the week number as an integer from a given date/timestamp/string.
#' A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
#' as defined by ISO 8601
#'
#' @rdname column_datetime_functions
#' @aliases weekofyear weekofyear,Column-method
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
#' @note year since 1.5.0
setMethod("year",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "year", x@jc)
            column(jc)
          })

#' @details
#' \code{atan2}: Returns the angle theta from the conversion of rectangular coordinates
#' (x, y) to polar coordinates (r, theta),
#' as if computed by \code{java.lang.Math.atan2()}. Units in radians.
#'
#' @rdname column_math_functions
#' @aliases atan2 atan2,Column-method
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
#' If \code{y} is later than \code{x} then the result is positive.
#'
#' @rdname column_datetime_diff_functions
#' @aliases datediff datediff,Column-method
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

#' @details
#' \code{hypot}: Computes "sqrt(a^2 + b^2)" without intermediate overflow or underflow.
#'
#' @rdname column_math_functions
#' @aliases hypot hypot,Column-method
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
#' @rdname column_string_functions
#' @aliases levenshtein levenshtein,Column-method
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
#' If \code{y} is later than \code{x}, then the result is positive. If \code{y} and \code{x}
#' are on the same day of month, or both are the last day of month, time of day will be ignored.
#' Otherwise, the difference is calculated based on 31 days per month, and rounded to 8 digits.
#'
#' @param roundOff an optional parameter to specify if the result is rounded off to 8 digits
#' @rdname column_datetime_diff_functions
#' @aliases months_between months_between,Column-method
#' @note months_between since 1.5.0
setMethod("months_between", signature(y = "Column"),
          function(y, x, roundOff = NULL) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- if (is.null(roundOff)) {
              callJStatic("org.apache.spark.sql.functions", "months_between", y@jc, x)
            } else {
              callJStatic("org.apache.spark.sql.functions", "months_between", y@jc, x,
                           as.logical(roundOff))
            }
            column(jc)
          })

#' @details
#' \code{nanvl}: Returns the first column (\code{y}) if it is not NaN, or the second column
#' (\code{x}) if the first column is NaN. Both inputs should be floating point columns
#' (DoubleType or FloatType).
#'
#' @rdname column_nonaggregate_functions
#' @aliases nanvl nanvl,Column-method
#' @note nanvl since 1.5.0
setMethod("nanvl", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "nanvl", y@jc, x)
            column(jc)
          })

#' @details
#' \code{pmod}: Returns the positive value of dividend mod divisor.
#' Column \code{x} is divisor column, and column \code{y} is the dividend column.
#'
#' @rdname column_math_functions
#' @aliases pmod pmod,Column-method
#' @note pmod since 1.5.0
setMethod("pmod", signature(y = "Column"),
          function(y, x) {
            if (class(x) == "Column") {
              x <- x@jc
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "pmod", y@jc, x)
            column(jc)
          })

#' @param rsd maximum relative standard deviation allowed (default = 0.05).
#'
#' @rdname column_aggregate_functions
#' @aliases approx_count_distinct,Column-method
#' @note approx_count_distinct(Column, numeric) since 3.0.0
setMethod("approx_count_distinct",
          signature(x = "Column"),
          function(x, rsd = 0.05) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approx_count_distinct", x@jc, rsd)
            column(jc)
          })

#' @rdname column_aggregate_functions
#' @aliases approxCountDistinct,Column-method
#' @note approxCountDistinct(Column, numeric) since 1.4.0
setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x, rsd = 0.05) {
            .Deprecated("approx_count_distinct")
            jc <- callJStatic("org.apache.spark.sql.functions", "approx_count_distinct", x@jc, rsd)
            column(jc)
          })

#' @details
#' \code{count_distinct}: Returns the number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases count_distinct count_distinct,Column-method
#' @note count_distinct since 3.2.0
setMethod("count_distinct",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "count_distinct", x@jc,
                              jcols)
            column(jc)
          })

#' @details
#' \code{countDistinct}: Returns the number of distinct items in a group.
#'
#' An alias of \code{count_distinct}, and it is encouraged to use \code{count_distinct} directly.
#'
#' @rdname column_aggregate_functions
#' @aliases countDistinct countDistinct,Column-method
#' @note countDistinct since 1.4.0
setMethod("countDistinct",
          signature(x = "Column"),
          function(x, ...) {
            count_distinct(x, ...)
          })

#' @details
#' \code{concat}: Concatenates multiple input columns together into a single column.
#' The function works with strings, binary and compatible array columns.
#'
#' @rdname column_collection_functions
#' @aliases concat concat,Column-method
#' @note concat since 1.5.0
setMethod("concat",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "concat", jcols)
            column(jc)
          })

#' @details
#' \code{greatest}: Returns the greatest value of the list of column names, skipping null values.
#' This function takes at least 2 parameters. It will return null if all parameters are null.
#'
#' @rdname column_nonaggregate_functions
#' @aliases greatest greatest,Column-method
#' @note greatest since 1.5.0
setMethod("greatest",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "greatest", jcols)
            column(jc)
          })

#' @details
#' \code{least}: Returns the least value of the list of column names, skipping null values.
#' This function takes at least 2 parameters. It will return null if all parameters are null.
#'
#' @rdname column_nonaggregate_functions
#' @aliases least least,Column-method
#' @note least since 1.5.0
setMethod("least",
          signature(x = "Column"),
          function(x, ...) {
            stopifnot(length(list(...)) > 0)
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "least", jcols)
            column(jc)
          })

#' @details
#' \code{n_distinct}: Returns the number of distinct items in a group.
#'
#' @rdname column_aggregate_functions
#' @aliases n_distinct n_distinct,Column-method
#' @note n_distinct since 1.4.0
setMethod("n_distinct", signature(x = "Column"),
          function(x, ...) {
            count_distinct(x, ...)
          })

#' @rdname count
#' @name n
#' @aliases n,Column-method
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
#' pattern letters of \code{java.time.format.DateTimeFormatter} can be used.
#' Note: Use when ever possible specialized functions like \code{year}. These benefit from a
#' specialized implementation.
#'
#' @rdname column_datetime_diff_functions
#'
#' @aliases date_format date_format,Column,character-method
#' @note date_format since 1.5.0
setMethod("date_format", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_format", y@jc, x)
            column(jc)
          })

setClassUnion("characterOrstructTypeOrColumn", c("character", "structType", "Column"))

#' @details
#' \code{from_json}: Parses a column containing a JSON string into a Column of \code{structType}
#' with the specified \code{schema} or array of \code{structType} if \code{as.json.array} is set
#' to \code{TRUE}. If the string is unparsable, the Column will contain the value NA.
#'
#' @rdname column_collection_functions
#' @param as.json.array indicating if input string is JSON array of objects or a single object.
#' @aliases from_json from_json,Column,characterOrstructTypeOrColumn-method
#' @examples
#'
#' \dontrun{
#' df2 <- sql("SELECT named_struct('date', cast('2000-01-01' as date)) as d")
#' df2 <- mutate(df2, d2 = to_json(df2$d, dateFormat = 'dd/MM/yyyy'))
#' schema <- structType(structField("date", "string"))
#' head(select(df2, from_json(df2$d2, schema, dateFormat = 'dd/MM/yyyy')))
#' df2 <- sql("SELECT named_struct('name', 'Bob') as people")
#' df2 <- mutate(df2, people_json = to_json(df2$people))
#' schema <- structType(structField("name", "string"))
#' head(select(df2, from_json(df2$people_json, schema)))
#' head(select(df2, from_json(df2$people_json, "name STRING")))
#' head(select(df2, from_json(df2$people_json, schema_of_json(head(df2)$people_json))))}
#' @note from_json since 2.2.0
setMethod("from_json", signature(x = "Column", schema = "characterOrstructTypeOrColumn"),
          function(x, schema, as.json.array = FALSE, ...) {
            if (is.character(schema)) {
              jschema <- structType(schema)$jobj
            } else if (class(schema) == "structType") {
              jschema <- schema$jobj
            } else {
              jschema <- schema@jc
            }

            if (as.json.array) {
              # This case is R-specifically different. Unlike Scala and Python side,
              # R side has 'as.json.array' option to indicate if the schema should be
              # treated as struct or element type of array in order to make it more
              # R-friendly.
              if (class(schema) == "Column") {
                df <- createDataFrame(list(list(0)))
                jschema <- collect(select(df, schema))[[1]][[1]]
                jschema <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                                       "createArrayType",
                                       jschema)
              } else {
                jschema <- callJStatic("org.apache.spark.sql.types.DataTypes",
                                       "createArrayType",
                                       jschema)
              }
            }
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_json",
                              x@jc, jschema, options)
            column(jc)
          })

#' @details
#' \code{schema_of_json}: Parses a JSON string and infers its schema in DDL format.
#'
#' @rdname column_collection_functions
#' @aliases schema_of_json schema_of_json,characterOrColumn-method
#' @examples
#'
#' \dontrun{
#' json <- "{\"name\":\"Bob\"}"
#' df <- sql("SELECT * FROM range(1)")
#' head(select(df, schema_of_json(json)))}
#' @note schema_of_json since 3.0.0
setMethod("schema_of_json", signature(x = "characterOrColumn"),
          function(x, ...) {
            if (class(x) == "character") {
              col <- callJStatic("org.apache.spark.sql.functions", "lit", x)
            } else {
              col <- x@jc
            }
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "schema_of_json",
                              col, options)
            column(jc)
          })

#' @details
#' \code{from_csv}: Parses a column containing a CSV string into a Column of \code{structType}
#' with the specified \code{schema}.
#' If the string is unparsable, the Column will contain the value NA.
#'
#' @rdname column_collection_functions
#' @aliases from_csv from_csv,Column,characterOrstructTypeOrColumn-method
#' @examples
#'
#' \dontrun{
#' csv <- "Amsterdam,2018"
#' df <- sql(paste0("SELECT '", csv, "' as csv"))
#' schema <- "city STRING, year INT"
#' head(select(df, from_csv(df$csv, schema)))
#' head(select(df, from_csv(df$csv, structType(schema))))
#' head(select(df, from_csv(df$csv, schema_of_csv(csv))))}
#' @note from_csv since 3.0.0
setMethod("from_csv", signature(x = "Column", schema = "characterOrstructTypeOrColumn"),
          function(x, schema, ...) {
            if (class(schema) == "structType") {
              schema <- callJMethod(schema$jobj, "toDDL")
            }

            if (is.character(schema)) {
              jschema <- callJStatic("org.apache.spark.sql.functions", "lit", schema)
            } else {
              jschema <- schema@jc
            }
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_csv",
                              x@jc, jschema, options)
            column(jc)
          })

#' @details
#' \code{schema_of_csv}: Parses a CSV string and infers its schema in DDL format.
#'
#' @rdname column_collection_functions
#' @aliases schema_of_csv schema_of_csv,characterOrColumn-method
#' @examples
#'
#' \dontrun{
#' csv <- "Amsterdam,2018"
#' df <- sql("SELECT * FROM range(1)")
#' head(select(df, schema_of_csv(csv)))}
#' @note schema_of_csv since 3.0.0
setMethod("schema_of_csv", signature(x = "characterOrColumn"),
          function(x, ...) {
            if (class(x) == "character") {
              col <- callJStatic("org.apache.spark.sql.functions", "lit", x)
            } else {
              col <- x@jc
            }
            options <- varargsToStrEnv(...)
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "schema_of_csv",
                              col, options)
            column(jc)
          })

#' @details
#' \code{from_utc_timestamp}: This is a common function for databases supporting TIMESTAMP WITHOUT
#' TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a
#' timestamp in UTC, and renders that timestamp as a timestamp in the given time zone.
#' However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
#' timezone-agnostic. So in Spark this function just shift the timestamp value from UTC timezone to
#' the given timezone.
#' This function may return confusing result if the input is a string with timezone, e.g.
#' (\code{2018-03-13T06:18:23+00:00}). The reason is that, Spark firstly cast the string to
#' timestamp according to the timezone in the string, and finally display the result by converting
#' the timestamp to string according to the session local timezone.
#'
#' @rdname column_datetime_diff_functions
#'
#' @aliases from_utc_timestamp from_utc_timestamp,Column,character-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, from_utc = from_utc_timestamp(df$time, "PST"),
#'                  to_utc = to_utc_timestamp(df$time, "PST"))
#' head(tmp)}
#' @note from_utc_timestamp since 1.5.0
setMethod("from_utc_timestamp", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "from_utc_timestamp", y@jc, x)
            column(jc)
          })

#' @details
#' \code{instr}: Locates the position of the first occurrence of a substring (\code{x})
#' in the given string column (\code{y}). Returns null if either of the arguments are null.
#' Note: The position is not zero based, but 1 based index. Returns 0 if the substring
#' could not be found in the string column.
#'
#' @rdname column_string_functions
#' @aliases instr instr,Column,character-method
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
#' \code{next_day("2015-07-27", "Sunday")} returns 2015-08-02 because that is the first Sunday
#' after 2015-07-27. Day of the week parameter is case insensitive, and accepts first three or
#' two characters: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
#'
#' @rdname column_datetime_diff_functions
#' @aliases next_day next_day,Column,character-method
#' @note next_day since 1.5.0
setMethod("next_day", signature(y = "Column", x = "character"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "next_day", y@jc, x)
            column(jc)
          })

#' @details
#' \code{to_utc_timestamp}: This is a common function for databases supporting TIMESTAMP WITHOUT
#' TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a
#' timestamp in the given timezone, and renders that timestamp as a timestamp in UTC.
#' However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
#' timezone-agnostic. So in Spark this function just shift the timestamp value from the given
#' timezone to UTC timezone.
#' This function may return confusing result if the input is a string with timezone, e.g.
#' (\code{2018-03-13T06:18:23+00:00}). The reason is that, Spark firstly cast the string to
#' timestamp according to the timezone in the string, and finally display the result by converting
#' the timestamp to string according to the session local timezone.
#'
#' @rdname column_datetime_diff_functions
#' @aliases to_utc_timestamp to_utc_timestamp,Column,character-method
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
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, t1 = add_months(df$time, 1),
#'                   t2 = date_add(df$time, 2),
#'                   t3 = date_sub(df$time, 3),
#'                   t4 = next_day(df$time, "Sun"))
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
#' @note date_sub since 1.5.0
setMethod("date_sub", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_sub", y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{format_number}: Formats numeric column \code{y} to a format like '#,###,###.##',
#' rounded to \code{x} decimal places with HALF_EVEN round mode, and returns the result
#' as a string column.
#' If \code{x} is 0, the result has no decimal point or fractional part.
#' If \code{x} < 0, the result will be null.
#'
#' @rdname column_string_functions
#' @aliases format_number format_number,Column,numeric-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, v1 = df$Freq/3)
#' head(select(tmp, format_number(tmp$v1, 0), format_number(tmp$v1, 2),
#'                  format_string("%4.2f %s", tmp$v1, tmp$Sex)), 10)}
#' @note format_number since 1.5.0
setMethod("format_number", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "format_number",
                              y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{sha2}: Calculates the SHA-2 family of hash functions of a binary column and
#' returns the value as a hex string. The second argument \code{x} specifies the number
#' of bits, and is one of 224, 256, 384, or 512.
#'
#' @rdname column_misc_functions
#' @aliases sha2 sha2,Column,numeric-method
#' @note sha2 since 1.5.0
setMethod("sha2", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sha2", y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{shiftleft}: Shifts the given value numBits left. If the given value is a long value,
#' this function will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftleft shiftleft,Column,numeric-method
#' @note shiftleft since 3.2.0
setMethod("shiftleft", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftleft",
                              y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{shiftLeft}: Shifts the given value numBits left. If the given value is a long value,
#' this function will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftLeft shiftLeft,Column,numeric-method
#' @note shiftLeft since 1.5.0
setMethod("shiftLeft", signature(y = "Column", x = "numeric"),
          function(y, x) {
            .Deprecated("shiftleft")
            shiftleft(y, x)
          })

#' @details
#' \code{shiftright}: (Signed) shifts the given value numBits right. If the given value is a long
#' value, it will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftright shiftright,Column,numeric-method
#' @note shiftright since 3.2.0
setMethod("shiftright", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftright",
                              y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{shiftRight}: (Signed) shifts the given value numBits right. If the given value is a long
#' value, it will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftRight shiftRight,Column,numeric-method
#' @note shiftRight since 1.5.0
setMethod("shiftRight", signature(y = "Column", x = "numeric"),
          function(y, x) {
            .Deprecated("shiftright")
            shiftright(y, x)
          })

#' @details
#' \code{shiftrightunsigned}: (Unsigned) shifts the given value numBits right. If the given value is
#' a long value, it will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftrightunsigned shiftrightunsigned,Column,numeric-method
#' @note shiftrightunsigned since 3.2.0
setMethod("shiftrightunsigned", signature(y = "Column", x = "numeric"),
          function(y, x) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "shiftrightunsigned",
                              y@jc, as.integer(x))
            column(jc)
          })

#' @details
#' \code{shiftRightUnsigned}: (Unsigned) shifts the given value numBits right. If the given value is
#' a long value, it will return a long value else it will return an integer value.
#'
#' @rdname column_math_functions
#' @aliases shiftRightUnsigned shiftRightUnsigned,Column,numeric-method
#' @note shiftRightUnsigned since 1.5.0
setMethod("shiftRightUnsigned", signature(y = "Column", x = "numeric"),
          function(y, x) {
            .Deprecated("shiftrightunsigned")
            shiftrightunsigned(y, x)
          })

#' @details
#' \code{concat_ws}: Concatenates multiple input string columns together into a single
#' string column, using the given separator.
#'
#' @param sep separator to use.
#' @rdname column_string_functions
#' @aliases concat_ws concat_ws,character,Column-method
#' @examples
#'
#' \dontrun{
#' # concatenate strings
#' tmp <- mutate(df, s1 = concat_ws("_", df$Class, df$Sex),
#'                   s2 = concat_ws("+", df$Class, df$Sex, df$Age, df$Survived))
#' head(tmp)}
#' @note concat_ws since 1.5.0
setMethod("concat_ws", signature(sep = "character", x = "Column"),
          function(sep, x, ...) {
            jcols <- lapply(list(x, ...), function(x) { x@jc })
            jc <- callJStatic("org.apache.spark.sql.functions", "concat_ws", sep, jcols)
            column(jc)
          })

#' @details
#' \code{conv}: Converts a number in a string column from one base to another.
#'
#' @param fromBase base to convert from.
#' @param toBase base to convert to.
#' @rdname column_math_functions
#' @aliases conv conv,Column,numeric,numeric-method
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

#' @details
#' \code{expr}: Parses the expression string into the column that it represents, similar to
#' \code{SparkDataFrame.selectExpr}
#'
#' @rdname column_nonaggregate_functions
#' @aliases expr expr,character-method
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
#' \code{from_unixtime}: Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC)
#' to a string representing the timestamp of that moment in the current system time zone in the JVM
#' in the given format.
#' See \href{https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html}{
#' Datetime Pattern} for available options.
#'
#' @rdname column_datetime_functions
#'
#' @aliases from_unixtime from_unixtime,Column-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, to_unix = unix_timestamp(df$time),
#'                   to_unix2 = unix_timestamp(df$time, 'yyyy-MM-dd HH'),
#'                   from_unix = from_unixtime(unix_timestamp(df$time)),
#'                   from_unix2 = from_unixtime(unix_timestamp(df$time), 'yyyy-MM-dd HH:mm'),
#'                   timestamp_from_unix = timestamp_seconds(unix_timestamp(df$time)))
#' head(tmp)}
#' @note from_unixtime since 1.5.0
setMethod("from_unixtime", signature(x = "Column"),
          function(x, format = "yyyy-MM-dd HH:mm:ss") {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "from_unixtime",
                              x@jc, format)
            column(jc)
          })

#' @details
#' \code{window}: Bucketizes rows into one or more time windows given a timestamp specifying column.
#' Window starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
#' [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
#' the order of months are not supported. It returns an output column of struct called 'window'
#' by default with the nested columns 'start' and 'end'
#'
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
#' @rdname column_datetime_functions
#' @aliases window window,Column-method
#' @examples
#'
#' \dontrun{
#' # One minute windows every 15 seconds 10 seconds after the minute, e.g. 09:00:10-09:01:10,
#' # 09:00:25-09:01:25, 09:00:40-09:01:40, ...
#' window(df$time, "1 minute", "15 seconds", "10 seconds")
#'
#' # One minute tumbling windows 15 seconds after the minute, e.g. 09:00:15-09:01:15,
#' # 09:01:15-09:02:15...
#' window(df$time, "1 minute", startTime = "15 seconds")
#'
#' # Thirty-second windows every 10 seconds, e.g. 09:00:00-09:00:30, 09:00:10-09:00:40, ...
#' window(df$time, "30 seconds", "10 seconds")}
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
#' @rdname column_string_functions
#' @aliases locate locate,character,Column-method
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
#' @param pad a character string to be padded with.
#' @rdname column_string_functions
#' @aliases lpad lpad,Column,numeric,character-method
#' @note lpad since 1.5.0
setMethod("lpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "lpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' @details
#' \code{rand}: Generates a random column with independent and identically distributed (i.i.d.)
#' samples uniformly distributed in [0.0, 1.0).
#' Note: the function is non-deterministic in general case.
#'
#' @rdname column_nonaggregate_functions
#' @param seed a random seed. Can be missing.
#' @aliases rand rand,missing-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, r1 = rand(), r2 = rand(10), r3 = randn(), r4 = randn(10))
#' head(tmp)}
#' @note rand since 1.5.0
setMethod("rand", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand")
            column(jc)
          })

#' @rdname column_nonaggregate_functions
#' @aliases rand,numeric-method
#' @note rand(numeric) since 1.5.0
setMethod("rand", signature(seed = "numeric"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "rand", as.integer(seed))
            column(jc)
          })

#' @details
#' \code{randn}: Generates a column with independent and identically distributed (i.i.d.) samples
#' from the standard normal distribution.
#' Note: the function is non-deterministic in general case.
#'
#' @rdname column_nonaggregate_functions
#' @aliases randn randn,missing-method
#' @note randn since 1.5.0
setMethod("randn", signature(seed = "missing"),
          function(seed) {
            jc <- callJStatic("org.apache.spark.sql.functions", "randn")
            column(jc)
          })

#' @rdname column_nonaggregate_functions
#' @aliases randn,numeric-method
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
#' @note rpad since 1.5.0
setMethod("rpad", signature(x = "Column", len = "numeric", pad = "character"),
          function(x, len, pad) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "rpad",
                              x@jc, as.integer(len), pad)
            column(jc)
          })

#' @details
#' \code{sec}: Returns the secant of the given value.
#'
#' @rdname column_math_functions
#' @aliases sec sec,Column-method
#' @note sec since 3.3.0
setMethod("sec",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sec", x@jc)
            column(jc)
          })

#' @details
#' \code{substring_index}: Returns the substring from string (\code{x}) before \code{count}
#' occurrences of the delimiter (\code{delim}). If \code{count} is positive, everything the left of
#' the final delimiter (counting from left) is returned. If \code{count} is negative, every to the
#' right of the final delimiter (counting from the right) is returned. \code{substring_index}
#' performs a case-sensitive match when searching for the delimiter.
#'
#' @param delim a delimiter string.
#' @param count number of occurrences of \code{delim} before the substring is returned.
#'              A positive number means counting from the left, while negative means
#'              counting from the right.
#' @rdname column_string_functions
#' @aliases substring_index substring_index,Column,character,numeric-method
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
#' @note unix_timestamp since 1.5.0
setMethod("unix_timestamp", signature(x = "missing", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp")
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases unix_timestamp,Column,missing-method
#' @note unix_timestamp(Column) since 1.5.0
setMethod("unix_timestamp", signature(x = "Column", format = "missing"),
          function(x, format) {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp", x@jc)
            column(jc)
          })

#' @rdname column_datetime_functions
#' @aliases unix_timestamp,Column,character-method
#' @note unix_timestamp(Column, character) since 1.5.0
setMethod("unix_timestamp", signature(x = "Column", format = "character"),
          function(x, format = "yyyy-MM-dd HH:mm:ss") {
            jc <- callJStatic("org.apache.spark.sql.functions", "unix_timestamp", x@jc, format)
            column(jc)
          })

#' @details
#' \code{when}: Evaluates a list of conditions and returns one of multiple possible result
#' expressions. For unmatched expressions null is returned.
#'
#' @rdname column_nonaggregate_functions
#' @param condition the condition to test on. Must be a Column expression.
#' @param value result expression.
#' @aliases when when,Column-method
#' @examples
#'
#' \dontrun{
#' tmp <- mutate(df, mpg_na = otherwise(when(df$mpg > 20, df$mpg), lit(NaN)),
#'                   mpg2 = ifelse(df$mpg > 20 & df$am > 0, 0, 1),
#'                   mpg3 = ifelse(df$mpg > 20, df$mpg, 20.0))
#' head(tmp)
#' tmp <- mutate(tmp, ind_na1 = is.nan(tmp$mpg_na), ind_na2 = isnan(tmp$mpg_na))
#' head(select(tmp, coalesce(tmp$mpg_na, tmp$mpg)))
#' head(select(tmp, nanvl(tmp$mpg_na, tmp$hp)))}
#' @note when since 1.5.0
setMethod("when", signature(condition = "Column", value = "ANY"),
          function(condition, value) {
              condition <- condition@jc
              value <- if (inherits(value, "Column")) { value@jc } else { value }
              jc <- callJStatic("org.apache.spark.sql.functions", "when", condition, value)
              column(jc)
          })

#' @details
#' \code{ifelse}: Evaluates a list of conditions and returns \code{yes} if the conditions are
#' satisfied. Otherwise \code{no} is returned for unmatched conditions.
#'
#' @rdname column_nonaggregate_functions
#' @param test a Column expression that describes the condition.
#' @param yes return values for \code{TRUE} elements of test.
#' @param no return values for \code{FALSE} elements of test.
#' @aliases ifelse ifelse,Column-method
#' @note ifelse since 1.5.0
setMethod("ifelse",
          signature(test = "Column", yes = "ANY", no = "ANY"),
          function(test, yes, no) {
              test <- test@jc
              yes <- if (inherits(yes, "Column")) { yes@jc } else { yes }
              no <- if (inherits(no, "Column")) { no@jc } else { no }
              jc <- callJMethod(callJStatic("org.apache.spark.sql.functions",
                                            "when",
                                            test, yes),
                                "otherwise", no)
              column(jc)
          })

###################### Window functions######################

#' @details
#' \code{cume_dist}: Returns the cumulative distribution of values within a window partition,
#' i.e. the fraction of rows that are below the current row:
#' (number of values before and including x) / (total number of rows in the partition).
#' This is equivalent to the \code{CUME_DIST} function in SQL.
#' The method should be used with no argument.
#'
#' @rdname column_window_functions
#' @aliases cume_dist cume_dist,missing-method
#' @note cume_dist since 1.6.0
setMethod("cume_dist",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "cume_dist")
            column(jc)
          })

#' @details
#' \code{dense_rank}: Returns the rank of rows within a window partition, without any gaps.
#' The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
#' sequence when there are ties. That is, if you were ranking a competition using dense_rank
#' and had three people tie for second place, you would say that all three were in second
#' place and that the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as coming in fifth.
#' This is equivalent to the \code{DENSE_RANK} function in SQL.
#' The method should be used with no argument.
#'
#' @rdname column_window_functions
#' @aliases dense_rank dense_rank,missing-method
#' @note dense_rank since 1.6.0
setMethod("dense_rank",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "dense_rank")
            column(jc)
          })

#' @details
#' \code{lag}: Returns the value that is \code{offset} rows before the current row, and
#' \code{defaultValue} if there is less than \code{offset} rows before the current row. For example,
#' an \code{offset} of one will return the previous row at any given point in the window partition.
#' This is equivalent to the \code{LAG} function in SQL.
#'
#' @rdname column_window_functions
#' @aliases lag lag,characterOrColumn-method
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

#' @details
#' \code{lead}: Returns the value that is \code{offset} rows after the current row, and
#' \code{defaultValue} if there is less than \code{offset} rows after the current row.
#' For example, an \code{offset} of one will return the next row at any given point
#' in the window partition.
#' This is equivalent to the \code{LEAD} function in SQL.
#'
#' @rdname column_window_functions
#' @aliases lead lead,characterOrColumn,numeric-method
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

#' @details
#' \code{nth_value}: Window function: returns the value that is the \code{offset}th
#' row of the window frame# (counting from 1), and \code{null} if the size of window
#' frame is less than \code{offset} rows.
#'
#' @param offset a numeric indicating number of row to use as the value
#' @param na.rm a logical which indicates that the Nth value should skip null in the
#'        determination of which row to use
#'
#' @rdname column_window_functions
#' @aliases nth_value nth_value,characterOrColumn-method
#' @note nth_value since 3.1.0
setMethod("nth_value",
          signature(x = "characterOrColumn", offset = "numeric"),
          function(x, offset, na.rm = FALSE) {
            x <- if (is.character(x)) {
              column(x)
            } else {
              x
            }
            offset <- as.integer(offset)
            jc <- callJStatic(
              "org.apache.spark.sql.functions",
              "nth_value",
              x@jc,
              offset,
              na.rm
            )
            column(jc)
          })

#' @details
#' \code{ntile}: Returns the ntile group id (from 1 to n inclusive) in an ordered window
#' partition. For example, if n is 4, the first quarter of the rows will get value 1, the second
#' quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
#' This is equivalent to the \code{NTILE} function in SQL.
#'
#' @rdname column_window_functions
#' @aliases ntile ntile,numeric-method
#' @note ntile since 1.6.0
setMethod("ntile",
          signature(x = "numeric"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "ntile", as.integer(x))
            column(jc)
          })

#' @details
#' \code{percent_rank}: Returns the relative rank (i.e. percentile) of rows within a window
#' partition.
#' This is computed by: (rank of row in its partition - 1) / (number of rows in the partition - 1).
#' This is equivalent to the \code{PERCENT_RANK} function in SQL.
#' The method should be used with no argument.
#'
#' @rdname column_window_functions
#' @aliases percent_rank percent_rank,missing-method
#' @note percent_rank since 1.6.0
setMethod("percent_rank",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "percent_rank")
            column(jc)
          })

#' @details
#' \code{rank}: Returns the rank of rows within a window partition.
#' The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
#' sequence when there are ties. That is, if you were ranking a competition using dense_rank
#' and had three people tie for second place, you would say that all three were in second
#' place and that the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as coming in fifth.
#' This is equivalent to the \code{RANK} function in SQL.
#' The method should be used with no argument.
#'
#' @rdname column_window_functions
#' @aliases rank rank,missing-method
#' @note rank since 1.6.0
setMethod("rank",
          signature(x = "missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "rank")
            column(jc)
          })

#' @rdname column_window_functions
#' @aliases rank,ANY-method
setMethod("rank",
          signature(x = "ANY"),
          function(x, ...) {
            base::rank(x, ...)
          })

#' @details
#' \code{row_number}: Returns a sequential number starting at 1 within a window partition.
#' This is equivalent to the \code{ROW_NUMBER} function in SQL.
#' The method should be used with no argument.
#'
#' @rdname column_window_functions
#' @aliases row_number row_number,missing-method
#' @note row_number since 1.6.0
setMethod("row_number",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "row_number")
            column(jc)
          })

###################### Collection functions######################

#' Create o.a.s.sql.expressions.UnresolvedNamedLambdaVariable,
#' convert it to o.s.sql.Column and wrap with R Column.
#' Used by higher order functions.
#'
#' @param ... character of length = 1
#'        if length(...) > 1 then argument is interpreted as a nested
#'        Column, for example \code{unresolved_named_lambda_var("a", "b", "c")}
#'        yields unresolved \code{a.b.c}
#' @return Column object wrapping JVM UnresolvedNamedLambdaVariable
#' @keywords internal
unresolved_named_lambda_var <- function(...) {
  jc <- newJObject(
    "org.apache.spark.sql.Column",
    newJObject(
      "org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable",
      lapply(list(...), function(x) {
        handledCallJStatic(
          "org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable",
          "freshVarName",
          x)
      })
    )
  )
  column(jc)
}

#' Create o.a.s.sql.expressions.LambdaFunction corresponding
#' to transformation described by func.
#' Used by higher order functions.
#'
#' @param fun R \code{function} (unary, binary or ternary)
#'        that transforms \code{Columns} into a \code{Column}
#' @return JVM \code{LambdaFunction} object
#' @keywords internal
create_lambda <- function(fun) {
  as_jexpr <- function(x) callJMethod(x@jc, "expr")

  # Process function arguments
  parameters <- formals(fun)
  nparameters <- length(parameters)

  stopifnot(
    nparameters >= 1 &
    nparameters <= 3 &
    !"..." %in% names(parameters)
  )

  args <- lapply(c("x", "y", "z")[seq_along(parameters)], function(p) {
      unresolved_named_lambda_var(p)
  })

  # Invoke function and validate return type
  result <- do.call(fun, args)
  stopifnot(class(result) == "Column")

  # Convert both Columns to Scala expressions
  jexpr <- as_jexpr(result)

  jargs <- handledCallJStatic(
    "org.apache.spark.api.python.PythonUtils",
    "toSeq",
    handledCallJStatic(
      "java.util.Arrays", "asList", lapply(args, as_jexpr)
    )
  )

  # Create Scala LambdaFunction
  newJObject(
    "org.apache.spark.sql.catalyst.expressions.LambdaFunction",
    jexpr,
    jargs,
    FALSE
  )
}

#' Invokes higher order function expression identified by name,
#' (relative to o.a.s.sql.catalyst.expressions)
#'
#' @param name character
#' @param cols list of character or Column objects
#' @param funs list of named list(fun = ..., expected_narg = ...)
#' @return a \code{Column} representing name applied to cols with funs
#' @keywords internal
invoke_higher_order_function <- function(name, cols, funs) {
  as_jexpr <- function(x) {
    if (class(x) == "character") {
      x <- column(x)
    }
    callJMethod(x@jc, "expr")
  }

  jexpr <- do.call(newJObject, c(
    paste("org.apache.spark.sql.catalyst.expressions", name, sep = "."),
    lapply(cols, as_jexpr),
    lapply(funs, create_lambda)
  ))

  column(newJObject("org.apache.spark.sql.Column", jexpr))
}

#' @details
#' \code{array_aggregate}  Applies a binary operator to an initial state
#' and all elements in the array, and reduces this to a single state.
#' The final state is converted into the final result by applying
#' a finish function.
#'
#' @rdname column_collection_functions
#' @aliases array_aggregate array_aggregate,characterOrColumn,Column,function-method
#' @note array_aggregate since 3.1.0
setMethod("array_aggregate",
          signature(x = "characterOrColumn", initialValue = "Column", merge = "function"),
          function(x, initialValue, merge, finish = NULL) {
            invoke_higher_order_function(
              "ArrayAggregate",
              cols = list(x, initialValue),
              funs = if (is.null(finish)) {
                list(merge)
              } else {
                list(merge, finish)
              }
            )
          })

#' @details
#' \code{array_contains}: Returns null if the array is null, true if the array contains
#' the value, and false otherwise.
#'
#' @rdname column_collection_functions
#' @aliases array_contains array_contains,Column-method
#' @note array_contains since 1.6.0
setMethod("array_contains",
          signature(x = "Column", value = "ANY"),
          function(x, value) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_contains", x@jc, value)
            column(jc)
          })

#' @details
#' \code{array_distinct}: Removes duplicate values from the array.
#'
#' @rdname column_collection_functions
#' @aliases array_distinct array_distinct,Column-method
#' @note array_distinct since 2.4.0
setMethod("array_distinct",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_distinct", x@jc)
            column(jc)
          })

#' @details
#' \code{array_except}: Returns an array of the elements in the first array but not in the second
#'  array, without duplicates. The order of elements in the result is not determined.
#'
#' @rdname column_collection_functions
#' @aliases array_except array_except,Column-method
#' @note array_except since 2.4.0
setMethod("array_except",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_except", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{array_exists} Returns whether a predicate holds for one or more elements in the array.
#'
#' @rdname column_collection_functions
#' @aliases array_exists array_exists,characterOrColumn,function-method
#' @note array_exists since 3.1.0
setMethod("array_exists",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "ArrayExists",
              cols = list(x),
              funs = list(f)
            )
          })

#' @details
#' \code{array_filter} Returns an array of elements for which a predicate holds in a given array.
#'
#' @rdname column_collection_functions
#' @aliases array_filter array_filter,characterOrColumn,function-method
#' @note array_filter since 3.1.0
setMethod("array_filter",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "ArrayFilter",
              cols = list(x),
              funs = list(f)
            )
          })

#' @details
#' \code{array_forall} Returns whether a predicate holds for every element in the array.
#'
#' @rdname column_collection_functions
#' @aliases array_forall array_forall,characterOrColumn,function-method
#' @note array_forall since 3.1.0
setMethod("array_forall",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "ArrayForAll",
              cols = list(x),
              funs = list(f)
            )
          })

#' @details
#' \code{array_intersect}: Returns an array of the elements in the intersection of the given two
#'  arrays, without duplicates.
#'
#' @rdname column_collection_functions
#' @aliases array_intersect array_intersect,Column-method
#' @note array_intersect since 2.4.0
setMethod("array_intersect",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_intersect", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{array_join}: Concatenates the elements of column using the delimiter.
#' Null values are replaced with nullReplacement if set, otherwise they are ignored.
#'
#' @param delimiter a character string that is used to concatenate the elements of column.
#' @param nullReplacement an optional character string that is used to replace the Null values.
#' @rdname column_collection_functions
#' @aliases array_join array_join,Column-method
#' @note array_join since 2.4.0
setMethod("array_join",
         signature(x = "Column", delimiter = "character"),
         function(x, delimiter, nullReplacement = NULL) {
           jc <- if (is.null(nullReplacement)) {
             callJStatic("org.apache.spark.sql.functions", "array_join", x@jc, delimiter)
           } else {
             callJStatic("org.apache.spark.sql.functions", "array_join", x@jc, delimiter,
                         as.character(nullReplacement))
           }
           column(jc)
         })

#' @details
#' \code{array_max}: Returns the maximum value of the array.
#'
#' @rdname column_collection_functions
#' @aliases array_max array_max,Column-method
#' @note array_max since 2.4.0
setMethod("array_max",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_max", x@jc)
            column(jc)
          })

#' @details
#' \code{array_min}: Returns the minimum value of the array.
#'
#' @rdname column_collection_functions
#' @aliases array_min array_min,Column-method
#' @note array_min since 2.4.0
setMethod("array_min",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_min", x@jc)
            column(jc)
          })

#' @details
#' \code{array_position}: Locates the position of the first occurrence of the given value
#' in the given array. Returns NA if either of the arguments are NA.
#' Note: The position is not zero based, but 1 based index. Returns 0 if the given
#' value could not be found in the array.
#'
#' @rdname column_collection_functions
#' @aliases array_position array_position,Column-method
#' @note array_position since 2.4.0
setMethod("array_position",
          signature(x = "Column", value = "ANY"),
          function(x, value) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_position", x@jc, value)
            column(jc)
          })

#' @details
#' \code{array_remove}: Removes all elements that equal to element from the given array.
#'
#' @rdname column_collection_functions
#' @aliases array_remove array_remove,Column-method
#' @note array_remove since 2.4.0
setMethod("array_remove",
          signature(x = "Column", value = "ANY"),
          function(x, value) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_remove", x@jc, value)
            column(jc)
          })

#' @details
#' \code{array_repeat}: Creates an array containing \code{x} repeated the number of times
#' given by \code{count}.
#'
#' @param count a Column or constant determining the number of repetitions.
#' @rdname column_collection_functions
#' @aliases array_repeat array_repeat,Column,numericOrColumn-method
#' @note array_repeat since 2.4.0
setMethod("array_repeat",
          signature(x = "Column", count = "numericOrColumn"),
          function(x, count) {
            if (class(count) == "Column") {
              count <- count@jc
            } else {
              count <- as.integer(count)
            }
            jc <- callJStatic("org.apache.spark.sql.functions", "array_repeat", x@jc, count)
            column(jc)
          })

#' @details
#' \code{array_sort}: Sorts the input array in ascending order. The elements of the input array
#' must be orderable. NA elements will be placed at the end of the returned array.
#'
#' @rdname column_collection_functions
#' @aliases array_sort array_sort,Column-method
#' @note array_sort since 2.4.0
setMethod("array_sort",
          signature(x = "Column"),
          function(x, comparator = NULL) {
            if (is.null(comparator)) {
               column(callJStatic("org.apache.spark.sql.functions", "array_sort", x@jc))
            } else {
              invoke_higher_order_function(
                "ArraySort",
                cols = list(x),
                funs = list(comparator)
              )
            }
          })

#' @details
#' \code{array_transform}  Returns an array of elements after applying
#' a transformation to each element in the input array.
#'
#' @rdname column_collection_functions
#' @aliases array_transform array_transform,characterOrColumn,characterOrColumn,function-method
#' @note array_transform since 3.1.0
setMethod("array_transform",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "ArrayTransform",
              cols = list(x),
              funs = list(f)
            )
          })

#' @details
#' \code{arrays_overlap}: Returns true if the input arrays have at least one non-null element in
#' common. If not and both arrays are non-empty and any of them contains a null, it returns null.
#' It returns false otherwise.
#'
#' @rdname column_collection_functions
#' @aliases arrays_overlap arrays_overlap,Column-method
#' @note arrays_overlap since 2.4.0
setMethod("arrays_overlap",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "arrays_overlap", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{array_union}: Returns an array of the elements in the union of the given two arrays,
#'  without duplicates.
#'
#' @rdname column_collection_functions
#' @aliases array_union array_union,Column-method
#' @note array_union since 2.4.0
setMethod("array_union",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "array_union", x@jc, y@jc)
            column(jc)
          })

#' @details
#' \code{arrays_zip}: Returns a merged array of structs in which the N-th struct contains all N-th
#' values of input arrays.
#'
#' @rdname column_collection_functions
#' @aliases arrays_zip arrays_zip,Column-method
#' @note arrays_zip since 2.4.0
setMethod("arrays_zip",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(arg) {
              stopifnot(class(arg) == "Column")
              arg@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "arrays_zip", jcols)
            column(jc)
          })

#' @details
#' \code{arrays_zip_with} Merge two given arrays, element-wise, into a single array
#' using a function. If one array is shorter, nulls are appended at the end
#' to match the length of the longer array, before applying the function.
#'
#' @rdname column_collection_functions
#' @aliases arrays_zip_with arrays_zip_with,characterOrColumn,characterOrColumn,function-method
#' @note zip_with since 3.1.0
setMethod("arrays_zip_with",
          signature(x = "characterOrColumn", y = "characterOrColumn", f = "function"),
          function(x, y, f) {
            invoke_higher_order_function(
              "ZipWith",
              cols = list(x, y),
              funs = list(f)
            )
          })

#' @details
#' \code{shuffle}: Returns a random permutation of the given array.
#'
#' @rdname column_collection_functions
#' @aliases shuffle shuffle,Column-method
#' @note shuffle since 2.4.0
setMethod("shuffle",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "shuffle", x@jc)
            column(jc)
          })

#' @details
#' \code{flatten}: Creates a single array from an array of arrays.
#' If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.
#'
#' @rdname column_collection_functions
#' @aliases flatten flatten,Column-method
#' @note flatten since 2.4.0
setMethod("flatten",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "flatten", x@jc)
            column(jc)
          })

#' @details
#' \code{map_concat}: Returns the union of all the given maps.
#'
#' @rdname column_collection_functions
#' @aliases map_concat map_concat,Column-method
#' @note map_concat since 3.0.0
setMethod("map_concat",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(arg) {
              stopifnot(class(arg) == "Column")
              arg@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "map_concat", jcols)
            column(jc)
          })

#' @details
#' \code{map_entries}: Returns an unordered array of all entries in the given map.
#'
#' @rdname column_collection_functions
#' @aliases map_entries map_entries,Column-method
#' @note map_entries since 3.0.0
setMethod("map_entries",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "map_entries", x@jc)
            column(jc)
         })

#' @details
#' \code{map_filter} Returns a map whose key-value pairs satisfy a predicate.
#'
#' @rdname column_collection_functions
#' @aliases map_filter map_filter,characterOrColumn,function-method
#' @note map_filter since 3.1.0
setMethod("map_filter",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "MapFilter",
              cols = list(x),
              funs = list(f))
          })

#' @details
#' \code{map_from_arrays}: Creates a new map column. The array in the first column is used for
#' keys. The array in the second column is used for values. All elements in the array for key
#' should not be null.
#'
#' @rdname column_collection_functions
#' @aliases map_from_arrays map_from_arrays,Column-method
#' @note map_from_arrays since 2.4.0
setMethod("map_from_arrays",
          signature(x = "Column", y = "Column"),
          function(x, y) {
            jc <- callJStatic("org.apache.spark.sql.functions", "map_from_arrays", x@jc, y@jc)
            column(jc)
         })

#' @details
#' \code{map_from_entries}: Returns a map created from the given array of entries.
#'
#' @rdname column_collection_functions
#' @aliases map_from_entries map_from_entries,Column-method
#' @note map_from_entries since 3.0.0
setMethod("map_from_entries",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "map_from_entries", x@jc)
            column(jc)
         })

#' @details
#' \code{map_keys}: Returns an unordered array containing the keys of the map.
#'
#' @rdname column_collection_functions
#' @aliases map_keys map_keys,Column-method
#' @note map_keys since 2.3.0
setMethod("map_keys",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "map_keys", x@jc)
            column(jc)
         })

#' @details
#' \code{transform_keys} Applies a function to every key-value pair in a map and returns
#' a map with the results of those applications as the new keys for the pairs.
#'
#' @rdname column_collection_functions
#' @aliases transform_keys transform_keys,characterOrColumn,function-method
#' @note transform_keys since 3.1.0
setMethod("transform_keys",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "TransformKeys",
              cols = list(x),
              funs = list(f)
            )
          })

#' @details
#' \code{transform_values}    Applies a function to every key-value pair in a map and returns
#' a map with the results of those applications as the new values for the pairs.
#'
#' @rdname column_collection_functions
#' @aliases transform_values transform_values,characterOrColumn,function-method
#' @note transform_values since 3.1.0
setMethod("transform_values",
          signature(x = "characterOrColumn", f = "function"),
          function(x, f) {
            invoke_higher_order_function(
              "TransformValues",
              cols = list(x),
              funs = list(f)
           )
          })


#' @details
#' \code{map_values}: Returns an unordered array containing the values of the map.
#'
#' @rdname column_collection_functions
#' @aliases map_values map_values,Column-method
#' @note map_values since 2.3.0
setMethod("map_values",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "map_values", x@jc)
            column(jc)
          })

#' @details
#' \code{map_zip} Merge two given maps, key-wise into a single map using a function.
#'
#' @rdname column_collection_functions
#' @aliases map_zip_with map_zip_with,characterOrColumn,characterOrColumn,function-method
#'
#' @note map_zip_with since 3.1.0
setMethod("map_zip_with",
          signature(x = "characterOrColumn", y = "characterOrColumn", f = "function"),
          function(x, y, f) {
            invoke_higher_order_function(
              "MapZipWith",
              cols = list(x, y),
              funs = list(f)
           )
          })

#' @details
#' \code{element_at}: Returns element of array at given index in \code{extraction} if
#' \code{x} is array. Returns value for the given key in \code{extraction} if \code{x} is map.
#' Note: The position is not zero based, but 1 based index.
#'
#' @param extraction index to check for in array or key to check for in map
#' @rdname column_collection_functions
#' @aliases element_at element_at,Column-method
#' @note element_at since 2.4.0
setMethod("element_at",
          signature(x = "Column", extraction = "ANY"),
          function(x, extraction) {
            jc <- callJStatic("org.apache.spark.sql.functions", "element_at", x@jc, extraction)
            column(jc)
          })

#' @details
#' \code{explode}: Creates a new row for each element in the given array or map column.
#' Uses the default column name \code{col} for elements in the array and
#' \code{key} and \code{value} for elements in the map unless specified otherwise.
#'
#' @rdname column_collection_functions
#' @aliases explode explode,Column-method
#' @note explode since 1.5.0
setMethod("explode",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "explode", x@jc)
            column(jc)
          })

#' @details
#' \code{size}: Returns length of array or map.
#'
#' @rdname column_collection_functions
#' @aliases size size,Column-method
#' @note size since 1.5.0
setMethod("size",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "size", x@jc)
            column(jc)
          })

#' @details
#' \code{slice}: Returns an array containing all the elements in x from the index start
#' (array indices start at 1, or from the end if start is negative) with the specified length.
#'
#' @rdname column_collection_functions
#' @param start the starting index
#' @param length the length of the slice
#' @aliases slice slice,Column-method
#' @note slice since 2.4.0
setMethod("slice",
          signature(x = "Column"),
          function(x, start, length) {
            jc <- callJStatic("org.apache.spark.sql.functions", "slice", x@jc, start, length)
            column(jc)
          })

#' @details
#' \code{sort_array}: Sorts the input array in ascending or descending order according to
#' the natural ordering of the array elements. NA elements will be placed at the beginning of
#' the returned array in ascending order or at the end of the returned array in descending order.
#'
#' @rdname column_collection_functions
#' @param asc a logical flag indicating the sorting order.
#'            TRUE, sorting is in ascending order.
#'            FALSE, sorting is in descending order.
#' @aliases sort_array sort_array,Column-method
#' @note sort_array since 1.6.0
setMethod("sort_array",
          signature(x = "Column"),
          function(x, asc = TRUE) {
            jc <- callJStatic("org.apache.spark.sql.functions", "sort_array", x@jc, asc)
            column(jc)
          })

#' @details
#' \code{posexplode}: Creates a new row for each element with position in the given array
#' or map column. Uses the default column name \code{pos} for position, and \code{col}
#' for elements in the array and \code{key} and \code{value} for elements in the map
#' unless specified otherwise.
#'
#' @rdname column_collection_functions
#' @aliases posexplode posexplode,Column-method
#' @note posexplode since 2.1.0
setMethod("posexplode",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "posexplode", x@jc)
            column(jc)
          })

#' @details
#' \code{create_array}: Creates a new array column. The input columns must all have the same data
#' type.
#'
#' @rdname column_nonaggregate_functions
#' @aliases create_array create_array,Column-method
#' @note create_array since 2.3.0
setMethod("create_array",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "array", jcols)
            column(jc)
          })

#' @details
#' \code{create_map}: Creates a new map column. The input columns must be grouped as key-value
#' pairs, e.g. (key1, value1, key2, value2, ...).
#' The key columns must all have the same data type, and can't be null.
#' The value columns must all have the same data type.
#'
#' @rdname column_nonaggregate_functions
#' @aliases create_map create_map,Column-method
#' @note create_map since 2.3.0
setMethod("create_map",
          signature(x = "Column"),
          function(x, ...) {
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "map", jcols)
            column(jc)
          })

#' @details
#' \code{collect_list}: Creates a list of objects with duplicates.
#' Note: the function is non-deterministic because the order of collected results depends
#' on the order of the rows which may be non-deterministic after a shuffle.
#'
#' @rdname column_aggregate_functions
#' @aliases collect_list collect_list,Column-method
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
#' Note: the function is non-deterministic because the order of collected results depends
#' on the order of the rows which may be non-deterministic after a shuffle.
#'
#' @rdname column_aggregate_functions
#' @aliases collect_set collect_set,Column-method
#' @note collect_set since 2.3.0
setMethod("collect_set",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "collect_set", x@jc)
            column(jc)
          })

#' @details
#' \code{split_string}: Splits string on regular expression.
#' Equivalent to \code{split} SQL function. Optionally a
#' \code{limit} can be specified
#'
#' @rdname column_string_functions
#' @param limit determines the length of the returned array.
#'              \itemize{
#'              \item \code{limit > 0}: length of the array will be at most \code{limit}
#'              \item \code{limit <= 0}: the returned array can have any length
#'              }
#'
#' @aliases split_string split_string,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, split_string(df$Class, "\\d", 2)))
#' head(select(df, split_string(df$Sex, "a")))
#' head(select(df, split_string(df$Class, "\\d")))
#' # This is equivalent to the following SQL expression
#' head(selectExpr(df, "split(Class, '\\\\d')"))}
#' @note split_string 2.3.0
setMethod("split_string",
          signature(x = "Column", pattern = "character"),
          function(x, pattern, limit = -1) {
            jc <- callJStatic("org.apache.spark.sql.functions",
                              "split", x@jc, pattern, as.integer(limit))
            column(jc)
          })

#' @details
#' \code{repeat_string}: Repeats string n times.
#' Equivalent to \code{repeat} SQL function.
#'
#' @param n number of repetitions.
#' @rdname column_string_functions
#' @aliases repeat_string repeat_string,Column-method
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

#' @details
#' \code{explode}: Creates a new row for each element in the given array or map column.
#' Unlike \code{explode}, if the array/map is \code{null} or empty
#' then \code{null} is produced.
#' Uses the default column name \code{col} for elements in the array and
#' \code{key} and \code{value} for elements in the map unless specified otherwise.
#'
#' @rdname column_collection_functions
#' @aliases explode_outer explode_outer,Column-method
#' @examples
#'
#' \dontrun{
#' df2 <- createDataFrame(data.frame(
#'   id = c(1, 2, 3), text = c("a,b,c", NA, "d,e")
#' ))
#'
#' head(select(df2, df2$id, explode_outer(split_string(df2$text, ","))))
#' head(select(df2, df2$id, posexplode_outer(split_string(df2$text, ","))))}
#' @note explode_outer since 2.3.0
setMethod("explode_outer",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "explode_outer", x@jc)
            column(jc)
          })

#' @details
#' \code{posexplode_outer}: Creates a new row for each element with position in the given
#' array or map column. Unlike \code{posexplode}, if the array/map is \code{null} or empty
#' then the row (\code{null}, \code{null}) is produced.
#' Uses the default column name \code{pos} for position, and \code{col}
#' for elements in the array and \code{key} and \code{value} for elements in the map
#' unless specified otherwise.
#'
#' @rdname column_collection_functions
#' @aliases posexplode_outer posexplode_outer,Column-method
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
#' \code{grouping_bit}: Indicates whether a specified column in a GROUP BY list is aggregated or
#' not, returns 1 for aggregated or 0 for not aggregated in the result set. Same as \code{GROUPING}
#' in SQL and \code{grouping} function in Scala.
#'
#' @rdname column_aggregate_functions
#' @aliases grouping_bit grouping_bit,Column-method
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
#' }.
#'
#' @rdname column_aggregate_functions
#' @aliases grouping_id grouping_id,Column-method
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
            jcols <- lapply(list(x, ...), function(x) {
              stopifnot(class(x) == "Column")
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "grouping_id", jcols)
            column(jc)
          })

#' @details
#' \code{input_file_name}: Creates a string column with the input file name for a given row.
#' The method should be used with no argument.
#'
#' @rdname column_nonaggregate_functions
#' @aliases input_file_name input_file_name,missing-method
#' @examples
#'
#' \dontrun{
#' tmp <- read.text("README.md")
#' head(select(tmp, input_file_name()))}
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

#' @details
#' \code{date_trunc}: Returns timestamp truncated to the unit specified by the format.
#'
#' @rdname column_datetime_functions
#' @aliases date_trunc date_trunc,character,Column-method
#' @examples
#'
#' \dontrun{
#' head(select(df, df$time, date_trunc("hour", df$time), date_trunc("minute", df$time),
#'             date_trunc("week", df$time), date_trunc("quarter", df$time)))}
#' @note date_trunc since 2.3.0
setMethod("date_trunc",
          signature(format = "character", x = "Column"),
          function(format, x) {
            jc <- callJStatic("org.apache.spark.sql.functions", "date_trunc", format, x@jc)
            column(jc)
          })

#' @details
#' \code{current_date}: Returns the current date at the start of query evaluation as a date column.
#' All calls of current_date within the same query return the same value.
#'
#' @rdname column_datetime_functions
#' @aliases current_date current_date,missing-method
#' @examples
#' \dontrun{
#' head(select(df, current_date(), current_timestamp()))}
#' @note current_date since 2.3.0
setMethod("current_date",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "current_date")
            column(jc)
          })

#' @details
#' \code{current_timestamp}: Returns the current timestamp at the start of query evaluation as
#' a timestamp column. All calls of current_timestamp within the same query return the same value.
#'
#' @rdname column_datetime_functions
#' @aliases current_timestamp current_timestamp,missing-method
#' @note current_timestamp since 2.3.0
setMethod("current_timestamp",
          signature("missing"),
          function() {
            jc <- callJStatic("org.apache.spark.sql.functions", "current_timestamp")
            column(jc)
          })

#' @details
#' \code{timestamp_seconds}: Converts the number of seconds from the Unix epoch
#' (1970-01-01T00:00:00Z) to a timestamp.
#'
#' @rdname column_datetime_functions
#' @aliases timestamp_seconds timestamp_seconds,Column-method
#' @note timestamp_seconds since 3.1.0
setMethod("timestamp_seconds",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic(
              "org.apache.spark.sql.functions", "timestamp_seconds", x@jc
            )
            column(jc)
          })

#' @details
#' \code{array_to_vector} Converts a column of array of numeric type into
#' a column of dense vectors in MLlib
#'
#' @rdname column_ml_functions
#' @aliases array_to_vector array_to_vector,Column-method
#' @note array_to_vector since 3.1.0
setMethod("array_to_vector",
          signature(x = "Column"),
          function(x) {
            jc <- callJStatic(
              "org.apache.spark.ml.functions",
              "array_to_vector",
              x@jc
            )
            column(jc)
          })

#' @details
#' \code{vector_to_array} Converts a column of MLlib sparse/dense vectors into
#' a column of dense arrays.
#'
#' @param dtype The data type of the output array. Valid values: "float64" or "float32".
#'
#' @rdname column_ml_functions
#' @aliases vector_to_array vector_to_array,Column-method
#' @note vector_to_array since 3.1.0
setMethod("vector_to_array",
          signature(x = "Column"),
          function(x, dtype = c("float64", "float32")) {
            dtype <- match.arg(dtype)
            jc <- callJStatic(
              "org.apache.spark.ml.functions",
              "vector_to_array",
              x@jc,
              dtype
            )
            column(jc)
          })

#' @details
#' \code{from_avro} Converts a binary column of Avro format into its corresponding catalyst value.
#' The specified schema must match the read data, otherwise the behavior is undefined:
#' it may fail or return arbitrary result.
#' To deserialize the data with a compatible and evolved schema, the expected Avro schema can be
#' set via the option avroSchema.
#'
#' @rdname column_avro_functions
#' @aliases from_avro from_avro,Column-method
#' @note from_avro since 3.1.0
setMethod("from_avro",
          signature(x = "characterOrColumn"),
          function(x, jsonFormatSchema, ...) {
            x <- if (is.character(x)) {
              column(x)
            } else {
              x
            }

            options <- varargsToStrEnv(...)
            jc <- callJStatic(
              "org.apache.spark.sql.avro.functions", "from_avro",
              x@jc,
              jsonFormatSchema,
              options
            )
            column(jc)
          })

#' @details
#' \code{to_avro} Converts a column into binary of Avro format.
#'
#' @rdname column_avro_functions
#' @aliases to_avro to_avro,Column-method
#' @note to_avro since 3.1.0
setMethod("to_avro",
          signature(x = "characterOrColumn"),
          function(x, jsonFormatSchema = NULL) {
            x <- if (is.character(x)) {
              column(x)
            } else {
              x
            }

            jc <- if (is.null(jsonFormatSchema)) {
              callJStatic("org.apache.spark.sql.avro.functions", "to_avro", x@jc)
            } else {
              callJStatic(
                "org.apache.spark.sql.avro.functions",
                "to_avro",
                x@jc,
                jsonFormatSchema
              )
            }
            column(jc)
          })

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

# DataFrame.R - DataFrame class and methods implemented in S4 OO classes

#' @include generics.R jobj.R schema.R RDD.R pairRDD.R column.R group.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a DataFrame
#' @description DataFrames can be created using functions like
#'              \code{jsonFile}, \code{table} etc.
#' @rdname DataFrame
#' @seealso jsonFile, table
#'
#' @param env An R environment that stores bookkeeping states of the DataFrame
#' @param sdf A Java object reference to the backing Scala DataFrame
#' @export
setClass("DataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "DataFrame", function(.Object, sdf, isCached) {
  .Object@env <- new.env()
  .Object@env$isCached <- isCached

  .Object@sdf <- sdf
  .Object
})

#' @rdname DataFrame
#' @export
#'
#' @param sdf A Java object reference to the backing Scala DataFrame
#' @param isCached TRUE if the dataFrame is cached
dataFrame <- function(sdf, isCached = FALSE) {
  new("DataFrame", sdf, isCached)
}

############################ DataFrame Methods ##############################################

#' Print Schema of a DataFrame
#'
#' Prints out the schema in tree format
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname printSchema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' printSchema(df)
#'}
setMethod("printSchema",
          signature(x = "DataFrame"),
          function(x) {
            schemaString <- callJMethod(schema(x)$jobj, "treeString")
            cat(schemaString)
          })

#' Get schema object
#'
#' Returns the schema of this DataFrame as a structType object.
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname schema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' dfSchema <- schema(df)
#'}
setMethod("schema",
          signature(x = "DataFrame"),
          function(x) {
            structType(callJMethod(x@sdf, "schema"))
          })

#' Explain
#'
#' Print the logical and physical Catalyst plans to the console for debugging.
#'
#' @param x A SparkSQL DataFrame
#' @param extended Logical. If extended is False, explain() only prints the physical plan.
#' @rdname explain
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' explain(df, TRUE)
#'}
setMethod("explain",
          signature(x = "DataFrame"),
          function(x, extended = FALSE) {
            queryExec <- callJMethod(x@sdf, "queryExecution")
            if (extended) {
              cat(callJMethod(queryExec, "toString"))
            } else {
              execPlan <- callJMethod(queryExec, "executedPlan")
              cat(callJMethod(execPlan, "toString"))
            }
          })

#' isLocal
#'
#' Returns True if the `collect` and `take` methods can be run locally
#' (without any Spark executors).
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname isLocal
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' isLocal(df)
#'}
setMethod("isLocal",
          signature(x = "DataFrame"),
          function(x) {
            callJMethod(x@sdf, "isLocal")
          })

#' showDF
#'
#' Print the first numRows rows of a DataFrame
#'
#' @param x A SparkSQL DataFrame
#' @param numRows The number of rows to print. Defaults to 20.
#'
#' @rdname showDF
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' showDF(df)
#'}
setMethod("showDF",
          signature(x = "DataFrame"),
          function(x, numRows = 20) {
            s <- callJMethod(x@sdf, "showString", numToInt(numRows))
            cat(s)
          })

#' show
#'
#' Print the DataFrame column names and types
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname show
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' df
#'}
setMethod("show", "DataFrame",
          function(object) {
            cols <- lapply(dtypes(object), function(l) {
              paste(l, collapse = ":")
            })
            s <- paste(cols, collapse = ", ")
            cat(paste("DataFrame[", s, "]\n", sep = ""))
          })

#' DataTypes
#'
#' Return all column names and their data types as a list
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname dtypes
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' dtypes(df)
#'}
setMethod("dtypes",
          signature(x = "DataFrame"),
          function(x) {
            lapply(schema(x)$fields(), function(f) {
              c(f$name(), f$dataType.simpleString())
            })
          })

#' Column names
#'
#' Return all column names as a list
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname columns
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' columns(df)
#'}
setMethod("columns",
          signature(x = "DataFrame"),
          function(x) {
            sapply(schema(x)$fields(), function(f) {
              f$name()
            })
          })

#' @rdname columns
#' @aliases names,DataFrame,function-method
setMethod("names",
          signature(x = "DataFrame"),
          function(x) {
            columns(x)
          })

#' Register Temporary Table
#'
#' Registers a DataFrame as a Temporary Table in the SQLContext
#'
#' @param x A SparkSQL DataFrame
#' @param tableName A character vector containing the name of the table
#'
#' @rdname registerTempTable
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' registerTempTable(df, "json_df")
#' new_df <- sql(sqlContext, "SELECT * FROM json_df")
#'}
setMethod("registerTempTable",
          signature(x = "DataFrame", tableName = "character"),
          function(x, tableName) {
              invisible(callJMethod(x@sdf, "registerTempTable", tableName))
          })

#' insertInto
#'
#' Insert the contents of a DataFrame into a table registered in the current SQL Context.
#'
#' @param x A SparkSQL DataFrame
#' @param tableName A character vector containing the name of the table
#' @param overwrite A logical argument indicating whether or not to overwrite
#' the existing rows in the table.
#'
#' @rdname insertInto
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df <- read.df(sqlContext, path, "parquet")
#' df2 <- read.df(sqlContext, path2, "parquet")
#' registerTempTable(df, "table1")
#' insertInto(df2, "table1", overwrite = TRUE)
#'}
setMethod("insertInto",
          signature(x = "DataFrame", tableName = "character"),
          function(x, tableName, overwrite = FALSE) {
            callJMethod(x@sdf, "insertInto", tableName, overwrite)
          })

#' Cache
#'
#' Persist with the default storage level (MEMORY_ONLY).
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' cache(df)
#'}
setMethod("cache",
          signature(x = "DataFrame"),
          function(x) {
            cached <- callJMethod(x@sdf, "cache")
            x@env$isCached <- TRUE
            x
          })

#' Persist
#'
#' Persist this DataFrame with the specified storage level. For details of the
#' supported storage levels, refer to
#' http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence.
#'
#' @param x The DataFrame to persist
#' @rdname persist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' persist(df, "MEMORY_AND_DISK")
#'}
setMethod("persist",
          signature(x = "DataFrame", newLevel = "character"),
          function(x, newLevel) {
            callJMethod(x@sdf, "persist", getStorageLevel(newLevel))
            x@env$isCached <- TRUE
            x
          })

#' Unpersist
#'
#' Mark this DataFrame as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param x The DataFrame to unpersist
#' @param blocking Whether to block until all blocks are deleted
#' @rdname unpersist-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' persist(df, "MEMORY_AND_DISK")
#' unpersist(df)
#'}
setMethod("unpersist",
          signature(x = "DataFrame"),
          function(x, blocking = TRUE) {
            callJMethod(x@sdf, "unpersist", blocking)
            x@env$isCached <- FALSE
            x
          })

#' Repartition
#'
#' Return a new DataFrame that has exactly numPartitions partitions.
#'
#' @param x A SparkSQL DataFrame
#' @param numPartitions The number of partitions to use.
#' @rdname repartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' newDF <- repartition(df, 2L)
#'}
setMethod("repartition",
          signature(x = "DataFrame", numPartitions = "numeric"),
          function(x, numPartitions) {
            sdf <- callJMethod(x@sdf, "repartition", numToInt(numPartitions))
            dataFrame(sdf)
          })

# toJSON
#
# Convert the rows of a DataFrame into JSON objects and return an RDD where
# each element contains a JSON string.
#
#@param x A SparkSQL DataFrame
# @return A StringRRDD of JSON objects
# @rdname tojson
# @export
# @examples
#\dontrun{
# sc <- sparkR.init()
# sqlContext <- sparkRSQL.init(sc)
# path <- "path/to/file.json"
# df <- jsonFile(sqlContext, path)
# newRDD <- toJSON(df)
#}
setMethod("toJSON",
          signature(x = "DataFrame"),
          function(x) {
            rdd <- callJMethod(x@sdf, "toJSON")
            jrdd <- callJMethod(rdd, "toJavaRDD")
            RDD(jrdd, serializedMode = "string")
          })

#' saveAsParquetFile
#'
#' Save the contents of a DataFrame as a Parquet file, preserving the schema. Files written out
#' with this method can be read back in as a DataFrame using parquetFile().
#'
#' @param x A SparkSQL DataFrame
#' @param path The directory where the file is saved
#' @rdname saveAsParquetFile
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' saveAsParquetFile(df, "/tmp/sparkr-tmp/")
#'}
setMethod("saveAsParquetFile",
          signature(x = "DataFrame", path = "character"),
          function(x, path) {
            invisible(callJMethod(x@sdf, "saveAsParquetFile", path))
          })

#' Distinct
#'
#' Return a new DataFrame containing the distinct rows in this DataFrame.
#'
#' @param x A SparkSQL DataFrame
#' @rdname distinct
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' distinctDF <- distinct(df)
#'}
setMethod("distinct",
          signature(x = "DataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "distinct")
            dataFrame(sdf)
          })

#' Sample
#'
#' Return a sampled subset of this DataFrame using a random seed.
#'
#' @param x A SparkSQL DataFrame
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @rdname sample
#' @aliases sample_frac
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' collect(sample(df, FALSE, 0.5))
#' collect(sample(df, TRUE, 0.5))
#'}
setMethod("sample",
          # TODO : Figure out how to send integer as java.lang.Long to JVM so
          # we can send seed as an argument through callJMethod
          signature(x = "DataFrame", withReplacement = "logical",
                    fraction = "numeric"),
          function(x, withReplacement, fraction) {
            if (fraction < 0.0) stop(cat("Negative fraction value:", fraction))
            sdf <- callJMethod(x@sdf, "sample", withReplacement, fraction)
            dataFrame(sdf)
          })

#' @rdname sample
#' @aliases sample
setMethod("sample_frac",
          signature(x = "DataFrame", withReplacement = "logical",
                    fraction = "numeric"),
          function(x, withReplacement, fraction) {
            sample(x, withReplacement, fraction)
          })

#' Count
#'
#' Returns the number of rows in a DataFrame
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname count
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' count(df)
#' }
setMethod("count",
          signature(x = "DataFrame"),
          function(x) {
            callJMethod(x@sdf, "count")
          })

#' Collects all the elements of a Spark DataFrame and coerces them into an R data.frame.
#'
#' @param x A SparkSQL DataFrame
#' @param stringsAsFactors (Optional) A logical indicating whether or not string columns
#' should be converted to factors. FALSE by default.

#' @rdname collect-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' collected <- collect(df)
#' firstName <- collected[[1]]$name
#' }
setMethod("collect",
          signature(x = "DataFrame"),
          function(x, stringsAsFactors = FALSE) {
            # listCols is a list of raw vectors, one per column
            listCols <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "dfToCols", x@sdf)
            cols <- lapply(listCols, function(col) {
              objRaw <- rawConnection(col)
              numRows <- readInt(objRaw)
              col <- readCol(objRaw, numRows)
              close(objRaw)
              col
            })
            names(cols) <- columns(x)
            do.call(cbind.data.frame, list(cols, stringsAsFactors = stringsAsFactors))
          })

#' Limit
#'
#' Limit the resulting DataFrame to the number of rows specified.
#'
#' @param x A SparkSQL DataFrame
#' @param num The number of rows to return
#' @return A new DataFrame containing the number of rows specified.
#'
#' @rdname limit
#' @export
#' @examples
#' \dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' limitedDF <- limit(df, 10)
#' }
setMethod("limit",
          signature(x = "DataFrame", num = "numeric"),
          function(x, num) {
            res <- callJMethod(x@sdf, "limit", as.integer(num))
            dataFrame(res)
          })

#' Take the first NUM rows of a DataFrame and return a the results as a data.frame
#'
#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' take(df, 2)
#' }
setMethod("take",
          signature(x = "DataFrame", num = "numeric"),
          function(x, num) {
            limited <- limit(x, num)
            collect(limited)
          })

#' Head
#'
#' Return the first NUM rows of a DataFrame as a data.frame. If NUM is NULL,
#' then head() returns the first 6 rows in keeping with the current data.frame
#' convention in R.
#'
#' @param x A SparkSQL DataFrame
#' @param num The number of rows to return. Default is 6.
#' @return A data.frame
#'
#' @rdname head
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' head(df)
#' }
setMethod("head",
          signature(x = "DataFrame"),
          function(x, num = 6L) {
          # Default num is 6L in keeping with R's data.frame convention
            take(x, num)
          })

#' Return the first row of a DataFrame
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname first
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' first(df)
#' }
setMethod("first",
          signature(x = "DataFrame"),
          function(x) {
            take(x, 1)
          })

# toRDD()
#
# Converts a Spark DataFrame to an RDD while preserving column names.
#
# @param x A Spark DataFrame
#
# @rdname DataFrame
# @export
# @examples
#\dontrun{
# sc <- sparkR.init()
# sqlContext <- sparkRSQL.init(sc)
# path <- "path/to/file.json"
# df <- jsonFile(sqlContext, path)
# rdd <- toRDD(df)
# }
setMethod("toRDD",
          signature(x = "DataFrame"),
          function(x) {
            jrdd <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "dfToRowRDD", x@sdf)
            colNames <- callJMethod(x@sdf, "columns")
            rdd <- RDD(jrdd, serializedMode = "row")
            lapply(rdd, function(row) {
              names(row) <- colNames
              row
            })
          })

#' GroupBy
#'
#' Groups the DataFrame using the specified columns, so we can run aggregation on them.
#'
#' @param x a DataFrame
#' @return a GroupedData
#' @seealso GroupedData
#' @aliases group_by
#' @rdname groupBy
#' @export
#' @examples
#' \dontrun{
#'   # Compute the average for all numeric columns grouped by department.
#'   avg(groupBy(df, "department"))
#'
#'   # Compute the max age and average salary, grouped by department and gender.
#'   agg(groupBy(df, "department", "gender"), salary="avg", "age" -> "max")
#' }
setMethod("groupBy",
           signature(x = "DataFrame"),
           function(x, ...) {
             cols <- list(...)
             if (length(cols) >= 1 && class(cols[[1]]) == "character") {
               sgd <- callJMethod(x@sdf, "groupBy", cols[[1]], listToSeq(cols[-1]))
             } else {
               jcol <- lapply(cols, function(c) { c@jc })
               sgd <- callJMethod(x@sdf, "groupBy", listToSeq(jcol))
             }
             groupedData(sgd)
           })

#' @rdname groupBy
#' @aliases group_by
setMethod("group_by",
          signature(x = "DataFrame"),
          function(x, ...) {
            groupBy(x, ...)
          })

#' Summarize data across columns
#'
#' Compute aggregates by specifying a list of columns
#'
#' @param x a DataFrame
#' @rdname DataFrame
#' @aliases summarize
#' @export
setMethod("agg",
          signature(x = "DataFrame"),
          function(x, ...) {
            agg(groupBy(x), ...)
          })

#' @rdname DataFrame
#' @aliases agg
setMethod("summarize",
          signature(x = "DataFrame"),
          function(x, ...) {
            agg(x, ...)
          })


############################## RDD Map Functions ##################################
# All of the following functions mirror the existing RDD map functions,           #
# but allow for use with DataFrames by first converting to an RRDD before calling #
# the requested map function.                                                     #
###################################################################################

# @rdname lapply
setMethod("lapply",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapply(rdd, FUN)
          })

# @rdname lapply
setMethod("map",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

# @rdname flatMap
setMethod("flatMap",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            flatMap(rdd, FUN)
          })

# @rdname lapplyPartition
setMethod("lapplyPartition",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapplyPartition(rdd, FUN)
          })

# @rdname lapplyPartition
setMethod("mapPartitions",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

# @rdname foreach
setMethod("foreach",
          signature(x = "DataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreach(rdd, func)
          })

# @rdname foreach
setMethod("foreachPartition",
          signature(x = "DataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreachPartition(rdd, func)
          })


############################## SELECT ##################################

getColumn <- function(x, c) {
  column(callJMethod(x@sdf, "col", c))
}

#' @rdname select
setMethod("$", signature(x = "DataFrame"),
          function(x, name) {
            getColumn(x, name)
          })

#' @rdname select
setMethod("$<-", signature(x = "DataFrame"),
          function(x, name, value) {
            stopifnot(class(value) == "Column" || is.null(value))
            cols <- columns(x)
            if (name %in% cols) {
              if (is.null(value)) {
                cols <- Filter(function(c) { c != name }, cols)
              }
              cols <- lapply(cols, function(c) {
                if (c == name) {
                  alias(value, name)
                } else {
                  col(c)
                }
              })
              nx <- select(x, cols)
            } else {
              if (is.null(value)) {
                return(x)
              }
              nx <- withColumn(x, name, value)
            }
            x@sdf <- nx@sdf
            x
          })

#' @rdname select
setMethod("[[", signature(x = "DataFrame"),
          function(x, i) {
            if (is.numeric(i)) {
              cols <- columns(x)
              i <- cols[[i]]
            }
            getColumn(x, i)
          })

#' @rdname select
setMethod("[", signature(x = "DataFrame", i = "missing"),
          function(x, i, j, ...) {
            if (is.numeric(j)) {
              cols <- columns(x)
              j <- cols[j]
            }
            if (length(j) > 1) {
              j <- as.list(j)
            }
            select(x, j)
          })

#' Select
#'
#' Selects a set of columns with names or Column expressions.
#' @param x A DataFrame
#' @param col A list of columns or single Column or name
#' @return A new DataFrame with selected columns
#' @export
#' @rdname select
#' @examples
#' \dontrun{
#'   select(df, "*")
#'   select(df, "col1", "col2")
#'   select(df, df$name, df$age + 1)
#'   select(df, c("col1", "col2"))
#'   select(df, list(df$name, df$age + 1))
#'   # Columns can also be selected using `[[` and `[`
#'   df[[2]] == df[["age"]]
#'   df[,2] == df[,"age"]
#'   # Similar to R data frames columns can also be selected using `$`
#'   df$age
#' }
setMethod("select", signature(x = "DataFrame", col = "character"),
          function(x, col, ...) {
            sdf <- callJMethod(x@sdf, "select", col, toSeq(...))
            dataFrame(sdf)
          })

#' @rdname select
#' @export
setMethod("select", signature(x = "DataFrame", col = "Column"),
          function(x, col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            sdf <- callJMethod(x@sdf, "select", listToSeq(jcols))
            dataFrame(sdf)
          })

#' @rdname select
#' @export
setMethod("select",
          signature(x = "DataFrame", col = "list"),
          function(x, col) {
            cols <- lapply(col, function(c) {
              if (class(c) == "Column") {
                c@jc
              } else {
                col(c)@jc
              }
            })
            sdf <- callJMethod(x@sdf, "select", listToSeq(cols))
            dataFrame(sdf)
          })

#' SelectExpr
#'
#' Select from a DataFrame using a set of SQL expressions.
#'
#' @param x A DataFrame to be selected from.
#' @param expr A string containing a SQL expression
#' @param ... Additional expressions
#' @return A DataFrame
#' @rdname selectExpr
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' selectExpr(df, "col1", "(col2 * 5) as newCol")
#' }
setMethod("selectExpr",
          signature(x = "DataFrame", expr = "character"),
          function(x, expr, ...) {
            exprList <- list(expr, ...)
            sdf <- callJMethod(x@sdf, "selectExpr", listToSeq(exprList))
            dataFrame(sdf)
          })

#' WithColumn
#'
#' Return a new DataFrame with the specified column added.
#'
#' @param x A DataFrame
#' @param colName A string containing the name of the new column.
#' @param col A Column expression.
#' @return A DataFrame with the new column added.
#' @rdname withColumn
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' newDF <- withColumn(df, "newCol", df$col1 * 5)
#' }
setMethod("withColumn",
          signature(x = "DataFrame", colName = "character", col = "Column"),
          function(x, colName, col) {
            select(x, x$"*", alias(col, colName))
          })

#' Mutate
#'
#' Return a new DataFrame with the specified columns added.
#'
#' @param x A DataFrame
#' @param col a named argument of the form name = col
#' @return A new DataFrame with the new columns added.
#' @rdname withColumn
#' @aliases withColumn
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' newDF <- mutate(df, newCol = df$col1 * 5, newCol2 = df$col1 * 2)
#' names(newDF) # Will contain newCol, newCol2
#' }
setMethod("mutate",
          signature(x = "DataFrame"),
          function(x, ...) {
            cols <- list(...)
            stopifnot(length(cols) > 0)
            stopifnot(class(cols[[1]]) == "Column")
            ns <- names(cols)
            if (!is.null(ns)) {
              for (n in ns) {
                if (n != "") {
                  cols[[n]] <- alias(cols[[n]], n)
                }
              }
            }
            do.call(select, c(x, x$"*", cols))
          })

#' WithColumnRenamed
#'
#' Rename an existing column in a DataFrame.
#'
#' @param x A DataFrame
#' @param existingCol The name of the column you want to change.
#' @param newCol The new column name.
#' @return A DataFrame with the column name changed.
#' @rdname withColumnRenamed
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' newDF <- withColumnRenamed(df, "col1", "newCol1")
#' }
setMethod("withColumnRenamed",
          signature(x = "DataFrame", existingCol = "character", newCol = "character"),
          function(x, existingCol, newCol) {
            cols <- lapply(columns(x), function(c) {
              if (c == existingCol) {
                alias(col(c), newCol)
              } else {
                col(c)
              }
            })
            select(x, cols)
          })

#' Rename
#'
#' Rename an existing column in a DataFrame.
#'
#' @param x A DataFrame
#' @param newCol A named pair of the form new_column_name = existing_column
#' @return A DataFrame with the column name changed.
#' @rdname withColumnRenamed
#' @aliases withColumnRenamed
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' newDF <- rename(df, col1 = df$newCol1)
#' }
setMethod("rename",
          signature(x = "DataFrame"),
          function(x, ...) {
            renameCols <- list(...)
            stopifnot(length(renameCols) > 0)
            stopifnot(class(renameCols[[1]]) == "Column")
            newNames <- names(renameCols)
            oldNames <- lapply(renameCols, function(col) {
              callJMethod(col@jc, "toString")
            })
            cols <- lapply(columns(x), function(c) {
              if (c %in% oldNames) {
                alias(col(c), newNames[[match(c, oldNames)]])
              } else {
                col(c)
              }
            })
            select(x, cols)
          })

setClassUnion("characterOrColumn", c("character", "Column"))

#' Arrange
#'
#' Sort a DataFrame by the specified column(s).
#'
#' @param x A DataFrame to be sorted.
#' @param col Either a Column object or character vector indicating the field to sort on
#' @param ... Additional sorting fields
#' @return A DataFrame where all elements are sorted.
#' @rdname arrange
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' arrange(df, df$col1)
#' arrange(df, "col1")
#' arrange(df, asc(df$col1), desc(abs(df$col2)))
#' }
setMethod("arrange",
          signature(x = "DataFrame", col = "characterOrColumn"),
          function(x, col, ...) {
            if (class(col) == "character") {
              sdf <- callJMethod(x@sdf, "sort", col, toSeq(...))
            } else if (class(col) == "Column") {
              jcols <- lapply(list(col, ...), function(c) {
                c@jc
              })
              sdf <- callJMethod(x@sdf, "sort", listToSeq(jcols))
            }
            dataFrame(sdf)
          })

#' @rdname arrange
#' @aliases orderBy,DataFrame,function-method
setMethod("orderBy",
          signature(x = "DataFrame", col = "characterOrColumn"),
          function(x, col) {
            arrange(x, col)
          })

#' Filter
#'
#' Filter the rows of a DataFrame according to a given condition.
#'
#' @param x A DataFrame to be sorted.
#' @param condition The condition to filter on. This may either be a Column expression
#' or a string containing a SQL statement
#' @return A DataFrame containing only the rows that meet the condition.
#' @rdname filter
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' filter(df, "col1 > 0")
#' filter(df, df$col2 != "abcdefg")
#' }
setMethod("filter",
          signature(x = "DataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            if (class(condition) == "Column") {
              condition <- condition@jc
            }
            sdf <- callJMethod(x@sdf, "filter", condition)
            dataFrame(sdf)
          })

#' @rdname filter
#' @aliases where,DataFrame,function-method
setMethod("where",
          signature(x = "DataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            filter(x, condition)
          })

#' Join
#'
#' Join two DataFrames based on the given join expression.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @param joinExpr (Optional) The expression used to perform the join. joinExpr must be a
#' Column expression. If joinExpr is omitted, join() wil perform a Cartesian join
#' @param joinType The type of join to perform. The following join types are available:
#' 'inner', 'outer', 'left_outer', 'right_outer', 'semijoin'. The default joinType is "inner".
#' @return A DataFrame containing the result of the join operation.
#' @rdname join
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- jsonFile(sqlContext, path)
#' df2 <- jsonFile(sqlContext, path2)
#' join(df1, df2) # Performs a Cartesian
#' join(df1, df2, df1$col1 == df2$col2) # Performs an inner join based on expression
#' join(df1, df2, df1$col1 == df2$col2, "right_outer")
#' }
setMethod("join",
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y, joinExpr = NULL, joinType = NULL) {
            if (is.null(joinExpr)) {
              sdf <- callJMethod(x@sdf, "join", y@sdf)
            } else {
              if (class(joinExpr) != "Column") stop("joinExpr must be a Column")
              if (is.null(joinType)) {
                sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc)
              } else {
                if (joinType %in% c("inner", "outer", "left_outer", "right_outer", "semijoin")) {
                  sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc, joinType)
                } else {
                  stop("joinType must be one of the following types: ",
                       "'inner', 'outer', 'left_outer', 'right_outer', 'semijoin'")
                }
              }
            }
            dataFrame(sdf)
          })

#' UnionAll
#'
#' Return a new DataFrame containing the union of rows in this DataFrame
#' and another DataFrame. This is equivalent to `UNION ALL` in SQL.
#' Note that this does not remove duplicate rows across the two DataFrames.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @return A DataFrame containing the result of the union.
#' @rdname unionAll
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- jsonFile(sqlContext, path)
#' df2 <- jsonFile(sqlContext, path2)
#' unioned <- unionAll(df, df2)
#' }
setMethod("unionAll",
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y) {
            unioned <- callJMethod(x@sdf, "unionAll", y@sdf)
            dataFrame(unioned)
          })

#' Intersect
#'
#' Return a new DataFrame containing rows only in both this DataFrame
#' and another DataFrame. This is equivalent to `INTERSECT` in SQL.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @return A DataFrame containing the result of the intersect.
#' @rdname intersect
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- jsonFile(sqlContext, path)
#' df2 <- jsonFile(sqlContext, path2)
#' intersectDF <- intersect(df, df2)
#' }
setMethod("intersect",
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y) {
            intersected <- callJMethod(x@sdf, "intersect", y@sdf)
            dataFrame(intersected)
          })

#' except
#'
#' Return a new DataFrame containing rows in this DataFrame
#' but not in another DataFrame. This is equivalent to `EXCEPT` in SQL.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @return A DataFrame containing the result of the except operation.
#' @rdname except
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- jsonFile(sqlContext, path)
#' df2 <- jsonFile(sqlContext, path2)
#' exceptDF <- except(df, df2)
#' }
#' @rdname except
#' @export
setMethod("except",
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y) {
            excepted <- callJMethod(x@sdf, "except", y@sdf)
            dataFrame(excepted)
          })

#' Save the contents of the DataFrame to a data source
#'
#' The data source is specified by the `source` and a set of options (...).
#' If `source` is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes:
#'  append: Contents of this DataFrame are expected to be appended to existing data.
#'  overwrite: Existing data is expected to be overwritten by the contents of
#     this DataFrame.
#'  error: An exception is expected to be thrown.
#'  ignore: The save operation is expected to not save the contents of the DataFrame
#     and to not change the existing data.
#'
#' @param df A SparkSQL DataFrame
#' @param path A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore'
#'
#' @rdname write.df
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' write.df(df, "myfile", "parquet", "overwrite")
#' }
setMethod("write.df",
          signature(df = "DataFrame", path = 'character'),
          function(df, path, source = NULL, mode = "append", ...){
            if (is.null(source)) {
              sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                    "org.apache.spark.sql.parquet")
            }
            allModes <- c("append", "overwrite", "error", "ignore")
            if (!(mode %in% allModes)) {
              stop('mode should be one of "append", "overwrite", "error", "ignore"')
            }
            jmode <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "saveMode", mode)
            options <- varargsToEnv(...)
            if (!is.null(path)) {
                options[['path']] = path
            }
            callJMethod(df@sdf, "save", source, jmode, options)
          })

#' @rdname write.df
#' @aliases saveDF
#' @export
setMethod("saveDF",
          signature(df = "DataFrame", path = 'character'),
          function(df, path, source = NULL, mode = "append", ...){
            write.df(df, path, source, mode, ...)
          })

#' saveAsTable
#'
#' Save the contents of the DataFrame to a data source as a table
#'
#' The data source is specified by the `source` and a set of options (...).
#' If `source` is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes:
#'  append: Contents of this DataFrame are expected to be appended to existing data.
#'  overwrite: Existing data is expected to be overwritten by the contents of
#     this DataFrame.
#'  error: An exception is expected to be thrown.
#'  ignore: The save operation is expected to not save the contents of the DataFrame
#     and to not change the existing data.
#'
#' @param df A SparkSQL DataFrame
#' @param tableName A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore'
#'
#' @rdname saveAsTable
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' saveAsTable(df, "myfile")
#' }
setMethod("saveAsTable",
          signature(df = "DataFrame", tableName = 'character', source = 'character',
                    mode = 'character'),
          function(df, tableName, source = NULL, mode="append", ...){
            if (is.null(source)) {
              sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                    "org.apache.spark.sql.parquet")
            }
            allModes <- c("append", "overwrite", "error", "ignore")
            if (!(mode %in% allModes)) {
              stop('mode should be one of "append", "overwrite", "error", "ignore"')
            }
            jmode <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "saveMode", mode)
            options <- varargsToEnv(...)
            callJMethod(df@sdf, "saveAsTable", tableName, source, jmode, options)
          })

#' describe
#'
#' Computes statistics for numeric columns.
#' If no columns are given, this function computes statistics for all numerical columns.
#'
#' @param x A DataFrame to be computed.
#' @param col A string of name
#' @param ... Additional expressions
#' @return A DataFrame
#' @rdname describe
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlContext, path)
#' describe(df)
#' describe(df, "col1")
#' describe(df, "col1", "col2")
#' }
setMethod("describe",
          signature(x = "DataFrame", col = "character"),
          function(x, col, ...) {
            colList <- list(col, ...)
            sdf <- callJMethod(x@sdf, "describe", listToSeq(colList))
            dataFrame(sdf)
          })

#' @rdname describe
setMethod("describe",
          signature(x = "DataFrame"),
          function(x) {
            colList <- as.list(c(columns(x)))
            sdf <- callJMethod(x@sdf, "describe", listToSeq(colList))
            dataFrame(sdf)
          })

#' dropna
#'
#' Returns a new DataFrame omitting rows with null values.
#'
#' @param x A SparkSQL DataFrame.
#' @param how "any" or "all".
#'            if "any", drop a row if it contains any nulls.
#'            if "all", drop a row only if all its values are null.
#'            if minNonNulls is specified, how is ignored.
#' @param minNonNulls If specified, drop rows that have less than
#'                    minNonNulls non-null values.
#'                    This overwrites the how parameter.
#' @param cols Optional list of column names to consider.
#' @return A DataFrame
#'
#' @rdname nafunctions
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' dropna(df)
#' }
setMethod("dropna",
          signature(x = "DataFrame"),
          function(x, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
            how <- match.arg(how)
            if (is.null(cols)) {
              cols <- columns(x)
            }
            if (is.null(minNonNulls)) {
              minNonNulls <- if (how == "any") { length(cols) } else { 1 }
            }

            naFunctions <- callJMethod(x@sdf, "na")
            sdf <- callJMethod(naFunctions, "drop",
                               as.integer(minNonNulls), listToSeq(as.list(cols)))
            dataFrame(sdf)
          })

#' @aliases dropna
#' @export
setMethod("na.omit",
          signature(x = "DataFrame"),
          function(x, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
            dropna(x, how, minNonNulls, cols)
          })

#' fillna
#'
#' Replace null values.
#'
#' @param x A SparkSQL DataFrame.
#' @param value Value to replace null values with.
#'              Should be an integer, numeric, character or named list.
#'              If the value is a named list, then cols is ignored and
#'              value must be a mapping from column name (character) to
#'              replacement value. The replacement value must be an
#'              integer, numeric or character.
#' @param cols optional list of column names to consider.
#'             Columns specified in cols that do not have matching data
#'             type are ignored. For example, if value is a character, and
#'             subset contains a non-character column, then the non-character
#'             column is simply ignored.
#' @return A DataFrame
#'
#' @rdname nafunctions
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' fillna(df, 1)
#' fillna(df, list("age" = 20, "name" = "unknown"))
#' }
setMethod("fillna",
          signature(x = "DataFrame"),
          function(x, value, cols = NULL) {
            if (!(class(value) %in% c("integer", "numeric", "character", "list"))) {
              stop("value should be an integer, numeric, charactor or named list.")
            }

            if (class(value) == "list") {
              # Check column names in the named list
              colNames <- names(value)
              if (length(colNames) == 0 || !all(colNames != "")) {
                stop("value should be an a named list with each name being a column name.")
              }

              # Convert to the named list to an environment to be passed to JVM
              valueMap <- new.env()
              for (col in colNames) {
                # Check each item in the named list is of valid type
                v <- value[[col]]
                if (!(class(v) %in% c("integer", "numeric", "character"))) {
                  stop("Each item in value should be an integer, numeric or charactor.")
                }
                valueMap[[col]] <- v
              }

              # When value is a named list, caller is expected not to pass in cols
              if (!is.null(cols)) {
                warning("When value is a named list, cols is ignored!")
                cols <- NULL
              }

              value <- valueMap
            } else if (is.integer(value)) {
              # Cast an integer to a numeric
              value <- as.numeric(value)
            }

            naFunctions <- callJMethod(x@sdf, "na")
            sdf <- if (length(cols) == 0) {
              callJMethod(naFunctions, "fill", value)
            } else {
              callJMethod(naFunctions, "fill", value, listToSeq(as.list(cols)))
            }
            dataFrame(sdf)
          })

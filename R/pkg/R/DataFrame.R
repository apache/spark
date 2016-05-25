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

# DataFrame.R - SparkDataFrame class and methods implemented in S4 OO classes

#' @include generics.R jobj.R schema.R RDD.R pairRDD.R column.R group.R
NULL

setOldClass("jobj")
setOldClass("structType")

#' @title S4 class that represents a SparkDataFrame
#' @description DataFrames can be created using functions like \link{createDataFrame},
#'              \link{read.json}, \link{table} etc.
#' @family SparkDataFrame functions
#' @rdname SparkDataFrame
#' @docType class
#'
#' @slot env An R environment that stores bookkeeping states of the SparkDataFrame
#' @slot sdf A Java object reference to the backing Scala DataFrame
#' @seealso \link{createDataFrame}, \link{read.json}, \link{table}
#' @seealso \url{https://spark.apache.org/docs/latest/sparkr.html#sparkr-dataframes}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df <- createDataFrame(sqlContext, faithful)
#'}
setClass("SparkDataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "SparkDataFrame", function(.Object, sdf, isCached) {
  .Object@env <- new.env()
  .Object@env$isCached <- isCached

  .Object@sdf <- sdf
  .Object
})

#' @rdname SparkDataFrame
#' @export
#' @param sdf A Java object reference to the backing Scala DataFrame
#' @param isCached TRUE if the SparkDataFrame is cached
dataFrame <- function(sdf, isCached = FALSE) {
  new("SparkDataFrame", sdf, isCached)
}

############################ SparkDataFrame Methods ##############################################

#' Print Schema of a SparkDataFrame
#'
#' Prints out the schema in tree format
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname printSchema
#' @name printSchema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' printSchema(df)
#'}
setMethod("printSchema",
          signature(x = "SparkDataFrame"),
          function(x) {
            schemaString <- callJMethod(schema(x)$jobj, "treeString")
            cat(schemaString)
          })

#' Get schema object
#'
#' Returns the schema of this SparkDataFrame as a structType object.
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname schema
#' @name schema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' dfSchema <- schema(df)
#'}
setMethod("schema",
          signature(x = "SparkDataFrame"),
          function(x) {
            structType(callJMethod(x@sdf, "schema"))
          })

#' Explain
#'
#' Print the logical and physical Catalyst plans to the console for debugging.
#'
#' @param x A SparkDataFrame
#' @param extended Logical. If extended is False, explain() only prints the physical plan.
#' @family SparkDataFrame functions
#' @rdname explain
#' @name explain
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' explain(df, TRUE)
#'}
setMethod("explain",
          signature(x = "SparkDataFrame"),
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
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname isLocal
#' @name isLocal
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' isLocal(df)
#'}
setMethod("isLocal",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(x@sdf, "isLocal")
          })

#' showDF
#'
#' Print the first numRows rows of a SparkDataFrame
#'
#' @param x A SparkDataFrame
#' @param numRows The number of rows to print. Defaults to 20.
#'
#' @family SparkDataFrame functions
#' @rdname showDF
#' @name showDF
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' showDF(df)
#'}
setMethod("showDF",
          signature(x = "SparkDataFrame"),
          function(x, numRows = 20, truncate = TRUE) {
            s <- callJMethod(x@sdf, "showString", numToInt(numRows), truncate)
            cat(s)
          })

#' show
#'
#' Print the SparkDataFrame column names and types
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname show
#' @name show
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' df
#'}
setMethod("show", "SparkDataFrame",
          function(object) {
            cols <- lapply(dtypes(object), function(l) {
              paste(l, collapse = ":")
            })
            s <- paste(cols, collapse = ", ")
            cat(paste(class(object), "[", s, "]\n", sep = ""))
          })

#' DataTypes
#'
#' Return all column names and their data types as a list
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname dtypes
#' @name dtypes
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' dtypes(df)
#'}
setMethod("dtypes",
          signature(x = "SparkDataFrame"),
          function(x) {
            lapply(schema(x)$fields(), function(f) {
              c(f$name(), f$dataType.simpleString())
            })
          })

#' Column names
#'
#' Return all column names as a list
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname columns
#' @name columns

#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' columns(df)
#' colnames(df)
#'}
setMethod("columns",
          signature(x = "SparkDataFrame"),
          function(x) {
            sapply(schema(x)$fields(), function(f) {
              f$name()
            })
          })

#' @rdname columns
#' @name names
setMethod("names",
          signature(x = "SparkDataFrame"),
          function(x) {
            columns(x)
          })

#' @rdname columns
#' @name names<-
setMethod("names<-",
          signature(x = "SparkDataFrame"),
          function(x, value) {
            if (!is.null(value)) {
              sdf <- callJMethod(x@sdf, "toDF", as.list(value))
              dataFrame(sdf)
            }
          })

#' @rdname columns
#' @name colnames
setMethod("colnames",
          signature(x = "SparkDataFrame"),
          function(x) {
            columns(x)
          })

#' @rdname columns
#' @name colnames<-
setMethod("colnames<-",
          signature(x = "SparkDataFrame"),
          function(x, value) {

            # Check parameter integrity
            if (class(value) != "character") {
              stop("Invalid column names.")
            }

            if (length(value) != ncol(x)) {
              stop(
                "Column names must have the same length as the number of columns in the dataset.")
            }

            if (any(is.na(value))) {
              stop("Column names cannot be NA.")
            }

            # Check if the column names have . in it
            if (any(regexec(".", value, fixed = TRUE)[[1]][1] != -1)) {
              stop("Colum names cannot contain the '.' symbol.")
            }

            sdf <- callJMethod(x@sdf, "toDF", as.list(value))
            dataFrame(sdf)
          })

#' coltypes
#'
#' Get column types of a SparkDataFrame
#'
#' @param x A SparkDataFrame
#' @return value A character vector with the column types of the given SparkDataFrame
#' @rdname coltypes
#' @name coltypes
#' @family SparkDataFrame functions
#' @export
#' @examples
#'\dontrun{
#' irisDF <- createDataFrame(sqlContext, iris)
#' coltypes(irisDF)
#'}
setMethod("coltypes",
          signature(x = "SparkDataFrame"),
          function(x) {
            # Get the data types of the SparkDataFrame by invoking dtypes() function
            types <- sapply(dtypes(x), function(x) {x[[2]]})

            # Map Spark data types into R's data types using DATA_TYPES environment
            rTypes <- sapply(types, USE.NAMES = F, FUN = function(x) {
              # Check for primitive types
              type <- PRIMITIVE_TYPES[[x]]

              if (is.null(type)) {
                # Check for complex types
                for (t in names(COMPLEX_TYPES)) {
                  if (substring(x, 1, nchar(t)) == t) {
                    type <- COMPLEX_TYPES[[t]]
                    break
                  }
                }

                if (is.null(type)) {
                  stop(paste("Unsupported data type: ", x))
                }
              }
              type
            })

            # Find which types don't have mapping to R
            naIndices <- which(is.na(rTypes))

            # Assign the original scala data types to the unmatched ones
            rTypes[naIndices] <- types[naIndices]

            rTypes
          })

#' coltypes
#'
#' Set the column types of a SparkDataFrame.
#'
#' @param x A SparkDataFrame
#' @param value A character vector with the target column types for the given
#'    SparkDataFrame. Column types can be one of integer, numeric/double, character, logical, or NA
#'    to keep that column as-is.
#' @rdname coltypes
#' @name coltypes<-
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' coltypes(df) <- c("character", "integer")
#' coltypes(df) <- c(NA, "numeric")
#'}
setMethod("coltypes<-",
          signature(x = "SparkDataFrame", value = "character"),
          function(x, value) {
            cols <- columns(x)
            ncols <- length(cols)
            if (length(value) == 0) {
              stop("Cannot set types of an empty SparkDataFrame with no Column")
            }
            if (length(value) != ncols) {
              stop("Length of type vector should match the number of columns for SparkDataFrame")
            }
            newCols <- lapply(seq_len(ncols), function(i) {
              col <- getColumn(x, cols[i])
              if (!is.na(value[i])) {
                stype <- rToSQLTypes[[value[i]]]
                if (is.null(stype)) {
                  stop("Only atomic type is supported for column types")
                }
                cast(col, stype)
              } else {
                col
              }
            })
            nx <- select(x, newCols)
            dataFrame(nx@sdf)
          })

#' Register Temporary Table
#'
#' Registers a SparkDataFrame as a Temporary Table in the SQLContext
#'
#' @param x A SparkDataFrame
#' @param tableName A character vector containing the name of the table
#'
#' @family SparkDataFrame functions
#' @rdname registerTempTable
#' @name registerTempTable
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' registerTempTable(df, "json_df")
#' new_df <- sql(sqlContext, "SELECT * FROM json_df")
#'}
setMethod("registerTempTable",
          signature(x = "SparkDataFrame", tableName = "character"),
          function(x, tableName) {
              invisible(callJMethod(x@sdf, "registerTempTable", tableName))
          })

#' insertInto
#'
#' Insert the contents of a SparkDataFrame into a table registered in the current SQL Context.
#'
#' @param x A SparkDataFrame
#' @param tableName A character vector containing the name of the table
#' @param overwrite A logical argument indicating whether or not to overwrite
#' the existing rows in the table.
#'
#' @family SparkDataFrame functions
#' @rdname insertInto
#' @name insertInto
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
          signature(x = "SparkDataFrame", tableName = "character"),
          function(x, tableName, overwrite = FALSE) {
            jmode <- convertToJSaveMode(ifelse(overwrite, "overwrite", "append"))
            write <- callJMethod(x@sdf, "write")
            write <- callJMethod(write, "mode", jmode)
            callJMethod(write, "insertInto", tableName)
          })

#' Cache
#'
#' Persist with the default storage level (MEMORY_ONLY).
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname cache
#' @name cache
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' cache(df)
#'}
setMethod("cache",
          signature(x = "SparkDataFrame"),
          function(x) {
            cached <- callJMethod(x@sdf, "cache")
            x@env$isCached <- TRUE
            x
          })

#' Persist
#'
#' Persist this SparkDataFrame with the specified storage level. For details of the
#' supported storage levels, refer to
#' \url{http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence}.
#'
#' @param x The SparkDataFrame to persist
#'
#' @family SparkDataFrame functions
#' @rdname persist
#' @name persist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' persist(df, "MEMORY_AND_DISK")
#'}
setMethod("persist",
          signature(x = "SparkDataFrame", newLevel = "character"),
          function(x, newLevel) {
            callJMethod(x@sdf, "persist", getStorageLevel(newLevel))
            x@env$isCached <- TRUE
            x
          })

#' Unpersist
#'
#' Mark this SparkDataFrame as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param x The SparkDataFrame to unpersist
#' @param blocking Whether to block until all blocks are deleted
#'
#' @family SparkDataFrame functions
#' @rdname unpersist-methods
#' @name unpersist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' persist(df, "MEMORY_AND_DISK")
#' unpersist(df)
#'}
setMethod("unpersist",
          signature(x = "SparkDataFrame"),
          function(x, blocking = TRUE) {
            callJMethod(x@sdf, "unpersist", blocking)
            x@env$isCached <- FALSE
            x
          })

#' Repartition
#'
#' The following options for repartition are possible:
#' \itemize{
#'  \item{"Option 1"} {Return a new SparkDataFrame partitioned by
#'                      the given columns into `numPartitions`.}
#'  \item{"Option 2"} {Return a new SparkDataFrame that has exactly `numPartitions`.}
#'  \item{"Option 3"} {Return a new SparkDataFrame partitioned by the given column(s),
#'                      using `spark.sql.shuffle.partitions` as number of partitions.}
#'}
#' @param x A SparkDataFrame
#' @param numPartitions The number of partitions to use.
#' @param col The column by which the partitioning will be performed.
#'
#' @family SparkDataFrame functions
#' @rdname repartition
#' @name repartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newDF <- repartition(df, 2L)
#' newDF <- repartition(df, numPartitions = 2L)
#' newDF <- repartition(df, col = df$"col1", df$"col2")
#' newDF <- repartition(df, 3L, col = df$"col1", df$"col2")
#'}
setMethod("repartition",
          signature(x = "SparkDataFrame"),
          function(x, numPartitions = NULL, col = NULL, ...) {
            if (!is.null(numPartitions) && is.numeric(numPartitions)) {
              # number of partitions and columns both are specified
              if (!is.null(col) && class(col) == "Column") {
                cols <- list(col, ...)
                jcol <- lapply(cols, function(c) { c@jc })
                sdf <- callJMethod(x@sdf, "repartition", numToInt(numPartitions), jcol)
              } else {
                # only number of partitions is specified
                sdf <- callJMethod(x@sdf, "repartition", numToInt(numPartitions))
              }
            } else if (!is.null(col) && class(col) == "Column") {
              # only columns are specified
              cols <- list(col, ...)
              jcol <- lapply(cols, function(c) { c@jc })
              sdf <- callJMethod(x@sdf, "repartition", jcol)
            } else {
              stop("Please, specify the number of partitions and/or a column(s)")
            }
            dataFrame(sdf)
          })

#' toJSON
#'
#' Convert the rows of a SparkDataFrame into JSON objects and return an RDD where
#' each element contains a JSON string.
#'
#' @param x A SparkDataFrame
#' @return A StringRRDD of JSON objects
#' @family SparkDataFrame functions
#' @rdname tojson
#' @noRd
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newRDD <- toJSON(df)
#'}
setMethod("toJSON",
          signature(x = "SparkDataFrame"),
          function(x) {
            rdd <- callJMethod(x@sdf, "toJSON")
            jrdd <- callJMethod(rdd, "toJavaRDD")
            RDD(jrdd, serializedMode = "string")
          })

#' write.json
#'
#' Save the contents of a SparkDataFrame as a JSON file (one object per line). Files written out
#' with this method can be read back in as a SparkDataFrame using read.json().
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#'
#' @family SparkDataFrame functions
#' @rdname write.json
#' @name write.json
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' write.json(df, "/tmp/sparkr-tmp/")
#'}
setMethod("write.json",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path) {
            write <- callJMethod(x@sdf, "write")
            invisible(callJMethod(write, "json", path))
          })

#' write.parquet
#'
#' Save the contents of a SparkDataFrame as a Parquet file, preserving the schema. Files written out
#' with this method can be read back in as a SparkDataFrame using read.parquet().
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#'
#' @family SparkDataFrame functions
#' @rdname write.parquet
#' @name write.parquet
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' write.parquet(df, "/tmp/sparkr-tmp1/")
#' saveAsParquetFile(df, "/tmp/sparkr-tmp2/")
#'}
setMethod("write.parquet",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path) {
            write <- callJMethod(x@sdf, "write")
            invisible(callJMethod(write, "parquet", path))
          })

#' @rdname write.parquet
#' @name saveAsParquetFile
#' @export
setMethod("saveAsParquetFile",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path) {
            .Deprecated("write.parquet")
            write.parquet(x, path)
          })

#' write.text
#'
#' Saves the content of the SparkDataFrame in a text file at the specified path.
#' The SparkDataFrame must have only one column of string type with the name "value".
#' Each row becomes a new line in the output file.
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#'
#' @family SparkDataFrame functions
#' @rdname write.text
#' @name write.text
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.txt"
#' df <- read.text(sqlContext, path)
#' write.text(df, "/tmp/sparkr-tmp/")
#'}
setMethod("write.text",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path) {
            write <- callJMethod(x@sdf, "write")
            invisible(callJMethod(write, "text", path))
          })

#' Distinct
#'
#' Return a new SparkDataFrame containing the distinct rows in this SparkDataFrame.
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname distinct
#' @name distinct
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' distinctDF <- distinct(df)
#'}
setMethod("distinct",
          signature(x = "SparkDataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "distinct")
            dataFrame(sdf)
          })

#' @rdname distinct
#' @name unique
setMethod("unique",
          signature(x = "SparkDataFrame"),
          function(x) {
            distinct(x)
          })

#' Sample
#'
#' Return a sampled subset of this SparkDataFrame using a random seed.
#'
#' @param x A SparkDataFrame
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#'
#' @family SparkDataFrame functions
#' @rdname sample
#' @name sample
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' collect(sample(df, FALSE, 0.5))
#' collect(sample(df, TRUE, 0.5))
#'}
setMethod("sample",
          signature(x = "SparkDataFrame", withReplacement = "logical",
                    fraction = "numeric"),
          function(x, withReplacement, fraction, seed) {
            if (fraction < 0.0) stop(cat("Negative fraction value:", fraction))
            if (!missing(seed)) {
              # TODO : Figure out how to send integer as java.lang.Long to JVM so
              # we can send seed as an argument through callJMethod
              sdf <- callJMethod(x@sdf, "sample", withReplacement, fraction, as.integer(seed))
            } else {
              sdf <- callJMethod(x@sdf, "sample", withReplacement, fraction)
            }
            dataFrame(sdf)
          })

#' @rdname sample
#' @name sample_frac
setMethod("sample_frac",
          signature(x = "SparkDataFrame", withReplacement = "logical",
                    fraction = "numeric"),
          function(x, withReplacement, fraction, seed) {
            sample(x, withReplacement, fraction, seed)
          })

#' nrow
#'
#' Returns the number of rows in a SparkDataFrame
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname nrow
#' @name count
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' count(df)
#' }
setMethod("count",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(x@sdf, "count")
          })

#' @name nrow
#' @rdname nrow
setMethod("nrow",
          signature(x = "SparkDataFrame"),
          function(x) {
            count(x)
          })

#' Returns the number of columns in a SparkDataFrame
#'
#' @param x a SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname ncol
#' @name ncol
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' ncol(df)
#' }
setMethod("ncol",
          signature(x = "SparkDataFrame"),
          function(x) {
            length(columns(x))
          })

#' Returns the dimensions (number of rows and columns) of a SparkDataFrame
#' @param x a SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname dim
#' @name dim
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' dim(df)
#' }
setMethod("dim",
          signature(x = "SparkDataFrame"),
          function(x) {
            c(count(x), ncol(x))
          })

#' Collects all the elements of a SparkDataFrame and coerces them into an R data.frame.
#'
#' @param x A SparkDataFrame
#' @param stringsAsFactors (Optional) A logical indicating whether or not string columns
#' should be converted to factors. FALSE by default.
#'
#' @family SparkDataFrame functions
#' @rdname collect
#' @name collect
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' collected <- collect(df)
#' firstName <- collected[[1]]$name
#' }
setMethod("collect",
          signature(x = "SparkDataFrame"),
          function(x, stringsAsFactors = FALSE) {
            dtypes <- dtypes(x)
            ncol <- length(dtypes)
            if (ncol <= 0) {
              # empty data.frame with 0 columns and 0 rows
              data.frame()
            } else {
              # listCols is a list of columns
              listCols <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "dfToCols", x@sdf)
              stopifnot(length(listCols) == ncol)

              # An empty data.frame with 0 columns and number of rows as collected
              nrow <- length(listCols[[1]])
              if (nrow <= 0) {
                df <- data.frame()
              } else {
                df <- data.frame(row.names = 1 : nrow)
              }

              # Append columns one by one
              for (colIndex in 1 : ncol) {
                # Note: appending a column of list type into a data.frame so that
                # data of complex type can be held. But getting a cell from a column
                # of list type returns a list instead of a vector. So for columns of
                # non-complex type, append them as vector.
                #
                # For columns of complex type, be careful to access them.
                # Get a column of complex type returns a list.
                # Get a cell from a column of complex type returns a list instead of a vector.
                col <- listCols[[colIndex]]
                if (length(col) <= 0) {
                  df[[colIndex]] <- col
                } else {
                  colType <- dtypes[[colIndex]][[2]]
                  # Note that "binary" columns behave like complex types.
                  if (!is.null(PRIMITIVE_TYPES[[colType]]) && colType != "binary") {
                    vec <- do.call(c, col)
                    stopifnot(class(vec) != "list")
                    df[[colIndex]] <- vec
                  } else {
                    df[[colIndex]] <- col
                  }
                }
              }
              names(df) <- names(x)
              df
            }
          })

#' Limit
#'
#' Limit the resulting SparkDataFrame to the number of rows specified.
#'
#' @param x A SparkDataFrame
#' @param num The number of rows to return
#' @return A new SparkDataFrame containing the number of rows specified.
#'
#' @family SparkDataFrame functions
#' @rdname limit
#' @name limit
#' @export
#' @examples
#' \dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' limitedDF <- limit(df, 10)
#' }
setMethod("limit",
          signature(x = "SparkDataFrame", num = "numeric"),
          function(x, num) {
            res <- callJMethod(x@sdf, "limit", as.integer(num))
            dataFrame(res)
          })

#' Take the first NUM rows of a SparkDataFrame and return a the results as a data.frame
#'
#' @family SparkDataFrame functions
#' @rdname take
#' @name take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' take(df, 2)
#' }
setMethod("take",
          signature(x = "SparkDataFrame", num = "numeric"),
          function(x, num) {
            limited <- limit(x, num)
            collect(limited)
          })

#' Head
#'
#' Return the first NUM rows of a SparkDataFrame as a data.frame. If NUM is NULL,
#' then head() returns the first 6 rows in keeping with the current data.frame
#' convention in R.
#'
#' @param x A SparkDataFrame
#' @param num The number of rows to return. Default is 6.
#' @return A data.frame
#'
#' @family SparkDataFrame functions
#' @rdname head
#' @name head
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' head(df)
#' }
setMethod("head",
          signature(x = "SparkDataFrame"),
          function(x, num = 6L) {
          # Default num is 6L in keeping with R's data.frame convention
            take(x, num)
          })

#' Return the first row of a SparkDataFrame
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname first
#' @name first
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' first(df)
#' }
setMethod("first",
          signature(x = "SparkDataFrame"),
          function(x) {
            take(x, 1)
          })

#' toRDD
#'
#' Converts a SparkDataFrame to an RDD while preserving column names.
#'
#' @param x A SparkDataFrame
#'
#' @noRd
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' rdd <- toRDD(df)
#'}
setMethod("toRDD",
          signature(x = "SparkDataFrame"),
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
#' Groups the SparkDataFrame using the specified columns, so we can run aggregation on them.
#'
#' @param x a SparkDataFrame
#' @return a GroupedData
#' @seealso GroupedData
#' @family SparkDataFrame functions
#' @rdname groupBy
#' @name groupBy
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
           signature(x = "SparkDataFrame"),
           function(x, ...) {
             cols <- list(...)
             if (length(cols) >= 1 && class(cols[[1]]) == "character") {
               sgd <- callJMethod(x@sdf, "groupBy", cols[[1]], cols[-1])
             } else {
               jcol <- lapply(cols, function(c) { c@jc })
               sgd <- callJMethod(x@sdf, "groupBy", jcol)
             }
             groupedData(sgd)
           })

#' @rdname groupBy
#' @name group_by
setMethod("group_by",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            groupBy(x, ...)
          })

#' Summarize data across columns
#'
#' Compute aggregates by specifying a list of columns
#'
#' @param x a SparkDataFrame
#' @family SparkDataFrame functions
#' @rdname agg
#' @name agg
#' @export
setMethod("agg",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            agg(groupBy(x), ...)
          })

#' @rdname agg
#' @name summarize
setMethod("summarize",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            agg(x, ...)
          })

dapplyInternal <- function(x, func, schema) {
  packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                               connection = NULL)

  broadcastArr <- lapply(ls(.broadcastNames),
                         function(name) { get(name, .broadcastNames) })

  sdf <- callJStatic(
           "org.apache.spark.sql.api.r.SQLUtils",
           "dapply",
           x@sdf,
           serialize(cleanClosure(func), connection = NULL),
           packageNamesArr,
           broadcastArr,
           if (is.null(schema)) { schema } else { schema$jobj })
  dataFrame(sdf)
}

#' dapply
#'
#' Apply a function to each partition of a SparkDataFrame.
#'
#' @param x A SparkDataFrame
#' @param func A function to be applied to each partition of the SparkDataFrame.
#'             func should have only one parameter, to which a data.frame corresponds
#'             to each partition will be passed.
#'             The output of func should be a data.frame.
#' @param schema The schema of the resulting DataFrame after the function is applied.
#'               It must match the output of func.
#' @family SparkDataFrame functions
#' @rdname dapply
#' @name dapply
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame (sqlContext, iris)
#'   df1 <- dapply(df, function(x) { x }, schema(df))
#'   collect(df1)
#'
#'   # filter and add a column
#'   df <- createDataFrame (
#'           sqlContext, 
#'           list(list(1L, 1, "1"), list(2L, 2, "2"), list(3L, 3, "3")),
#'           c("a", "b", "c"))
#'   schema <- structType(structField("a", "integer"), structField("b", "double"),
#'                      structField("c", "string"), structField("d", "integer"))
#'   df1 <- dapply(
#'            df,
#'            function(x) {
#'              y <- x[x[1] > 1, ]
#'              y <- cbind(y, y[1] + 1L)
#'            },
#'            schema)
#'   collect(df1)
#'   # the result
#'   #       a b c d
#'   #     1 2 2 2 3
#'   #     2 3 3 3 4
#' }
setMethod("dapply",
          signature(x = "SparkDataFrame", func = "function", schema = "structType"),
          function(x, func, schema) {
            dapplyInternal(x, func, schema)
          })

#' dapplyCollect
#'
#' Apply a function to each partition of a SparkDataFrame and collect the result back
#’ to R as a data.frame.
#'
#' @param x A SparkDataFrame
#' @param func A function to be applied to each partition of the SparkDataFrame.
#'             func should have only one parameter, to which a data.frame corresponds
#'             to each partition will be passed.
#'             The output of func should be a data.frame.
#' @family SparkDataFrame functions
#' @rdname dapply
#' @name dapplyCollect
#' @export
#' @examples
#' \dontrun{
#'   df <- createDataFrame (sqlContext, iris)
#'   ldf <- dapplyCollect(df, function(x) { x })
#'
#'   # filter and add a column
#'   df <- createDataFrame (
#'           sqlContext, 
#'           list(list(1L, 1, "1"), list(2L, 2, "2"), list(3L, 3, "3")),
#'           c("a", "b", "c"))
#'   ldf <- dapplyCollect(
#'            df,
#'            function(x) {
#'              y <- x[x[1] > 1, ]
#'              y <- cbind(y, y[1] + 1L)
#'            })
#'   # the result
#'   #       a b c d
#'   #       2 2 2 3
#'   #       3 3 3 4
#' }
setMethod("dapplyCollect",
          signature(x = "SparkDataFrame", func = "function"),
          function(x, func) {
            df <- dapplyInternal(x, func, NULL)

            content <- callJMethod(df@sdf, "collect")
            # content is a list of items of struct type. Each item has a single field
            # which is a serialized data.frame corresponds to one partition of the
            # SparkDataFrame.
            ldfs <- lapply(content, function(x) { unserialize(x[[1]]) })
            ldf <- do.call(rbind, ldfs)
            row.names(ldf) <- NULL
            ldf
          })

############################## RDD Map Functions ##################################
# All of the following functions mirror the existing RDD map functions,           #
# but allow for use with DataFrames by first converting to an RRDD before calling #
# the requested map function.                                                     #
###################################################################################

#' @rdname lapply
#' @noRd
setMethod("lapply",
          signature(X = "SparkDataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapply(rdd, FUN)
          })

#' @rdname lapply
#' @noRd
setMethod("map",
          signature(X = "SparkDataFrame", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' @rdname flatMap
#' @noRd
setMethod("flatMap",
          signature(X = "SparkDataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            flatMap(rdd, FUN)
          })

#' @rdname lapplyPartition
#' @noRd
setMethod("lapplyPartition",
          signature(X = "SparkDataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapplyPartition(rdd, FUN)
          })

#' @rdname lapplyPartition
#' @noRd
setMethod("mapPartitions",
          signature(X = "SparkDataFrame", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

#' @rdname foreach
#' @noRd
setMethod("foreach",
          signature(x = "SparkDataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreach(rdd, func)
          })

#' @rdname foreach
#' @noRd
setMethod("foreachPartition",
          signature(x = "SparkDataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreachPartition(rdd, func)
          })


############################## SELECT ##################################

getColumn <- function(x, c) {
  column(callJMethod(x@sdf, "col", c))
}

#' @rdname select
#' @name $
setMethod("$", signature(x = "SparkDataFrame"),
          function(x, name) {
            getColumn(x, name)
          })

#' @rdname select
#' @name $<-
setMethod("$<-", signature(x = "SparkDataFrame"),
          function(x, name, value) {
            stopifnot(class(value) == "Column" || is.null(value))

            if (is.null(value)) {
              nx <- drop(x, name)
            } else {
              nx <- withColumn(x, name, value)
            }
            x@sdf <- nx@sdf
            x
          })

setClassUnion("numericOrcharacter", c("numeric", "character"))

#' @rdname subset
#' @name [[
setMethod("[[", signature(x = "SparkDataFrame", i = "numericOrcharacter"),
          function(x, i) {
            if (is.numeric(i)) {
              cols <- columns(x)
              i <- cols[[i]]
            }
            getColumn(x, i)
          })

#' @rdname subset
#' @name [
setMethod("[", signature(x = "SparkDataFrame"),
          function(x, i, j, ..., drop = F) {
            # Perform filtering first if needed
            filtered <- if (missing(i)) {
              x
            } else {
              if (class(i) != "Column") {
                stop(paste0("Expressions other than filtering predicates are not supported ",
                      "in the first parameter of extract operator [ or subset() method."))
              }
              filter(x, i)
            }

            # If something is to be projected, then do so on the filtered SparkDataFrame
            if (missing(j)) {
              filtered
            } else {
              if (is.numeric(j)) {
                cols <- columns(filtered)
                j <- cols[j]
              }
              if (length(j) > 1) {
                j <- as.list(j)
              }
              selected <- select(filtered, j)

              # Acknowledge parameter drop. Return a Column or SparkDataFrame accordingly
              if (ncol(selected) == 1 & drop == T) {
                getColumn(selected, names(selected))
              } else {
                selected
              }
            }
          })

#' Subset
#'
#' Return subsets of SparkDataFrame according to given conditions
#' @param x A SparkDataFrame
#' @param subset (Optional) A logical expression to filter on rows
#' @param select expression for the single Column or a list of columns to select from the SparkDataFrame
#' @param drop if TRUE, a Column will be returned if the resulting dataset has only one column.
#' Otherwise, a SparkDataFrame will always be returned.
#' @return A new SparkDataFrame containing only the rows that meet the condition with selected columns
#' @export
#' @family SparkDataFrame functions
#' @rdname subset
#' @name subset
#' @family subsetting functions
#' @examples
#' \dontrun{
#'   # Columns can be selected using `[[` and `[`
#'   df[[2]] == df[["age"]]
#'   df[,2] == df[,"age"]
#'   df[,c("name", "age")]
#'   # Or to filter rows
#'   df[df$age > 20,]
#'   # SparkDataFrame can be subset on both rows and Columns
#'   df[df$name == "Smith", c(1,2)]
#'   df[df$age %in% c(19, 30), 1:2]
#'   subset(df, df$age %in% c(19, 30), 1:2)
#'   subset(df, df$age %in% c(19), select = c(1,2))
#'   subset(df, select = c(1,2))
#' }
setMethod("subset", signature(x = "SparkDataFrame"),
          function(x, subset, select, drop = F, ...) {
            if (missing(subset)) {
                x[, select, drop = drop, ...]
            } else {
                x[subset, select, drop = drop, ...]
            }
          })

#' Select
#'
#' Selects a set of columns with names or Column expressions.
#' @param x A SparkDataFrame
#' @param col A list of columns or single Column or name
#' @return A new SparkDataFrame with selected columns
#' @export
#' @family SparkDataFrame functions
#' @rdname select
#' @name select
#' @family subsetting functions
#' @examples
#' \dontrun{
#'   select(df, "*")
#'   select(df, "col1", "col2")
#'   select(df, df$name, df$age + 1)
#'   select(df, c("col1", "col2"))
#'   select(df, list(df$name, df$age + 1))
#'   # Similar to R data frames columns can also be selected using `$`
#'   df[,df$age]
#' }
setMethod("select", signature(x = "SparkDataFrame", col = "character"),
          function(x, col, ...) {
            if (length(col) > 1) {
              if (length(list(...)) > 0) {
                stop("To select multiple columns, use a character vector or list for col")
              }

              select(x, as.list(col))
            } else {
              sdf <- callJMethod(x@sdf, "select", col, list(...))
              dataFrame(sdf)
            }
          })

#' @family SparkDataFrame functions
#' @rdname select
#' @export
setMethod("select", signature(x = "SparkDataFrame", col = "Column"),
          function(x, col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            sdf <- callJMethod(x@sdf, "select", jcols)
            dataFrame(sdf)
          })

#' @family SparkDataFrame functions
#' @rdname select
#' @export
setMethod("select",
          signature(x = "SparkDataFrame", col = "list"),
          function(x, col) {
            cols <- lapply(col, function(c) {
              if (class(c) == "Column") {
                c@jc
              } else {
                col(c)@jc
              }
            })
            sdf <- callJMethod(x@sdf, "select", cols)
            dataFrame(sdf)
          })

#' SelectExpr
#'
#' Select from a SparkDataFrame using a set of SQL expressions.
#'
#' @param x A SparkDataFrame to be selected from.
#' @param expr A string containing a SQL expression
#' @param ... Additional expressions
#' @return A SparkDataFrame
#' @family SparkDataFrame functions
#' @rdname selectExpr
#' @name selectExpr
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' selectExpr(df, "col1", "(col2 * 5) as newCol")
#' }
setMethod("selectExpr",
          signature(x = "SparkDataFrame", expr = "character"),
          function(x, expr, ...) {
            exprList <- list(expr, ...)
            sdf <- callJMethod(x@sdf, "selectExpr", exprList)
            dataFrame(sdf)
          })

#' WithColumn
#'
#' Return a new SparkDataFrame by adding a column or replacing the existing column
#' that has the same name.
#'
#' @param x A SparkDataFrame
#' @param colName A column name.
#' @param col A Column expression.
#' @return A SparkDataFrame with the new column added or the existing column replaced.
#' @family SparkDataFrame functions
#' @rdname withColumn
#' @name withColumn
#' @seealso \link{rename} \link{mutate}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newDF <- withColumn(df, "newCol", df$col1 * 5)
#' # Replace an existing column
#' newDF2 <- withColumn(newDF, "newCol", newDF$col1)
#' }
setMethod("withColumn",
          signature(x = "SparkDataFrame", colName = "character", col = "Column"),
          function(x, colName, col) {
            sdf <- callJMethod(x@sdf, "withColumn", colName, col@jc)
            dataFrame(sdf)
          })

#' Mutate
#'
#' Return a new SparkDataFrame with the specified columns added or replaced.
#'
#' @param .data A SparkDataFrame
#' @param col a named argument of the form name = col
#' @return A new SparkDataFrame with the new columns added or replaced.
#' @family SparkDataFrame functions
#' @rdname mutate
#' @name mutate
#' @seealso \link{rename} \link{withColumn}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newDF <- mutate(df, newCol = df$col1 * 5, newCol2 = df$col1 * 2)
#' names(newDF) # Will contain newCol, newCol2
#' newDF2 <- transform(df, newCol = df$col1 / 5, newCol2 = df$col1 * 2)
#'
#' df <- createDataFrame(sqlContext, 
#'                       list(list("Andy", 30L), list("Justin", 19L)), c("name", "age"))
#' # Replace the "age" column
#' df1 <- mutate(df, age = df$age + 1L)
#' }
setMethod("mutate",
          signature(.data = "SparkDataFrame"),
          function(.data, ...) {
            x <- .data
            cols <- list(...)
            if (length(cols) <= 0) {
              return(x)
            }

            lapply(cols, function(col) {
              stopifnot(class(col) == "Column")
            })

            # Check if there is any duplicated column name in the DataFrame
            dfCols <- columns(x)
            if (length(unique(dfCols)) != length(dfCols)) {
              stop("Error: found duplicated column name in the DataFrame")
            }

            # TODO: simplify the implementation of this method after SPARK-12225 is resolved.

            # For named arguments, use the names for arguments as the column names
            # For unnamed arguments, use the argument symbols as the column names
            args <- sapply(substitute(list(...))[-1], deparse)
            ns <- names(cols)
            if (!is.null(ns)) {
              lapply(seq_along(args), function(i) {
                if (ns[[i]] != "") {
                  args[[i]] <<- ns[[i]]
                }
              })
            }
            ns <- args

            # The last column of the same name in the specific columns takes effect
            deDupCols <- list()
            for (i in 1:length(cols)) {
              deDupCols[[ns[[i]]]] <- alias(cols[[i]], ns[[i]])
            }

            # Construct the column list for projection
            colList <- lapply(dfCols, function(col) {
              if (!is.null(deDupCols[[col]])) {
                # Replace existing column
                tmpCol <- deDupCols[[col]]
                deDupCols[[col]] <<- NULL
                tmpCol
              } else {
                col(col)
              }
            })

            do.call(select, c(x, colList, deDupCols))
          })

#' @export
#' @rdname mutate
#' @name transform
setMethod("transform",
          signature(`_data` = "SparkDataFrame"),
          function(`_data`, ...) {
            mutate(`_data`, ...)
          })

#' rename
#'
#' Rename an existing column in a SparkDataFrame.
#'
#' @param x A SparkDataFrame
#' @param existingCol The name of the column you want to change.
#' @param newCol The new column name.
#' @return A SparkDataFrame with the column name changed.
#' @family SparkDataFrame functions
#' @rdname rename
#' @name withColumnRenamed
#' @seealso \link{mutate}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newDF <- withColumnRenamed(df, "col1", "newCol1")
#' }
setMethod("withColumnRenamed",
          signature(x = "SparkDataFrame", existingCol = "character", newCol = "character"),
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

#' @param newColPair A named pair of the form new_column_name = existing_column
#' @rdname rename
#' @name rename
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' newDF <- rename(df, col1 = df$newCol1)
#' }
setMethod("rename",
          signature(x = "SparkDataFrame"),
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
#' Sort a SparkDataFrame by the specified column(s).
#'
#' @param x A SparkDataFrame to be sorted.
#' @param col A character or Column object vector indicating the fields to sort on
#' @param ... Additional sorting fields
#' @param decreasing A logical argument indicating sorting order for columns when
#'                   a character vector is specified for col
#' @return A SparkDataFrame where all elements are sorted.
#' @family SparkDataFrame functions
#' @rdname arrange
#' @name arrange
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' arrange(df, df$col1)
#' arrange(df, asc(df$col1), desc(abs(df$col2)))
#' arrange(df, "col1", decreasing = TRUE)
#' arrange(df, "col1", "col2", decreasing = c(TRUE, FALSE))
#' }
setMethod("arrange",
          signature(x = "SparkDataFrame", col = "Column"),
          function(x, col, ...) {
              jcols <- lapply(list(col, ...), function(c) {
                c@jc
              })

            sdf <- callJMethod(x@sdf, "sort", jcols)
            dataFrame(sdf)
          })

#' @rdname arrange
#' @name arrange
#' @export
setMethod("arrange",
          signature(x = "SparkDataFrame", col = "character"),
          function(x, col, ..., decreasing = FALSE) {

            # all sorting columns
            by <- list(col, ...)

            if (length(decreasing) == 1) {
              # in case only 1 boolean argument - decreasing value is specified,
              # it will be used for all columns
              decreasing <- rep(decreasing, length(by))
            } else if (length(decreasing) != length(by)) {
              stop("Arguments 'col' and 'decreasing' must have the same length")
            }

            # builds a list of columns of type Column
            # example: [[1]] Column Species ASC
            #          [[2]] Column Petal_Length DESC
            jcols <- lapply(seq_len(length(decreasing)), function(i){
              if (decreasing[[i]]) {
                desc(getColumn(x, by[[i]]))
              } else {
                asc(getColumn(x, by[[i]]))
              }
            })

            do.call("arrange", c(x, jcols))
          })

#' @rdname arrange
#' @name orderBy
#' @export
setMethod("orderBy",
          signature(x = "SparkDataFrame", col = "characterOrColumn"),
          function(x, col, ...) {
            arrange(x, col, ...)
          })

#' Filter
#'
#' Filter the rows of a SparkDataFrame according to a given condition.
#'
#' @param x A SparkDataFrame to be sorted.
#' @param condition The condition to filter on. This may either be a Column expression
#' or a string containing a SQL statement
#' @return A SparkDataFrame containing only the rows that meet the condition.
#' @family SparkDataFrame functions
#' @rdname filter
#' @name filter
#' @family subsetting functions
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' filter(df, "col1 > 0")
#' filter(df, df$col2 != "abcdefg")
#' }
setMethod("filter",
          signature(x = "SparkDataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            if (class(condition) == "Column") {
              condition <- condition@jc
            }
            sdf <- callJMethod(x@sdf, "filter", condition)
            dataFrame(sdf)
          })

#' @family SparkDataFrame functions
#' @rdname filter
#' @name where
setMethod("where",
          signature(x = "SparkDataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            filter(x, condition)
          })

#' dropDuplicates
#'
#' Returns a new SparkDataFrame with duplicate rows removed, considering only
#' the subset of columns.
#'
#' @param x A SparkDataFrame.
#' @param colnames A character vector of column names.
#' @return A SparkDataFrame with duplicate rows removed.
#' @family SparkDataFrame functions
#' @rdname dropduplicates
#' @name dropDuplicates
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' dropDuplicates(df)
#' dropDuplicates(df, c("col1", "col2"))
#' }
setMethod("dropDuplicates",
          signature(x = "SparkDataFrame"),
          function(x, colNames = columns(x)) {
            stopifnot(class(colNames) == "character")

            sdf <- callJMethod(x@sdf, "dropDuplicates", as.list(colNames))
            dataFrame(sdf)
          })

#' Join
#'
#' Join two SparkDataFrames based on the given join expression.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @param joinExpr (Optional) The expression used to perform the join. joinExpr must be a
#' Column expression. If joinExpr is omitted, join() will perform a Cartesian join
#' @param joinType The type of join to perform. The following join types are available:
#' 'inner', 'outer', 'full', 'fullouter', leftouter', 'left_outer', 'left',
#' 'right_outer', 'rightouter', 'right', and 'leftsemi'. The default joinType is "inner".
#' @return A SparkDataFrame containing the result of the join operation.
#' @family SparkDataFrame functions
#' @rdname join
#' @name join
#' @seealso \link{merge}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.json(sqlContext, path)
#' df2 <- read.json(sqlContext, path2)
#' join(df1, df2) # Performs a Cartesian
#' join(df1, df2, df1$col1 == df2$col2) # Performs an inner join based on expression
#' join(df1, df2, df1$col1 == df2$col2, "right_outer")
#' }
setMethod("join",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y, joinExpr = NULL, joinType = NULL) {
            if (is.null(joinExpr)) {
              sdf <- callJMethod(x@sdf, "join", y@sdf)
            } else {
              if (class(joinExpr) != "Column") stop("joinExpr must be a Column")
              if (is.null(joinType)) {
                sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc)
              } else {
                if (joinType %in% c("inner", "outer", "full", "fullouter",
                    "leftouter", "left_outer", "left",
                    "rightouter", "right_outer", "right", "leftsemi")) {
                  joinType <- gsub("_", "", joinType)
                  sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc, joinType)
                } else {
                  stop("joinType must be one of the following types: ",
                      "'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left_outer', 'left',
                      'rightouter', 'right_outer', 'right', 'leftsemi'")
                }
              }
            }
            dataFrame(sdf)
          })

#' @name merge
#' @title Merges two data frames
#' @param x the first data frame to be joined
#' @param y the second data frame to be joined
#' @param by a character vector specifying the join columns. If by is not
#'   specified, the common column names in \code{x} and \code{y} will be used.
#' @param by.x a character vector specifying the joining columns for x.
#' @param by.y a character vector specifying the joining columns for y.
#' @param all.x a boolean value indicating whether all the rows in x should
#'              be including in the join
#' @param all.y a boolean value indicating whether all the rows in y should
#'              be including in the join
#' @param sort a logical argument indicating whether the resulting columns should be sorted
#' @details  If all.x and all.y are set to FALSE, a natural join will be returned. If
#'   all.x is set to TRUE and all.y is set to FALSE, a left outer join will
#'   be returned. If all.x is set to FALSE and all.y is set to TRUE, a right
#'   outer join will be returned. If all.x and all.y are set to TRUE, a full
#'   outer join will be returned.
#' @family SparkDataFrame functions
#' @rdname merge
#' @seealso \link{join}
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.json(sqlContext, path)
#' df2 <- read.json(sqlContext, path2)
#' merge(df1, df2) # Performs a Cartesian
#' merge(df1, df2, by = "col1") # Performs an inner join based on expression
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.y = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.x = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.x = TRUE, all.y = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all = TRUE, sort = FALSE)
#' merge(df1, df2, by = "col1", all = TRUE, suffixes = c("-X", "-Y"))
#' }
setMethod("merge",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y, by = intersect(names(x), names(y)), by.x = by, by.y = by,
                   all = FALSE, all.x = all, all.y = all,
                   sort = TRUE, suffixes = c("_x", "_y"), ... ) {

            if (length(suffixes) != 2) {
              stop("suffixes must have length 2")
            }

            # join type is identified based on the values of all, all.x and all.y
            # default join type is inner, according to R it should be natural but since it
            # is not supported in spark inner join is used
            joinType <- "inner"
            if (all || (all.x && all.y)) {
              joinType <- "outer"
            } else if (all.x) {
              joinType <- "left_outer"
            } else if (all.y) {
              joinType <- "right_outer"
            }

            # join expression is based on by.x, by.y if both by.x and by.y are not missing
            # or on by, if by.x or by.y are missing or have different lengths
            if (length(by.x) > 0 && length(by.x) == length(by.y)) {
              joinX <- by.x
              joinY <- by.y
            } else if (length(by) > 0) {
              # if join columns have the same name for both dataframes,
              # they are used in join expression
              joinX <- by
              joinY <- by
            } else {
              # if by or both by.x and by.y have length 0, use Cartesian Product
              joinRes <- join(x, y)
              return (joinRes)
            }

            # sets alias for making colnames unique in dataframes 'x' and 'y'
            colsX <- generateAliasesForIntersectedCols(x, by, suffixes[1])
            colsY <- generateAliasesForIntersectedCols(y, by, suffixes[2])

            # selects columns with their aliases from dataframes
            # in case same column names are present in both data frames
            xsel <- select(x, colsX)
            ysel <- select(y, colsY)

            # generates join conditions and adds them into a list
            # it also considers alias names of the columns while generating join conditions
            joinColumns <- lapply(seq_len(length(joinX)), function(i) {
              colX <- joinX[[i]]
              colY <- joinY[[i]]

              if (colX %in% by) {
                colX <- paste(colX, suffixes[1], sep = "")
              }
              if (colY %in% by) {
                colY <- paste(colY, suffixes[2], sep = "")
              }

              colX <- getColumn(xsel, colX)
              colY <- getColumn(ysel, colY)

              colX == colY
            })

            # concatenates join columns with '&' and executes join
            joinExpr <- Reduce("&", joinColumns)
            joinRes <- join(xsel, ysel, joinExpr, joinType)

            # sorts the result by 'by' columns if sort = TRUE
            if (sort && length(by) > 0) {
              colNameWithSuffix <- paste(by, suffixes[2], sep = "")
              joinRes <- do.call("arrange", c(joinRes, colNameWithSuffix, decreasing = FALSE))
            }

            joinRes
          })

#'
#' Creates a list of columns by replacing the intersected ones with aliases.
#' The name of the alias column is formed by concatanating the original column name and a suffix.
#'
#' @param x a SparkDataFrame on which the
#' @param intersectedColNames a list of intersected column names
#' @param suffix a suffix for the column name
#' @return list of columns
#'
generateAliasesForIntersectedCols <- function (x, intersectedColNames, suffix) {
  allColNames <- names(x)
  # sets alias for making colnames unique in dataframe 'x'
  cols <- lapply(allColNames, function(colName) {
    col <- getColumn(x, colName)
    if (colName %in% intersectedColNames) {
      newJoin <- paste(colName, suffix, sep = "")
      if (newJoin %in% allColNames){
        stop ("The following column name: ", newJoin, " occurs more than once in the 'DataFrame'.",
          "Please use different suffixes for the intersected columns.")
      }
      col <- alias(col, newJoin)
    }
    col
  })
  cols
}

#' rbind
#'
#' Return a new SparkDataFrame containing the union of rows in this SparkDataFrame
#' and another SparkDataFrame. This is equivalent to `UNION ALL` in SQL.
#' Note that this does not remove duplicate rows across the two SparkDataFrames.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the union.
#' @family SparkDataFrame functions
#' @rdname rbind
#' @name unionAll
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.json(sqlContext, path)
#' df2 <- read.json(sqlContext, path2)
#' unioned <- unionAll(df, df2)
#' }
setMethod("unionAll",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            unioned <- callJMethod(x@sdf, "unionAll", y@sdf)
            dataFrame(unioned)
          })

#' @title Union two or more SparkDataFrames
#' @description Returns a new SparkDataFrame containing rows of all parameters.
#'
#' @rdname rbind
#' @name rbind
#' @export
setMethod("rbind",
          signature(... = "SparkDataFrame"),
          function(x, ..., deparse.level = 1) {
            if (nargs() == 3) {
              unionAll(x, ...)
            } else {
              unionAll(x, Recall(..., deparse.level = 1))
            }
          })

#' Intersect
#'
#' Return a new SparkDataFrame containing rows only in both this SparkDataFrame
#' and another SparkDataFrame. This is equivalent to `INTERSECT` in SQL.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the intersect.
#' @family SparkDataFrame functions
#' @rdname intersect
#' @name intersect
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.json(sqlContext, path)
#' df2 <- read.json(sqlContext, path2)
#' intersectDF <- intersect(df, df2)
#' }
setMethod("intersect",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            intersected <- callJMethod(x@sdf, "intersect", y@sdf)
            dataFrame(intersected)
          })

#' except
#'
#' Return a new SparkDataFrame containing rows in this SparkDataFrame
#' but not in another SparkDataFrame. This is equivalent to `EXCEPT` in SQL.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the except operation.
#' @family SparkDataFrame functions
#' @rdname except
#' @name except
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.json(sqlContext, path)
#' df2 <- read.json(sqlContext, path2)
#' exceptDF <- except(df, df2)
#' }
#' @rdname except
#' @export
setMethod("except",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            excepted <- callJMethod(x@sdf, "except", y@sdf)
            dataFrame(excepted)
          })

#' Save the contents of the SparkDataFrame to a data source
#'
#' The data source is specified by the `source` and a set of options (...).
#' If `source` is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes: \cr
#'  append: Contents of this SparkDataFrame are expected to be appended to existing data. \cr
#'  overwrite: Existing data is expected to be overwritten by the contents of this
#'     SparkDataFrame. \cr
#'  error: An exception is expected to be thrown. \cr
#'  ignore: The save operation is expected to not save the contents of the SparkDataFrame
#'     and to not change the existing data. \cr
#'
#' @param df A SparkDataFrame
#' @param path A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore' save mode (it is 'error' by default)
#'
#' @family SparkDataFrame functions
#' @rdname write.df
#' @name write.df
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' write.df(df, "myfile", "parquet", "overwrite")
#' saveDF(df, parquetPath2, "parquet", mode = saveMode, mergeSchema = mergeSchema)
#' }
setMethod("write.df",
          signature(df = "SparkDataFrame", path = "character"),
          function(df, path, source = NULL, mode = "error", ...){
            if (is.null(source)) {
              if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
                sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              } else if (exists(".sparkRHivesc", envir = .sparkREnv)) {
                sqlContext <- get(".sparkRHivesc", envir = .sparkREnv)
              } else {
                stop("sparkRHive or sparkRSQL context has to be specified")
              }
              source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                    "org.apache.spark.sql.parquet")
            }
            jmode <- convertToJSaveMode(mode)
            options <- varargsToEnv(...)
            if (!is.null(path)) {
                options[["path"]] <- path
            }
            write <- callJMethod(df@sdf, "write")
            write <- callJMethod(write, "format", source)
            write <- callJMethod(write, "mode", jmode)
            write <- callJMethod(write, "save", path)
          })

#' @rdname write.df
#' @name saveDF
#' @export
setMethod("saveDF",
          signature(df = "SparkDataFrame", path = "character"),
          function(df, path, source = NULL, mode = "error", ...){
            write.df(df, path, source, mode, ...)
          })

#' saveAsTable
#'
#' Save the contents of the SparkDataFrame to a data source as a table
#'
#' The data source is specified by the `source` and a set of options (...).
#' If `source` is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes: \cr
#'  append: Contents of this SparkDataFrame are expected to be appended to existing data. \cr
#'  overwrite: Existing data is expected to be overwritten by the contents of this
#'     SparkDataFrame. \cr
#'  error: An exception is expected to be thrown. \cr
#'  ignore: The save operation is expected to not save the contents of the SparkDataFrame
#'     and to not change the existing data. \cr
#'
#' @param df A SparkDataFrame
#' @param tableName A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore' save mode (it is 'error' by default)
#'
#' @family SparkDataFrame functions
#' @rdname saveAsTable
#' @name saveAsTable
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' saveAsTable(df, "myfile")
#' }
setMethod("saveAsTable",
          signature(df = "SparkDataFrame", tableName = "character"),
          function(df, tableName, source = NULL, mode="error", ...){
            if (is.null(source)) {
              if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
                sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              } else if (exists(".sparkRHivesc", envir = .sparkREnv)) {
                sqlContext <- get(".sparkRHivesc", envir = .sparkREnv)
              } else {
                stop("sparkRHive or sparkRSQL context has to be specified")
              }
               source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                     "org.apache.spark.sql.parquet")
            }
            jmode <- convertToJSaveMode(mode)
            options <- varargsToEnv(...)

            write <- callJMethod(df@sdf, "write")
            write <- callJMethod(write, "format", source)
            write <- callJMethod(write, "mode", jmode)
            write <- callJMethod(write, "options", options)
            callJMethod(write, "saveAsTable", tableName)
          })

#' summary
#'
#' Computes statistics for numeric columns.
#' If no columns are given, this function computes statistics for all numerical columns.
#'
#' @param x A SparkDataFrame to be computed.
#' @param col A string of name
#' @param ... Additional expressions
#' @return A SparkDataFrame
#' @family SparkDataFrame functions
#' @rdname summary
#' @name describe
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' describe(df)
#' describe(df, "col1")
#' describe(df, "col1", "col2")
#' }
setMethod("describe",
          signature(x = "SparkDataFrame", col = "character"),
          function(x, col, ...) {
            colList <- list(col, ...)
            sdf <- callJMethod(x@sdf, "describe", colList)
            dataFrame(sdf)
          })

#' @rdname summary
#' @name describe
setMethod("describe",
          signature(x = "SparkDataFrame"),
          function(x) {
            colList <- as.list(c(columns(x)))
            sdf <- callJMethod(x@sdf, "describe", colList)
            dataFrame(sdf)
          })

#' @rdname summary
#' @name summary
setMethod("summary",
          signature(object = "SparkDataFrame"),
          function(object, ...) {
            describe(object)
          })


#' dropna
#'
#' Returns a new SparkDataFrame omitting rows with null values.
#'
#' @param x A SparkDataFrame.
#' @param how "any" or "all".
#'            if "any", drop a row if it contains any nulls.
#'            if "all", drop a row only if all its values are null.
#'            if minNonNulls is specified, how is ignored.
#' @param minNonNulls If specified, drop rows that have less than
#'                    minNonNulls non-null values.
#'                    This overwrites the how parameter.
#' @param cols Optional list of column names to consider.
#' @return A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname nafunctions
#' @name dropna
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlCtx, path)
#' dropna(df)
#' }
setMethod("dropna",
          signature(x = "SparkDataFrame"),
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
                               as.integer(minNonNulls), as.list(cols))
            dataFrame(sdf)
          })

#' @rdname nafunctions
#' @name na.omit
#' @export
setMethod("na.omit",
          signature(object = "SparkDataFrame"),
          function(object, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
            dropna(object, how, minNonNulls, cols)
          })

#' fillna
#'
#' Replace null values.
#'
#' @param x A SparkDataFrame.
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
#'
#' @rdname nafunctions
#' @name fillna
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlCtx, path)
#' fillna(df, 1)
#' fillna(df, list("age" = 20, "name" = "unknown"))
#' }
setMethod("fillna",
          signature(x = "SparkDataFrame"),
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
              # Check each item in the named list is of valid type
              lapply(value, function(v) {
                if (!(class(v) %in% c("integer", "numeric", "character"))) {
                  stop("Each item in value should be an integer, numeric or charactor.")
                }
              })

              # Convert to the named list to an environment to be passed to JVM
              valueMap <- convertNamedListToEnv(value)

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
              callJMethod(naFunctions, "fill", value, as.list(cols))
            }
            dataFrame(sdf)
          })

#' This function downloads the contents of a SparkDataFrame into an R's data.frame.
#' Since data.frames are held in memory, ensure that you have enough memory
#' in your system to accommodate the contents.
#'
#' @title Download data from a SparkDataFrame into a data.frame
#' @param x a SparkDataFrame
#' @return a data.frame
#' @family SparkDataFrame functions
#' @rdname as.data.frame
#' @examples \dontrun{
#'
#' irisDF <- createDataFrame(sqlContext, iris)
#' df <- as.data.frame(irisDF[irisDF$Species == "setosa", ])
#' }
setMethod("as.data.frame",
          signature(x = "SparkDataFrame"),
          function(x, row.names = NULL, optional = FALSE, ...) {
            as.data.frame(collect(x), row.names, optional, ...)
          })

#' The specified SparkDataFrame is attached to the R search path. This means that
#' the SparkDataFrame is searched by R when evaluating a variable, so columns in
#' the SparkDataFrame can be accessed by simply giving their names.
#'
#' @family SparkDataFrame functions
#' @rdname attach
#' @title Attach SparkDataFrame to R search path
#' @param what (SparkDataFrame) The SparkDataFrame to attach
#' @param pos (integer) Specify position in search() where to attach.
#' @param name (character) Name to use for the attached SparkDataFrame. Names
#'   starting with package: are reserved for library.
#' @param warn.conflicts (logical) If TRUE, warnings are printed about conflicts
#' from attaching the database, unless that SparkDataFrame contains an object
#' @examples
#' \dontrun{
#' attach(irisDf)
#' summary(Sepal_Width)
#' }
#' @seealso \link{detach}
setMethod("attach",
          signature(what = "SparkDataFrame"),
          function(what, pos = 2, name = deparse(substitute(what)), warn.conflicts = TRUE) {
            newEnv <- assignNewEnv(what)
            attach(newEnv, pos = pos, name = name, warn.conflicts = warn.conflicts)
          })

#' Evaluate a R expression in an environment constructed from a SparkDataFrame
#' with() allows access to columns of a SparkDataFrame by simply referring to
#' their name. It appends every column of a SparkDataFrame into a new
#' environment. Then, the given expression is evaluated in this new
#' environment.
#'
#' @rdname with
#' @title Evaluate a R expression in an environment constructed from a SparkDataFrame
#' @param data (SparkDataFrame) SparkDataFrame to use for constructing an environment.
#' @param expr (expression) Expression to evaluate.
#' @param ... arguments to be passed to future methods.
#' @examples
#' \dontrun{
#' with(irisDf, nrow(Sepal_Width))
#' }
#' @seealso \link{attach}
setMethod("with",
          signature(data = "SparkDataFrame"),
          function(data, expr, ...) {
            newEnv <- assignNewEnv(data)
            eval(substitute(expr), envir = newEnv, enclos = newEnv)
          })

#' Display the structure of a SparkDataFrame, including column names, column types, as well as a
#' a small sample of rows.
#' @name str
#' @title Compactly display the structure of a dataset
#' @rdname str
#' @family SparkDataFrame functions
#' @param object a SparkDataFrame
#' @examples \dontrun{
#' # Create a SparkDataFrame from the Iris dataset
#' irisDF <- createDataFrame(sqlContext, iris)
#'
#' # Show the structure of the SparkDataFrame
#' str(irisDF)
#' }
setMethod("str",
          signature(object = "SparkDataFrame"),
          function(object) {

            # TODO: These could be made global parameters, though in R it's not the case
            MAX_CHAR_PER_ROW <- 120
            MAX_COLS <- 100

            # Get the column names and types of the DataFrame
            names <- names(object)
            types <- coltypes(object)

            # Get the first elements of the dataset. Limit number of columns accordingly
            localDF <- if (ncol(object) > MAX_COLS) {
              head(object[, c(1:MAX_COLS)])
            } else {
              head(object)
            }

            # The number of observations will not be displayed as computing the
            # number of rows is a very expensive operation
            cat(paste0("'", class(object), "': ", length(names), " variables:\n"))

            if (nrow(localDF) > 0) {
              for (i in 1 : ncol(localDF)) {
                # Get the first elements for each column

                firstElements <- if (types[i] == "character") {
                  paste(paste0("\"", localDF[, i], "\""), collapse = " ")
                } else {
                  paste(localDF[, i], collapse = " ")
                }

                # Add the corresponding number of spaces for alignment
                spaces <- paste(rep(" ", max(nchar(names) - nchar(names[i]))), collapse = "")

                # Get the short type. For 'character', it would be 'chr';
                # 'for numeric', it's 'num', etc.
                dataType <- SHORT_TYPES[[types[i]]]
                if (is.null(dataType)) {
                  dataType <- substring(types[i], 1, 3)
                }

                # Concatenate the colnames, coltypes, and first
                # elements of each column
                line <- paste0(" $ ", names[i], spaces, ": ",
                               dataType, " ", firstElements)

                # Chop off extra characters if this is too long
                cat(substr(line, 1, MAX_CHAR_PER_ROW))
                cat("\n")
              }

              if (ncol(localDF) < ncol(object)) {
                cat(paste0("\nDisplaying first ", ncol(localDF), " columns only."))
              }
            }
          })

#' drop
#'
#' Returns a new SparkDataFrame with columns dropped.
#' This is a no-op if schema doesn't contain column name(s).
#'
#' @param x A SparkDataFrame.
#' @param cols A character vector of column names or a Column.
#' @return A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname drop
#' @name drop
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlCtx, path)
#' drop(df, "col1")
#' drop(df, c("col1", "col2"))
#' drop(df, df$col1)
#' }
setMethod("drop",
          signature(x = "SparkDataFrame"),
          function(x, col) {
            stopifnot(class(col) == "character" || class(col) == "Column")

            if (class(col) == "Column") {
              sdf <- callJMethod(x@sdf, "drop", col@jc)
            } else {
              sdf <- callJMethod(x@sdf, "drop", as.list(col))
            }
            dataFrame(sdf)
          })

# Expose base::drop
setMethod("drop",
          signature(x = "ANY"),
          function(x) {
            base::drop(x)
          })

#' This function computes a histogram for a given SparkR Column.
#' 
#' @name histogram
#' @title Histogram
#' @param nbins the number of bins (optional). Default value is 10.
#' @param df the SparkDataFrame containing the Column to build the histogram from.
#' @param colname the name of the column to build the histogram from.
#' @return a data.frame with the histogram statistics, i.e., counts and centroids.
#' @rdname histogram
#' @family SparkDataFrame functions
#' @export
#' @examples 
#' \dontrun{
#' 
#' # Create a SparkDataFrame from the Iris dataset
#' irisDF <- createDataFrame(sqlContext, iris)
#' 
#' # Compute histogram statistics
#' histStats <- histogram(irisDF, irisDF$Sepal_Length, nbins = 12)
#'
#' # Once SparkR has computed the histogram statistics, the histogram can be
#' # rendered using the ggplot2 library:
#'
#' require(ggplot2)
#' plot <- ggplot(histStats, aes(x = centroids, y = counts)) +
#'         geom_bar(stat = "identity") +
#'         xlab("Sepal_Length") + ylab("Frequency")   
#' } 
setMethod("histogram",
          signature(df = "SparkDataFrame", col = "characterOrColumn"),
          function(df, col, nbins = 10) {
            # Validate nbins
            if (nbins < 2) {
              stop("The number of bins must be a positive integer number greater than 1.")
            }

            # Round nbins to the smallest integer
            nbins <- floor(nbins)

            # Validate col
            if (is.null(col)) {
              stop("col must be specified.")
            }

            colname <- col
            x <- if (class(col) == "character") {
              if (!colname %in% names(df)) {
                stop("Specified colname does not belong to the given SparkDataFrame.")
              }

              # Filter NA values in the target column and remove all other columns
              df <- na.omit(df[, colname, drop = F])
              getColumn(df, colname)

            } else if (class(col) == "Column") {

              # The given column needs to be appended to the SparkDataFrame so that we can
              # use method describe() to compute statistics in one single pass. The new
              # column must have a name that doesn't exist in the dataset.
              # To do so, we generate a random column name with more characters than the
              # longest colname in the dataset, but no more than 100 (think of a UUID).
              # This column name will never be visible to the user, so the name is irrelevant.
              # Limiting the colname length to 100 makes debugging easier and it does
              # introduce a negligible probability of collision: assuming the user has 1 million
              # columns AND all of them have names 100 characters long (which is very unlikely),
              # AND they run 1 billion histograms, the probability of collision will roughly be
              # 1 in 4.4 x 10 ^ 96
              colname <- paste(base:::sample(c(letters, LETTERS),
                                             size = min(max(nchar(colnames(df))) + 1, 100),
                                             replace = TRUE),
                               collapse = "")

              # Append the given column to the dataset. This is to support Columns that
              # don't belong to the SparkDataFrame but are rather expressions
              df <- withColumn(df, colname, col)

              # Filter NA values in the target column. Cannot remove all other columns
              # since given Column may be an expression on one or more existing columns
              df <- na.omit(df)

              col
            }

            stats <- collect(describe(df[, colname, drop = F]))
            min <- as.numeric(stats[4, 2])
            max <- as.numeric(stats[5, 2])

            # Normalize the data
            xnorm <- (x - min) / (max - min)

            # Round the data to 4 significant digits. This is to avoid rounding issues.
            xnorm <- cast(xnorm * 10000, "integer") / 10000.0

            # Since min = 0, max = 1 (data is already normalized)
            normBinSize <- 1 / nbins
            binsize <- (max - min) / nbins
            approxBins <- xnorm / normBinSize

            # Adjust values that are equal to the upper bound of each bin
            bins <- cast(approxBins -
                           ifelse(approxBins == cast(approxBins, "integer") & x != min, 1, 0),
                         "integer")

            df$bins <- bins
            histStats <- collect(count(groupBy(df, "bins")))
            names(histStats) <- c("bins", "counts")

            # Fill bins with zero counts
            y <- data.frame("bins" = seq(0, nbins - 1))
            histStats <- merge(histStats, y, all.x = T, all.y = T)
            histStats[is.na(histStats$count), 2] <- 0

            # Compute centroids
            histStats$centroids <- histStats$bins * binsize + min + binsize / 2

            # Return the statistics
            return(histStats)
          })

#' Saves the content of the SparkDataFrame to an external database table via JDBC
#'
#' Additional JDBC database connection properties can be set (...)
#'
#' Also, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes: \cr
#'  append: Contents of this SparkDataFrame are expected to be appended to existing data. \cr
#'  overwrite: Existing data is expected to be overwritten by the contents of this
#'     SparkDataFrame. \cr
#'  error: An exception is expected to be thrown. \cr
#'  ignore: The save operation is expected to not save the contents of the SparkDataFrame
#'     and to not change the existing data. \cr
#'
#' @param x A SparkDataFrame
#' @param url JDBC database url of the form `jdbc:subprotocol:subname`
#' @param tableName The name of the table in the external database
#' @param mode One of 'append', 'overwrite', 'error', 'ignore' save mode (it is 'error' by default)
#' @family SparkDataFrame functions
#' @rdname write.jdbc
#' @name write.jdbc
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' jdbcUrl <- "jdbc:mysql://localhost:3306/databasename"
#' write.jdbc(df, jdbcUrl, "table", user = "username", password = "password")
#' }
setMethod("write.jdbc",
          signature(x = "SparkDataFrame", url = "character", tableName = "character"),
          function(x, url, tableName, mode = "error", ...){
            jmode <- convertToJSaveMode(mode)
            jprops <- varargsToJProperties(...)
            write <- callJMethod(x@sdf, "write")
            write <- callJMethod(write, "mode", jmode)
            invisible(callJMethod(write, "jdbc", url, tableName, jprops))
          })

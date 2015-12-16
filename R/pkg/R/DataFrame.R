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
#' @description DataFrames can be created using functions like \link{createDataFrame},
#'              \link{read.json}, \link{table} etc.
#' @family DataFrame functions
#' @rdname DataFrame
#' @docType class
#'
#' @slot env An R environment that stores bookkeeping states of the DataFrame
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x, numRows = 20, truncate = TRUE) {
            s <- callJMethod(x@sdf, "showString", numToInt(numRows), truncate)
            cat(s)
          })

#' show
#'
#' Print the DataFrame column names and types
#'
#' @param x A SparkSQL DataFrame
#'
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            sapply(schema(x)$fields(), function(f) {
              f$name()
            })
          })

#' @rdname columns
#' @name names
setMethod("names",
          signature(x = "DataFrame"),
          function(x) {
            columns(x)
          })

#' @rdname columns
#' @name names<-
setMethod("names<-",
          signature(x = "DataFrame"),
          function(x, value) {
            if (!is.null(value)) {
              sdf <- callJMethod(x@sdf, "toDF", as.list(value))
              dataFrame(sdf)
            }
          })

#' @rdname columns
#' @name colnames
setMethod("colnames",
          signature(x = "DataFrame"),
          function(x) {
            columns(x)
          })

#' @rdname columns
#' @name colnames<-
setMethod("colnames<-",
          signature(x = "DataFrame", value = "character"),
          function(x, value) {
            sdf <- callJMethod(x@sdf, "toDF", as.list(value))
            dataFrame(sdf)
          })

#' coltypes
#'
#' Get column types of a DataFrame
#'
#' @param x A SparkSQL DataFrame
#' @return value A character vector with the column types of the given DataFrame
#' @rdname coltypes
#' @name coltypes
#' @family DataFrame functions
#' @export
#' @examples
#'\dontrun{
#' irisDF <- createDataFrame(sqlContext, iris)
#' coltypes(irisDF)
#'}
setMethod("coltypes",
          signature(x = "DataFrame"),
          function(x) {
            # Get the data types of the DataFrame by invoking dtypes() function
            types <- sapply(dtypes(x), function(x) {x[[2]]})

            # Map Spark data types into R's data types using DATA_TYPES environment
            rTypes <- sapply(types, USE.NAMES=F, FUN=function(x) {
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
#' Set the column types of a DataFrame.
#'
#' @param x A SparkSQL DataFrame
#' @param value A character vector with the target column types for the given
#'    DataFrame. Column types can be one of integer, numeric/double, character, logical, or NA
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
          signature(x = "DataFrame", value = "character"),
          function(x, value) {
            cols <- columns(x)
            ncols <- length(cols)
            if (length(value) == 0) {
              stop("Cannot set types of an empty DataFrame with no Column")
            }
            if (length(value) != ncols) {
              stop("Length of type vector should match the number of columns for DataFrame")
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
#' Registers a DataFrame as a Temporary Table in the SQLContext
#'
#' @param x A SparkSQL DataFrame
#' @param tableName A character vector containing the name of the table
#'
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' \url{http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence}.
#'
#' @param x The DataFrame to persist
#'
#' @family DataFrame functions
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
#'
#' @family DataFrame functions
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
#'
#' @family DataFrame functions
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
#'}
setMethod("repartition",
          signature(x = "DataFrame", numPartitions = "numeric"),
          function(x, numPartitions) {
            sdf <- callJMethod(x@sdf, "repartition", numToInt(numPartitions))
            dataFrame(sdf)
          })

#' toJSON
#'
#' Convert the rows of a DataFrame into JSON objects and return an RDD where
#' each element contains a JSON string.
#'
#' @param x A SparkSQL DataFrame
#' @return A StringRRDD of JSON objects
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            rdd <- callJMethod(x@sdf, "toJSON")
            jrdd <- callJMethod(rdd, "toJavaRDD")
            RDD(jrdd, serializedMode = "string")
          })

#' write.json
#'
#' Save the contents of a DataFrame as a JSON file (one object per line). Files written out
#' with this method can be read back in as a DataFrame using read.json().
#'
#' @param x A SparkSQL DataFrame
#' @param path The directory where the file is saved
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame", path = "character"),
          function(x, path) {
            write <- callJMethod(x@sdf, "write")
            invisible(callJMethod(write, "json", path))
          })

#' write.parquet
#'
#' Save the contents of a DataFrame as a Parquet file, preserving the schema. Files written out
#' with this method can be read back in as a DataFrame using read.parquet().
#'
#' @param x A SparkSQL DataFrame
#' @param path The directory where the file is saved
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame", path = "character"),
          function(x, path) {
            write <- callJMethod(x@sdf, "write")
            invisible(callJMethod(write, "parquet", path))
          })

#' @rdname write.parquet
#' @name saveAsParquetFile
#' @export
setMethod("saveAsParquetFile",
          signature(x = "DataFrame", path = "character"),
          function(x, path) {
            .Deprecated("write.parquet")
            write.parquet(x, path)
          })

#' Distinct
#'
#' Return a new DataFrame containing the distinct rows in this DataFrame.
#'
#' @param x A SparkSQL DataFrame
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "distinct")
            dataFrame(sdf)
          })

#' @rdname distinct
#' @name unique
setMethod("unique",
          signature(x = "DataFrame"),
          function(x) {
            distinct(x)
          })

#' Sample
#'
#' Return a sampled subset of this DataFrame using a random seed.
#'
#' @param x A SparkSQL DataFrame
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame", withReplacement = "logical",
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
          signature(x = "DataFrame", withReplacement = "logical",
                    fraction = "numeric"),
          function(x, withReplacement, fraction, seed) {
            sample(x, withReplacement, fraction, seed)
          })

#' nrow
#'
#' Returns the number of rows in a DataFrame
#'
#' @param x A SparkSQL DataFrame
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            callJMethod(x@sdf, "count")
          })

#' @name nrow
#' @rdname nrow
setMethod("nrow",
          signature(x = "DataFrame"),
          function(x) {
            count(x)
          })

#' Returns the number of columns in a DataFrame
#'
#' @param x a SparkSQL DataFrame
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            length(columns(x))
          })

#' Returns the dimentions (number of rows and columns) of a DataFrame
#' @param x a SparkSQL DataFrame
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            c(count(x), ncol(x))
          })

#' Collects all the elements of a Spark DataFrame and coerces them into an R data.frame.
#'
#' @param x A SparkSQL DataFrame
#' @param stringsAsFactors (Optional) A logical indicating whether or not string columns
#' should be converted to factors. FALSE by default.
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
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
#' Limit the resulting DataFrame to the number of rows specified.
#'
#' @param x A SparkSQL DataFrame
#' @param num The number of rows to return
#' @return A new DataFrame containing the number of rows specified.
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame", num = "numeric"),
          function(x, num) {
            res <- callJMethod(x@sdf, "limit", as.integer(num))
            dataFrame(res)
          })

#' Take the first NUM rows of a DataFrame and return a the results as a data.frame
#'
#' @family DataFrame functions
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
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x, num = 6L) {
          # Default num is 6L in keeping with R's data.frame convention
            take(x, num)
          })

#' Return the first row of a DataFrame
#'
#' @param x A SparkSQL DataFrame
#'
#' @family DataFrame functions
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
          signature(x = "DataFrame"),
          function(x) {
            take(x, 1)
          })

#' toRDD
#'
#' Converts a Spark DataFrame to an RDD while preserving column names.
#'
#' @param x A Spark DataFrame
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
#' @family DataFrame functions
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
           signature(x = "DataFrame"),
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
          signature(x = "DataFrame"),
          function(x, ...) {
            groupBy(x, ...)
          })

#' Summarize data across columns
#'
#' Compute aggregates by specifying a list of columns
#'
#' @param x a DataFrame
#' @family DataFrame functions
#' @rdname agg
#' @name agg
#' @export
setMethod("agg",
          signature(x = "DataFrame"),
          function(x, ...) {
            agg(groupBy(x), ...)
          })

#' @rdname agg
#' @name summarize
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

#' @rdname lapply
#' @noRd
setMethod("lapply",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapply(rdd, FUN)
          })

#' @rdname lapply
#' @noRd
setMethod("map",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' @rdname flatMap
#' @noRd
setMethod("flatMap",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            flatMap(rdd, FUN)
          })

#' @rdname lapplyPartition
#' @noRd
setMethod("lapplyPartition",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapplyPartition(rdd, FUN)
          })

#' @rdname lapplyPartition
#' @noRd
setMethod("mapPartitions",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

#' @rdname foreach
#' @noRd
setMethod("foreach",
          signature(x = "DataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreach(rdd, func)
          })

#' @rdname foreach
#' @noRd
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
#' @name $
setMethod("$", signature(x = "DataFrame"),
          function(x, name) {
            getColumn(x, name)
          })

#' @rdname select
#' @name $<-
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

setClassUnion("numericOrcharacter", c("numeric", "character"))

#' @rdname subset
#' @name [[
setMethod("[[", signature(x = "DataFrame", i = "numericOrcharacter"),
          function(x, i) {
            if (is.numeric(i)) {
              cols <- columns(x)
              i <- cols[[i]]
            }
            getColumn(x, i)
          })

#' @rdname subset
#' @name [
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

#' @rdname subset
#' @name [
setMethod("[", signature(x = "DataFrame", i = "Column"),
          function(x, i, j, ...) {
            # It could handle i as "character" but it seems confusing and not required
            # https://stat.ethz.ch/R-manual/R-devel/library/base/html/Extract.data.frame.html
            filtered <- filter(x, i)
            if (!missing(j)) {
              filtered[, j, ...]
            } else {
              filtered
            }
          })

#' Subset
#'
#' Return subsets of DataFrame according to given conditions
#' @param x A DataFrame
#' @param subset (Optional) A logical expression to filter on rows
#' @param select expression for the single Column or a list of columns to select from the DataFrame
#' @return A new DataFrame containing only the rows that meet the condition with selected columns
#' @export
#' @family DataFrame functions
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
#'   # DataFrame can be subset on both rows and Columns
#'   df[df$name == "Smith", c(1,2)]
#'   df[df$age %in% c(19, 30), 1:2]
#'   subset(df, df$age %in% c(19, 30), 1:2)
#'   subset(df, df$age %in% c(19), select = c(1,2))
#'   subset(df, select = c(1,2))
#' }
setMethod("subset", signature(x = "DataFrame"),
          function(x, subset, select, ...) {
            if (missing(subset)) {
              x[, select, ...]
            } else {
              x[subset, select, ...]
            }
          })

#' Select
#'
#' Selects a set of columns with names or Column expressions.
#' @param x A DataFrame
#' @param col A list of columns or single Column or name
#' @return A new DataFrame with selected columns
#' @export
#' @family DataFrame functions
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
setMethod("select", signature(x = "DataFrame", col = "character"),
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

#' @family DataFrame functions
#' @rdname select
#' @export
setMethod("select", signature(x = "DataFrame", col = "Column"),
          function(x, col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            sdf <- callJMethod(x@sdf, "select", jcols)
            dataFrame(sdf)
          })

#' @family DataFrame functions
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
            sdf <- callJMethod(x@sdf, "select", cols)
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
#' @family DataFrame functions
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
          signature(x = "DataFrame", expr = "character"),
          function(x, expr, ...) {
            exprList <- list(expr, ...)
            sdf <- callJMethod(x@sdf, "selectExpr", exprList)
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
#' @family DataFrame functions
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
#' @param .data A DataFrame
#' @param col a named argument of the form name = col
#' @return A new DataFrame with the new columns added.
#' @family DataFrame functions
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
#' }
setMethod("mutate",
          signature(.data = "DataFrame"),
          function(.data, ...) {
            x <- .data
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

#' @export
#' @rdname mutate
#' @name transform
setMethod("transform",
          signature(`_data` = "DataFrame"),
          function(`_data`, ...) {
            mutate(`_data`, ...)
          })

#' rename
#'
#' Rename an existing column in a DataFrame.
#'
#' @param x A DataFrame
#' @param existingCol The name of the column you want to change.
#' @param newCol The new column name.
#' @return A DataFrame with the column name changed.
#' @family DataFrame functions
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
#' @param col A character or Column object vector indicating the fields to sort on
#' @param ... Additional sorting fields
#' @param decreasing A logical argument indicating sorting order for columns when
#'                   a character vector is specified for col
#' @return A DataFrame where all elements are sorted.
#' @family DataFrame functions
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
          signature(x = "DataFrame", col = "Column"),
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
          signature(x = "DataFrame", col = "character"),
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
#' @family DataFrame functions
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
          signature(x = "DataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            if (class(condition) == "Column") {
              condition <- condition@jc
            }
            sdf <- callJMethod(x@sdf, "filter", condition)
            dataFrame(sdf)
          })

#' @family DataFrame functions
#' @rdname filter
#' @name where
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
#' Column expression. If joinExpr is omitted, join() will perform a Cartesian join
#' @param joinType The type of join to perform. The following join types are available:
#' 'inner', 'outer', 'full', 'fullouter', leftouter', 'left_outer', 'left',
#' 'right_outer', 'rightouter', 'right', and 'leftsemi'. The default joinType is "inner".
#' @return A DataFrame containing the result of the join operation.
#' @family DataFrame functions
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
          signature(x = "DataFrame", y = "DataFrame"),
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
#' @family DataFrame functions
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
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y, by = intersect(names(x), names(y)), by.x = by, by.y = by,
                   all = FALSE, all.x = all, all.y = all,
                   sort = TRUE, suffixes = c("_x","_y"), ... ) {

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
#' @param x a DataFrame on which the
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
#' Return a new DataFrame containing the union of rows in this DataFrame
#' and another DataFrame. This is equivalent to `UNION ALL` in SQL.
#' Note that this does not remove duplicate rows across the two DataFrames.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @return A DataFrame containing the result of the union.
#' @family DataFrame functions
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
          signature(x = "DataFrame", y = "DataFrame"),
          function(x, y) {
            unioned <- callJMethod(x@sdf, "unionAll", y@sdf)
            dataFrame(unioned)
          })

#' @title Union two or more DataFrames
#' @description Returns a new DataFrame containing rows of all parameters.
#'
#' @rdname rbind
#' @name rbind
#' @export
setMethod("rbind",
          signature(... = "DataFrame"),
          function(x, ..., deparse.level = 1) {
            if (nargs() == 3) {
              unionAll(x, ...)
            } else {
              unionAll(x, Recall(..., deparse.level = 1))
            }
          })

#' Intersect
#'
#' Return a new DataFrame containing rows only in both this DataFrame
#' and another DataFrame. This is equivalent to `INTERSECT` in SQL.
#'
#' @param x A Spark DataFrame
#' @param y A Spark DataFrame
#' @return A DataFrame containing the result of the intersect.
#' @family DataFrame functions
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
#' @family DataFrame functions
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
#' data already exists in the data source. There are four modes: \cr
#'  append: Contents of this DataFrame are expected to be appended to existing data. \cr
#'  overwrite: Existing data is expected to be overwritten by the contents of this DataFrame. \cr
#'  error: An exception is expected to be thrown. \cr
#'  ignore: The save operation is expected to not save the contents of the DataFrame
#'     and to not change the existing data. \cr
#'
#' @param df A SparkSQL DataFrame
#' @param path A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore' save mode (it is 'error' by default)
#'
#' @family DataFrame functions
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
          signature(df = "DataFrame", path = "character"),
          function(df, path, source = NULL, mode = "error", ...){
            if (is.null(source)) {
              sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                    "org.apache.spark.sql.parquet")
            }
            allModes <- c("append", "overwrite", "error", "ignore")
            # nolint start
            if (!(mode %in% allModes)) {
              stop('mode should be one of "append", "overwrite", "error", "ignore"')
            }
            # nolint end
            jmode <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "saveMode", mode)
            options <- varargsToEnv(...)
            if (!is.null(path)) {
                options[["path"]] <- path
            }
            callJMethod(df@sdf, "save", source, jmode, options)
          })

#' @rdname write.df
#' @name saveDF
#' @export
setMethod("saveDF",
          signature(df = "DataFrame", path = "character"),
          function(df, path, source = NULL, mode = "error", ...){
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
#' data already exists in the data source. There are four modes: \cr
#'  append: Contents of this DataFrame are expected to be appended to existing data. \cr
#'  overwrite: Existing data is expected to be overwritten by the contents of this DataFrame. \cr
#'  error: An exception is expected to be thrown. \cr
#'  ignore: The save operation is expected to not save the contents of the DataFrame
#'     and to not change the existing data. \cr
#'
#' @param df A SparkSQL DataFrame
#' @param tableName A name for the table
#' @param source A name for external data source
#' @param mode One of 'append', 'overwrite', 'error', 'ignore' save mode (it is 'error' by default)
#'
#' @family DataFrame functions
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
          signature(df = "DataFrame", tableName = "character", source = "character",
                    mode = "character"),
          function(df, tableName, source = NULL, mode="error", ...){
            if (is.null(source)) {
              sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
              source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                                    "org.apache.spark.sql.parquet")
            }
            allModes <- c("append", "overwrite", "error", "ignore")
            # nolint start
            if (!(mode %in% allModes)) {
              stop('mode should be one of "append", "overwrite", "error", "ignore"')
            }
            # nolint end
            jmode <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "saveMode", mode)
            options <- varargsToEnv(...)
            callJMethod(df@sdf, "saveAsTable", tableName, source, jmode, options)
          })

#' summary
#'
#' Computes statistics for numeric columns.
#' If no columns are given, this function computes statistics for all numerical columns.
#'
#' @param x A DataFrame to be computed.
#' @param col A string of name
#' @param ... Additional expressions
#' @return A DataFrame
#' @family DataFrame functions
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
          signature(x = "DataFrame", col = "character"),
          function(x, col, ...) {
            colList <- list(col, ...)
            sdf <- callJMethod(x@sdf, "describe", colList)
            dataFrame(sdf)
          })

#' @rdname summary
#' @name describe
setMethod("describe",
          signature(x = "DataFrame"),
          function(x) {
            colList <- as.list(c(columns(x)))
            sdf <- callJMethod(x@sdf, "describe", colList)
            dataFrame(sdf)
          })

#' @rdname summary
#' @name summary
setMethod("summary",
          signature(object = "DataFrame"),
          function(object, ...) {
            describe(object)
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
#' @family DataFrame functions
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
                               as.integer(minNonNulls), as.list(cols))
            dataFrame(sdf)
          })

#' @rdname nafunctions
#' @name na.omit
#' @export
setMethod("na.omit",
          signature(object = "DataFrame"),
          function(object, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
            dropna(object, how, minNonNulls, cols)
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

#' This function downloads the contents of a DataFrame into an R's data.frame.
#' Since data.frames are held in memory, ensure that you have enough memory
#' in your system to accommodate the contents.
#'
#' @title Download data from a DataFrame into a data.frame
#' @param x a DataFrame
#' @return a data.frame
#' @family DataFrame functions
#' @rdname as.data.frame
#' @examples \dontrun{
#'
#' irisDF <- createDataFrame(sqlContext, iris)
#' df <- as.data.frame(irisDF[irisDF$Species == "setosa", ])
#' }
setMethod("as.data.frame",
          signature(x = "DataFrame"),
          function(x, ...) {
            # Check if additional parameters have been passed
            if (length(list(...)) > 0) {
              stop(paste("Unused argument(s): ", paste(list(...), collapse=", ")))
            }
            collect(x)
          })

#' The specified DataFrame is attached to the R search path. This means that
#' the DataFrame is searched by R when evaluating a variable, so columns in
#' the DataFrame can be accessed by simply giving their names.
#'
#' @family DataFrame functions
#' @rdname attach
#' @title Attach DataFrame to R search path
#' @param what (DataFrame) The DataFrame to attach
#' @param pos (integer) Specify position in search() where to attach.
#' @param name (character) Name to use for the attached DataFrame. Names
#'   starting with package: are reserved for library.
#' @param warn.conflicts (logical) If TRUE, warnings are printed about conflicts
#' from attaching the database, unless that DataFrame contains an object
#' @examples
#' \dontrun{
#' attach(irisDf)
#' summary(Sepal_Width)
#' }
#' @seealso \link{detach}
setMethod("attach",
          signature(what = "DataFrame"),
          function(what, pos = 2, name = deparse(substitute(what)), warn.conflicts = TRUE) {
            newEnv <- assignNewEnv(what)
            attach(newEnv, pos = pos, name = name, warn.conflicts = warn.conflicts)
          })

#' Evaluate a R expression in an environment constructed from a DataFrame
#' with() allows access to columns of a DataFrame by simply referring to
#' their name. It appends every column of a DataFrame into a new
#' environment. Then, the given expression is evaluated in this new
#' environment.
#'
#' @rdname with
#' @title Evaluate a R expression in an environment constructed from a DataFrame
#' @param data (DataFrame) DataFrame to use for constructing an environment.
#' @param expr (expression) Expression to evaluate.
#' @param ... arguments to be passed to future methods.
#' @examples
#' \dontrun{
#' with(irisDf, nrow(Sepal_Width))
#' }
#' @seealso \link{attach}
setMethod("with",
          signature(data = "DataFrame"),
          function(data, expr, ...) {
            newEnv <- assignNewEnv(data)
            eval(substitute(expr), envir = newEnv, enclos = newEnv)
          })

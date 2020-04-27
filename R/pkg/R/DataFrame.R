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

#' S4 class that represents a SparkDataFrame
#'
#' SparkDataFrames can be created using functions like \link{createDataFrame},
#' \link{read.json}, \link{table} etc.
#'
#' @family SparkDataFrame functions
#' @rdname SparkDataFrame
#' @docType class
#'
#' @slot env An R environment that stores bookkeeping states of the SparkDataFrame
#' @slot sdf A Java object reference to the backing Scala DataFrame
#' @seealso \link{createDataFrame}, \link{read.json}, \link{table}
#' @seealso \url{https://spark.apache.org/docs/latest/sparkr.html#sparkr-dataframes}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createDataFrame(faithful)
#'}
#' @note SparkDataFrame since 2.0.0
setClass("SparkDataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "SparkDataFrame", function(.Object, sdf, isCached) {
  .Object@env <- new.env()
  .Object@env$isCached <- isCached

  .Object@sdf <- sdf
  .Object
})

#' Set options/mode and then return the write object
#' @noRd
setWriteOptions <- function(write, path = NULL, mode = "error", ...) {
  options <- varargsToStrEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  write <- setWriteMode(write, mode)
  write <- callJMethod(write, "options", options)
  write
}

#' Set mode and then return the write object
#' @noRd
setWriteMode <- function(write, mode) {
  if (!is.character(mode)) {
    stop("mode should be character or omitted. It is 'error' by default.")
  }
  write <- handledCallJMethod(write, "mode", mode)
  write
}

#' @param sdf A Java object reference to the backing Scala DataFrame
#' @param isCached TRUE if the SparkDataFrame is cached
#' @noRd
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
#' @aliases printSchema,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' printSchema(df)
#'}
#' @note printSchema since 1.4.0
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
#' @aliases schema,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' dfSchema <- schema(df)
#'}
#' @note schema since 1.4.0
setMethod("schema",
          signature(x = "SparkDataFrame"),
          function(x) {
            structType(callJMethod(x@sdf, "schema"))
          })

#' Explain
#'
#' Print the logical and physical Catalyst plans to the console for debugging.
#'
#' @family SparkDataFrame functions
#' @aliases explain,SparkDataFrame-method
#' @rdname explain
#' @name explain
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' explain(df, TRUE)
#'}
#' @note explain since 1.4.0
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
#' Returns True if the \code{collect} and \code{take} methods can be run locally
#' (without any Spark executors).
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname isLocal
#' @name isLocal
#' @aliases isLocal,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' isLocal(df)
#'}
#' @note isLocal since 1.4.0
setMethod("isLocal",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(x@sdf, "isLocal")
          })

#' showDF
#'
#' Print the first numRows rows of a SparkDataFrame
#'
#' @param x a SparkDataFrame.
#' @param numRows the number of rows to print. Defaults to 20.
#' @param truncate whether truncate long strings. If \code{TRUE}, strings more than
#'                 20 characters will be truncated. However, if set greater than zero,
#'                 truncates strings longer than \code{truncate} characters and all cells
#'                 will be aligned right.
#' @param vertical whether print output rows vertically (one line per column value).
#' @param ... further arguments to be passed to or from other methods.
#' @family SparkDataFrame functions
#' @aliases showDF,SparkDataFrame-method
#' @rdname showDF
#' @name showDF
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' showDF(df)
#'}
#' @note showDF since 1.4.0
setMethod("showDF",
          signature(x = "SparkDataFrame"),
          function(x, numRows = 20, truncate = TRUE, vertical = FALSE) {
            if (is.logical(truncate) && truncate) {
              s <- callJMethod(x@sdf, "showString", numToInt(numRows), numToInt(20), vertical)
            } else {
              truncate2 <- as.numeric(truncate)
              s <- callJMethod(x@sdf, "showString", numToInt(numRows), numToInt(truncate2),
                               vertical)
            }
            cat(s)
          })

#' show
#'
#' Print class and type information of a Spark object.
#'
#' @param object a Spark object. Can be a SparkDataFrame, Column, GroupedData, WindowSpec.
#'
#' @family SparkDataFrame functions
#' @rdname show
#' @aliases show,SparkDataFrame-method
#' @name show
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' show(df)
#'}
#' @note show(SparkDataFrame) since 1.4.0
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
#' @aliases dtypes,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' dtypes(df)
#'}
#' @note dtypes since 1.4.0
setMethod("dtypes",
          signature(x = "SparkDataFrame"),
          function(x) {
            lapply(schema(x)$fields(), function(f) {
              c(f$name(), f$dataType.simpleString())
            })
          })

#' Column Names of SparkDataFrame
#'
#' Return a vector of column names.
#'
#' @param x a SparkDataFrame.
#'
#' @family SparkDataFrame functions
#' @rdname columns
#' @name columns
#' @aliases columns,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' columns(df)
#' colnames(df)
#'}
#' @note columns since 1.4.0
setMethod("columns",
          signature(x = "SparkDataFrame"),
          function(x) {
            sapply(schema(x)$fields(), function(f) {
              f$name()
            })
          })

#' @rdname columns
#' @name names
#' @aliases names,SparkDataFrame-method
#' @note names since 1.5.0
setMethod("names",
          signature(x = "SparkDataFrame"),
          function(x) {
            columns(x)
          })

#' @rdname columns
#' @aliases names<-,SparkDataFrame-method
#' @name names<-
#' @note names<- since 1.5.0
setMethod("names<-",
          signature(x = "SparkDataFrame"),
          function(x, value) {
            colnames(x) <- value
            x
          })

#' @rdname columns
#' @aliases colnames,SparkDataFrame-method
#' @name colnames
#' @note colnames since 1.6.0
setMethod("colnames",
          signature(x = "SparkDataFrame"),
          function(x) {
            columns(x)
          })

#' @param value a character vector. Must have the same length as the number
#'              of columns to be renamed.
#' @rdname columns
#' @aliases colnames<-,SparkDataFrame-method
#' @name colnames<-
#' @note colnames<- since 1.6.0
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
              stop("Column names cannot contain the '.' symbol.")
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
#' @aliases coltypes,SparkDataFrame-method
#' @name coltypes
#' @family SparkDataFrame functions
#' @examples
#'\dontrun{
#' irisDF <- createDataFrame(iris)
#' coltypes(irisDF) # get column types
#'}
#' @note coltypes since 1.6.0
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
                  specialtype <- specialtypeshandle(x)
                  if (is.null(specialtype)) {
                    stop(paste("Unsupported data type: ", x))
                  }
                  type <- PRIMITIVE_TYPES[[specialtype]]
                }
              }
              type[[1]]
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
#' @param value A character vector with the target column types for the given
#'    SparkDataFrame. Column types can be one of integer, numeric/double, character, logical, or NA
#'    to keep that column as-is.
#' @rdname coltypes
#' @name coltypes<-
#' @aliases coltypes<-,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' coltypes(df) <- c("character", "integer") # set column types
#' coltypes(df) <- c(NA, "numeric") # set column types
#'}
#' @note coltypes<- since 1.6.0
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

#' Creates a temporary view using the given name.
#'
#' Creates a new temporary view using a SparkDataFrame in the Spark Session. If a
#' temporary view with the same name already exists, replaces it.
#'
#' @param x A SparkDataFrame
#' @param viewName A character vector containing the name of the table
#'
#' @family SparkDataFrame functions
#' @rdname createOrReplaceTempView
#' @name createOrReplaceTempView
#' @aliases createOrReplaceTempView,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "json_df")
#' new_df <- sql("SELECT * FROM json_df")
#'}
#' @note createOrReplaceTempView since 2.0.0
setMethod("createOrReplaceTempView",
          signature(x = "SparkDataFrame", viewName = "character"),
          function(x, viewName) {
              invisible(callJMethod(x@sdf, "createOrReplaceTempView", viewName))
          })

#' (Deprecated) Register Temporary Table
#'
#' Registers a SparkDataFrame as a Temporary Table in the SparkSession
#' @param x A SparkDataFrame
#' @param tableName A character vector containing the name of the table
#'
#' @seealso \link{createOrReplaceTempView}
#' @rdname registerTempTable-deprecated
#' @name registerTempTable
#' @aliases registerTempTable,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' registerTempTable(df, "json_df")
#' new_df <- sql("SELECT * FROM json_df")
#'}
#' @note registerTempTable since 1.4.0
setMethod("registerTempTable",
          signature(x = "SparkDataFrame", tableName = "character"),
          function(x, tableName) {
              .Deprecated("createOrReplaceTempView")
              invisible(callJMethod(x@sdf, "createOrReplaceTempView", tableName))
          })

#' insertInto
#'
#' Insert the contents of a SparkDataFrame into a table registered in the current SparkSession.
#'
#' @param x a SparkDataFrame.
#' @param tableName a character vector containing the name of the table.
#' @param overwrite a logical argument indicating whether or not to overwrite.
#' @param ... further arguments to be passed to or from other methods.
#' the existing rows in the table.
#'
#' @family SparkDataFrame functions
#' @rdname insertInto
#' @name insertInto
#' @aliases insertInto,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' df2 <- read.df(path2, "parquet")
#' saveAsTable(df, "table1")
#' insertInto(df2, "table1", overwrite = TRUE)
#'}
#' @note insertInto since 1.4.0
setMethod("insertInto",
          signature(x = "SparkDataFrame", tableName = "character"),
          function(x, tableName, overwrite = FALSE) {
            write <- callJMethod(x@sdf, "write")
            write <- setWriteMode(write, ifelse(overwrite, "overwrite", "append"))
            invisible(callJMethod(write, "insertInto", tableName))
          })

#' Cache
#'
#' Persist with the default storage level (MEMORY_ONLY).
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @aliases cache,SparkDataFrame-method
#' @rdname cache
#' @name cache
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' cache(df)
#'}
#' @note cache since 1.4.0
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
#' \url{http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence}.
#'
#' @param x the SparkDataFrame to persist.
#' @param newLevel storage level chosen for the persistence. See available options in
#'        the description.
#'
#' @family SparkDataFrame functions
#' @rdname persist
#' @name persist
#' @aliases persist,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' persist(df, "MEMORY_AND_DISK")
#'}
#' @note persist since 1.4.0
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
#' @param x the SparkDataFrame to unpersist.
#' @param blocking whether to block until all blocks are deleted.
#' @param ... further arguments to be passed to or from other methods.
#'
#' @family SparkDataFrame functions
#' @rdname unpersist
#' @aliases unpersist,SparkDataFrame-method
#' @name unpersist
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' persist(df, "MEMORY_AND_DISK")
#' unpersist(df)
#'}
#' @note unpersist since 1.4.0
setMethod("unpersist",
          signature(x = "SparkDataFrame"),
          function(x, blocking = TRUE) {
            callJMethod(x@sdf, "unpersist", blocking)
            x@env$isCached <- FALSE
            x
          })

#' StorageLevel
#'
#' Get storagelevel of this SparkDataFrame.
#'
#' @param x the SparkDataFrame to get the storageLevel.
#'
#' @family SparkDataFrame functions
#' @rdname storageLevel
#' @aliases storageLevel,SparkDataFrame-method
#' @name storageLevel
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' persist(df, "MEMORY_AND_DISK")
#' storageLevel(df)
#'}
#' @note storageLevel since 2.1.0
setMethod("storageLevel",
          signature(x = "SparkDataFrame"),
          function(x) {
            storageLevelToString(callJMethod(x@sdf, "storageLevel"))
          })

#' Coalesce
#'
#' Returns a new SparkDataFrame that has exactly \code{numPartitions} partitions.
#' This operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100
#' partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of
#' the current partitions. If a larger number of partitions is requested, it will stay at the
#' current number of partitions.
#'
#' However, if you're doing a drastic coalesce on a SparkDataFrame, e.g. to numPartitions = 1,
#' this may result in your computation taking place on fewer nodes than
#' you like (e.g. one node in the case of numPartitions = 1). To avoid this,
#' call \code{repartition}. This will add a shuffle step, but means the
#' current upstream partitions will be executed in parallel (per whatever
#' the current partitioning is).
#'
#' @param numPartitions the number of partitions to use.
#'
#' @family SparkDataFrame functions
#' @rdname coalesce
#' @name coalesce
#' @aliases coalesce,SparkDataFrame-method
#' @seealso \link{repartition}, \link{repartitionByRange}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- coalesce(df, 1L)
#'}
#' @note coalesce(SparkDataFrame) since 2.1.1
setMethod("coalesce",
          signature(x = "SparkDataFrame"),
          function(x, numPartitions) {
            stopifnot(is.numeric(numPartitions))
            sdf <- callJMethod(x@sdf, "coalesce", numToInt(numPartitions))
            dataFrame(sdf)
          })

#' Repartition
#'
#' The following options for repartition are possible:
#' \itemize{
#'  \item{1.} {Return a new SparkDataFrame that has exactly \code{numPartitions}.}
#'  \item{2.} {Return a new SparkDataFrame hash partitioned by
#'                      the given columns into \code{numPartitions}.}
#'  \item{3.} {Return a new SparkDataFrame hash partitioned by the given column(s),
#'                      using \code{spark.sql.shuffle.partitions} as number of partitions.}
#'}
#' @param x a SparkDataFrame.
#' @param numPartitions the number of partitions to use.
#' @param col the column by which the partitioning will be performed.
#' @param ... additional column(s) to be used in the partitioning.
#'
#' @family SparkDataFrame functions
#' @rdname repartition
#' @name repartition
#' @aliases repartition,SparkDataFrame-method
#' @seealso \link{coalesce}, \link{repartitionByRange}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- repartition(df, 2L)
#' newDF <- repartition(df, numPartitions = 2L)
#' newDF <- repartition(df, col = df$"col1", df$"col2")
#' newDF <- repartition(df, 3L, col = df$"col1", df$"col2")
#'}
#' @note repartition since 1.4.0
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


#' Repartition by range
#'
#' The following options for repartition by range are possible:
#' \itemize{
#'  \item{1.} {Return a new SparkDataFrame range partitioned by
#'                      the given columns into \code{numPartitions}.}
#'  \item{2.} {Return a new SparkDataFrame range partitioned by the given column(s),
#'                      using \code{spark.sql.shuffle.partitions} as number of partitions.}
#'}
#'
#' @param x a SparkDataFrame.
#' @param numPartitions the number of partitions to use.
#' @param col the column by which the range partitioning will be performed.
#' @param ... additional column(s) to be used in the range partitioning.
#'
#' @family SparkDataFrame functions
#' @rdname repartitionByRange
#' @name repartitionByRange
#' @aliases repartitionByRange,SparkDataFrame-method
#' @seealso \link{repartition}, \link{coalesce}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- repartitionByRange(df, col = df$col1, df$col2)
#' newDF <- repartitionByRange(df, 3L, col = df$col1, df$col2)
#'}
#' @note repartitionByRange since 2.4.0
setMethod("repartitionByRange",
          signature(x = "SparkDataFrame"),
          function(x, numPartitions = NULL, col = NULL, ...) {
            if (!is.null(numPartitions) && !is.null(col)) {
              # number of partitions and columns both are specified
              if (is.numeric(numPartitions) && class(col) == "Column") {
                cols <- list(col, ...)
                jcol <- lapply(cols, function(c) { c@jc })
                sdf <- callJMethod(x@sdf, "repartitionByRange", numToInt(numPartitions), jcol)
              } else {
                stop(paste("numPartitions and col must be numeric and Column; however, got",
                           class(numPartitions), "and", class(col)))
              }
            } else if (!is.null(col))  {
              # only columns are specified
              if (class(col) == "Column") {
                cols <- list(col, ...)
                jcol <- lapply(cols, function(c) { c@jc })
                sdf <- callJMethod(x@sdf, "repartitionByRange", jcol)
              } else {
                stop(paste("col must be Column; however, got", class(col)))
              }
            } else if (!is.null(numPartitions)) {
              # only numPartitions is specified
              stop("At least one partition-by column must be specified.")
            } else {
              stop("Please, specify a column(s) or the number of partitions with a column(s)")
            }
            dataFrame(sdf)
          })

#' toJSON
#'
#' Converts a SparkDataFrame into a SparkDataFrame of JSON string.
#'
#' Each row is turned into a JSON document with columns as different fields.
#' The returned SparkDataFrame has a single character column with the name \code{value}
#'
#' @param x a SparkDataFrame
#' @return a SparkDataFrame
#' @family SparkDataFrame functions
#' @rdname toJSON
#' @name toJSON
#' @aliases toJSON,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.parquet"
#' df <- read.parquet(path)
#' df_json <- toJSON(df)
#'}
#' @note toJSON since 2.2.0
setMethod("toJSON",
          signature(x = "SparkDataFrame"),
          function(x) {
            jsonDS <- callJMethod(x@sdf, "toJSON")
            df <- callJMethod(jsonDS, "toDF")
            dataFrame(df)
          })

#' Save the contents of SparkDataFrame as a JSON file
#'
#' Save the contents of a SparkDataFrame as a JSON file (\href{http://jsonlines.org/}{
#' JSON Lines text format or newline-delimited JSON}). Files written out
#' with this method can be read back in as a SparkDataFrame using read.json().
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional argument(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @rdname write.json
#' @name write.json
#' @aliases write.json,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' write.json(df, "/tmp/sparkr-tmp/")
#'}
#' @note write.json since 1.6.0
setMethod("write.json",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path, mode = "error", ...) {
            write <- callJMethod(x@sdf, "write")
            write <- setWriteOptions(write, mode = mode, ...)
            invisible(handledCallJMethod(write, "json", path))
          })

#' Save the contents of SparkDataFrame as an ORC file, preserving the schema.
#'
#' Save the contents of a SparkDataFrame as an ORC file, preserving the schema. Files written out
#' with this method can be read back in as a SparkDataFrame using read.orc().
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional argument(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @aliases write.orc,SparkDataFrame,character-method
#' @rdname write.orc
#' @name write.orc
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' write.orc(df, "/tmp/sparkr-tmp1/")
#' }
#' @note write.orc since 2.0.0
setMethod("write.orc",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path, mode = "error", ...) {
            write <- callJMethod(x@sdf, "write")
            write <- setWriteOptions(write, mode = mode, ...)
            invisible(handledCallJMethod(write, "orc", path))
          })

#' Save the contents of SparkDataFrame as a Parquet file, preserving the schema.
#'
#' Save the contents of a SparkDataFrame as a Parquet file, preserving the schema. Files written out
#' with this method can be read back in as a SparkDataFrame using read.parquet().
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional argument(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @rdname write.parquet
#' @name write.parquet
#' @aliases write.parquet,SparkDataFrame,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' write.parquet(df, "/tmp/sparkr-tmp1/")
#' saveAsParquetFile(df, "/tmp/sparkr-tmp2/")
#'}
#' @note write.parquet since 1.6.0
setMethod("write.parquet",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path, mode = "error", ...) {
            write <- callJMethod(x@sdf, "write")
            write <- setWriteOptions(write, mode = mode, ...)
            invisible(handledCallJMethod(write, "parquet", path))
          })

#' @rdname write.parquet
#' @name saveAsParquetFile
#' @aliases saveAsParquetFile,SparkDataFrame,character-method
#' @note saveAsParquetFile since 1.4.0
setMethod("saveAsParquetFile",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path) {
            .Deprecated("write.parquet")
            write.parquet(x, path)
          })

#' Save the content of SparkDataFrame in a text file at the specified path.
#'
#' Save the content of the SparkDataFrame in a text file at the specified path.
#' The SparkDataFrame must have only one column of string type with the name "value".
#' Each row becomes a new line in the output file.
#'
#' @param x A SparkDataFrame
#' @param path The directory where the file is saved
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional argument(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @aliases write.text,SparkDataFrame,character-method
#' @rdname write.text
#' @name write.text
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.txt"
#' df <- read.text(path)
#' write.text(df, "/tmp/sparkr-tmp/")
#'}
#' @note write.text since 2.0.0
setMethod("write.text",
          signature(x = "SparkDataFrame", path = "character"),
          function(x, path, mode = "error", ...) {
            write <- callJMethod(x@sdf, "write")
            write <- setWriteOptions(write, mode = mode, ...)
            invisible(handledCallJMethod(write, "text", path))
          })

#' Distinct
#'
#' Return a new SparkDataFrame containing the distinct rows in this SparkDataFrame.
#'
#' @param x A SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @aliases distinct,SparkDataFrame-method
#' @rdname distinct
#' @name distinct
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' distinctDF <- distinct(df)
#'}
#' @note distinct since 1.4.0
setMethod("distinct",
          signature(x = "SparkDataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "distinct")
            dataFrame(sdf)
          })

#' @rdname distinct
#' @name unique
#' @aliases unique,SparkDataFrame-method
#' @note unique since 1.5.0
setMethod("unique",
          signature(x = "SparkDataFrame"),
          function(x) {
            distinct(x)
          })

#' Sample
#'
#' Return a sampled subset of this SparkDataFrame using a random seed.
#' Note: this is not guaranteed to provide exactly the fraction specified
#' of the total count of of the given SparkDataFrame.
#'
#' @param x A SparkDataFrame
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value. Default is a random seed.
#'
#' @family SparkDataFrame functions
#' @aliases sample,SparkDataFrame-method
#' @rdname sample
#' @name sample
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' collect(sample(df, fraction = 0.5))
#' collect(sample(df, FALSE, 0.5))
#' collect(sample(df, TRUE, 0.5, seed = 3))
#'}
#' @note sample since 1.4.0
setMethod("sample",
          signature(x = "SparkDataFrame"),
          function(x, withReplacement = FALSE, fraction, seed) {
            if (!is.numeric(fraction)) {
              stop(paste("fraction must be numeric; however, got", class(fraction)))
            }
            if (!is.logical(withReplacement)) {
              stop(paste("withReplacement must be logical; however, got", class(withReplacement)))
            }

            if (!missing(seed)) {
              if (is.null(seed)) {
                stop("seed must not be NULL or NA; however, got NULL")
              }
              if (is.na(seed)) {
                stop("seed must not be NULL or NA; however, got NA")
              }

              # TODO : Figure out how to send integer as java.lang.Long to JVM so
              # we can send seed as an argument through callJMethod
              sdf <- handledCallJMethod(x@sdf, "sample", as.logical(withReplacement),
                                        as.numeric(fraction), as.integer(seed))
            } else {
              sdf <- handledCallJMethod(x@sdf, "sample",
                                        as.logical(withReplacement), as.numeric(fraction))
            }
            dataFrame(sdf)
          })

#' @rdname sample
#' @aliases sample_frac,SparkDataFrame-method
#' @name sample_frac
#' @note sample_frac since 1.4.0
setMethod("sample_frac",
          signature(x = "SparkDataFrame"),
          function(x, withReplacement = FALSE, fraction, seed) {
            sample(x, withReplacement, fraction, seed)
          })

#' Returns the number of rows in a SparkDataFrame
#'
#' @param x a SparkDataFrame.
#' @family SparkDataFrame functions
#' @rdname nrow
#' @name nrow
#' @aliases count,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' count(df)
#' }
#' @note count since 1.4.0
setMethod("count",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(x@sdf, "count")
          })

#' @name nrow
#' @rdname nrow
#' @aliases nrow,SparkDataFrame-method
#' @note nrow since 1.5.0
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
#' @aliases ncol,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' ncol(df)
#' }
#' @note ncol since 1.5.0
setMethod("ncol",
          signature(x = "SparkDataFrame"),
          function(x) {
            length(columns(x))
          })

#' Returns the dimensions of SparkDataFrame
#'
#' Returns the dimensions (number of rows and columns) of a SparkDataFrame
#' @param x a SparkDataFrame
#'
#' @family SparkDataFrame functions
#' @rdname dim
#' @aliases dim,SparkDataFrame-method
#' @name dim
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' dim(df)
#' }
#' @note dim since 1.5.0
setMethod("dim",
          signature(x = "SparkDataFrame"),
          function(x) {
            c(count(x), ncol(x))
          })

#' Collects all the elements of a SparkDataFrame and coerces them into an R data.frame.
#'
#' @param x a SparkDataFrame.
#' @param stringsAsFactors (Optional) a logical indicating whether or not string columns
#' should be converted to factors. FALSE by default.
#' @param ... further arguments to be passed to or from other methods.
#'
#' @family SparkDataFrame functions
#' @rdname collect
#' @aliases collect,SparkDataFrame-method
#' @name collect
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' collected <- collect(df)
#' class(collected)
#' firstName <- names(collected)[1]
#' }
#' @note collect since 1.4.0
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
                  if (is.null(PRIMITIVE_TYPES[[colType]])) {
                    specialtype <- specialtypeshandle(colType)
                    if (!is.null(specialtype)) {
                      colType <- specialtype
                    }
                  }

                  # Note that "binary" columns behave like complex types.
                  if (!is.null(PRIMITIVE_TYPES[[colType]]) && colType != "binary") {
                    vec <- do.call(c, col)
                    stopifnot(class(vec) != "list")
                    class(vec) <- PRIMITIVE_TYPES[[colType]]
                    if (is.character(vec) && stringsAsFactors) {
                      vec <- as.factor(vec)
                    }
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
#' @aliases limit,SparkDataFrame,numeric-method
#' @examples
#' \dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' limitedDF <- limit(df, 10)
#' }
#' @note limit since 1.4.0
setMethod("limit",
          signature(x = "SparkDataFrame", num = "numeric"),
          function(x, num) {
            res <- callJMethod(x@sdf, "limit", as.integer(num))
            dataFrame(res)
          })

#' Take the first NUM rows of a SparkDataFrame and return the results as a R data.frame
#'
#' @param x a SparkDataFrame.
#' @param num number of rows to take.
#' @family SparkDataFrame functions
#' @rdname take
#' @name take
#' @aliases take,SparkDataFrame,numeric-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' take(df, 2)
#' }
#' @note take since 1.4.0
setMethod("take",
          signature(x = "SparkDataFrame", num = "numeric"),
          function(x, num) {
            limited <- limit(x, num)
            collect(limited)
          })

#' Head
#'
#' Return the first \code{num} rows of a SparkDataFrame as a R data.frame. If \code{num} is not
#' specified, then head() returns the first 6 rows as with R data.frame.
#'
#' @param x a SparkDataFrame.
#' @param num the number of rows to return. Default is 6.
#' @return A data.frame.
#'
#' @family SparkDataFrame functions
#' @aliases head,SparkDataFrame-method
#' @rdname head
#' @name head
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' head(df)
#' }
#' @note head since 1.4.0
setMethod("head",
          signature(x = "SparkDataFrame"),
          function(x, num = 6L) {
          # Default num is 6L in keeping with R's data.frame convention
            take(x, num)
          })

#' Return the first row of a SparkDataFrame
#'
#' @param x a SparkDataFrame or a column used in aggregation function.
#' @param ... further arguments to be passed to or from other methods.
#'
#' @family SparkDataFrame functions
#' @aliases first,SparkDataFrame-method
#' @rdname first
#' @name first
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' first(df)
#' }
#' @note first(SparkDataFrame) since 1.4.0
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
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
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
#' @param x a SparkDataFrame.
#' @param ... character name(s) or Column(s) to group on.
#' @return A GroupedData.
#' @family SparkDataFrame functions
#' @aliases groupBy,SparkDataFrame-method
#' @rdname groupBy
#' @name groupBy
#' @examples
#' \dontrun{
#'   # Compute the average for all numeric columns grouped by department.
#'   avg(groupBy(df, "department"))
#'
#'   # Compute the max age and average salary, grouped by department and gender.
#'   agg(groupBy(df, "department", "gender"), salary="avg", "age" -> "max")
#' }
#' @note groupBy since 1.4.0
#' @seealso \link{agg}, \link{cube}, \link{rollup}
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
#' @aliases group_by,SparkDataFrame-method
#' @note group_by since 1.4.0
setMethod("group_by",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            groupBy(x, ...)
          })

#' Summarize data across columns
#'
#' Compute aggregates by specifying a list of columns
#'
#' @family SparkDataFrame functions
#' @aliases agg,SparkDataFrame-method
#' @rdname summarize
#' @name agg
#' @note agg since 1.4.0
setMethod("agg",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            agg(groupBy(x), ...)
          })

#' @rdname summarize
#' @name summarize
#' @aliases summarize,SparkDataFrame-method
#' @note summarize since 1.4.0
setMethod("summarize",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            agg(x, ...)
          })

dapplyInternal <- function(x, func, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }

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

setClassUnion("characterOrstructType", c("character", "structType"))

#' dapply
#'
#' Apply a function to each partition of a SparkDataFrame.
#'
#' @param x A SparkDataFrame
#' @param func A function to be applied to each partition of the SparkDataFrame.
#'             func should have only one parameter, to which a R data.frame corresponds
#'             to each partition will be passed.
#'             The output of func should be a R data.frame.
#' @param schema The schema of the resulting SparkDataFrame after the function is applied.
#'               It must match the output of func. Since Spark 2.3, the DDL-formatted string
#'               is also supported for the schema.
#' @family SparkDataFrame functions
#' @rdname dapply
#' @aliases dapply,SparkDataFrame,function,characterOrstructType-method
#' @name dapply
#' @seealso \link{dapplyCollect}
#' @examples
#' \dontrun{
#'   df <- createDataFrame(iris)
#'   df1 <- dapply(df, function(x) { x }, schema(df))
#'   collect(df1)
#'
#'   # filter and add a column
#'   df <- createDataFrame(
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
#'
#'   # The schema also can be specified in a DDL-formatted string.
#'   schema <- "a INT, d DOUBLE, c STRING, d INT"
#'   df1 <- dapply(
#'            df,
#'            function(x) {
#'              y <- x[x[1] > 1, ]
#'              y <- cbind(y, y[1] + 1L)
#'            },
#'            schema)
#'
#'   collect(df1)
#'   # the result
#'   #       a b c d
#'   #     1 2 2 2 3
#'   #     2 3 3 3 4
#' }
#' @note dapply since 2.0.0
setMethod("dapply",
          signature(x = "SparkDataFrame", func = "function", schema = "characterOrstructType"),
          function(x, func, schema) {
            dapplyInternal(x, func, schema)
          })

#' dapplyCollect
#'
#' Apply a function to each partition of a SparkDataFrame and collect the result back
#' to R as a data.frame.
#'
#' @param x A SparkDataFrame
#' @param func A function to be applied to each partition of the SparkDataFrame.
#'             func should have only one parameter, to which a R data.frame corresponds
#'             to each partition will be passed.
#'             The output of func should be a R data.frame.
#' @family SparkDataFrame functions
#' @rdname dapplyCollect
#' @aliases dapplyCollect,SparkDataFrame,function-method
#' @name dapplyCollect
#' @seealso \link{dapply}
#' @examples
#' \dontrun{
#'   df <- createDataFrame(iris)
#'   ldf <- dapplyCollect(df, function(x) { x })
#'
#'   # filter and add a column
#'   df <- createDataFrame(
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
#' @note dapplyCollect since 2.0.0
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

#' gapply
#'
#' Groups the SparkDataFrame using the specified columns and applies the R function to each
#' group.
#'
#' @param cols grouping columns.
#' @param func a function to be applied to each group partition specified by grouping
#'             column of the SparkDataFrame. See Details.
#' @param schema the schema of the resulting SparkDataFrame after the function is applied.
#'               The schema must match to output of \code{func}. It has to be defined for each
#'               output column with preferred output column name and corresponding data type.
#'               Since Spark 2.3, the DDL-formatted string is also supported for the schema.
#' @return A SparkDataFrame.
#' @family SparkDataFrame functions
#' @aliases gapply,SparkDataFrame-method
#' @rdname gapply
#' @name gapply
#' @details
#' \code{func} is a function of two arguments. The first, usually named \code{key}
#' (though this is not enforced) corresponds to the grouping key, will be an
#' unnamed \code{list} of \code{length(cols)} length-one objects corresponding
#' to the grouping columns' values for the current group.
#'
#' The second, herein \code{x}, will be a local \code{\link{data.frame}} with the
#' columns of the input not in \code{cols} for the rows corresponding to \code{key}.
#'
#' The output of \code{func} must be a \code{data.frame} matching \code{schema} --
#' in particular this means the names of the output \code{data.frame} are irrelevant
#'
#' @seealso \link{gapplyCollect}
#' @examples
#'
#' \dontrun{
#' # Computes the arithmetic mean of the second column by grouping
#' # on the first and third columns. Output the grouping values and the average.
#'
#' df <- createDataFrame (
#' list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
#'   c("a", "b", "c", "d"))
#'
#' # Here our output contains three columns, the key which is a combination of two
#' # columns with data types integer and string and the mean which is a double.
#' schema <- structType(structField("a", "integer"), structField("c", "string"),
#'   structField("avg", "double"))
#' result <- gapply(
#'   df,
#'   c("a", "c"),
#'   function(key, x) {
#'     # key will either be list(1L, '1') (for the group where a=1L,c='1') or
#'     #   list(3L, '3') (for the group where a=3L,c='3')
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#' }, schema)
#'
#' # The schema also can be specified in a DDL-formatted string.
#' schema <- "a INT, c STRING, avg DOUBLE"
#' result <- gapply(
#'   df,
#'   c("a", "c"),
#'   function(key, x) {
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#' }, schema)
#'
#' # We can also group the data and afterwards call gapply on GroupedData.
#' # For example:
#' gdf <- group_by(df, "a", "c")
#' result <- gapply(
#'   gdf,
#'   function(key, x) {
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#' }, schema)
#' collect(result)
#'
#' # Result
#' # ------
#' # a c avg
#' # 3 3 3.0
#' # 1 1 1.5
#'
#' # Fits linear models on iris dataset by grouping on the 'Species' column and
#' # using 'Sepal_Length' as a target variable, 'Sepal_Width', 'Petal_Length'
#' # and 'Petal_Width' as training features.
#'
#' df <- createDataFrame (iris)
#' schema <- structType(structField("(Intercept)", "double"),
#'   structField("Sepal_Width", "double"),structField("Petal_Length", "double"),
#'   structField("Petal_Width", "double"))
#' df1 <- gapply(
#'   df,
#'   df$"Species",
#'   function(key, x) {
#'     m <- suppressWarnings(lm(Sepal_Length ~
#'     Sepal_Width + Petal_Length + Petal_Width, x))
#'     data.frame(t(coef(m)))
#'   }, schema)
#' collect(df1)
#'
#' # Result
#' # ---------
#' # Model  (Intercept)  Sepal_Width  Petal_Length  Petal_Width
#' # 1        0.699883    0.3303370    0.9455356    -0.1697527
#' # 2        1.895540    0.3868576    0.9083370    -0.6792238
#' # 3        2.351890    0.6548350    0.2375602     0.2521257
#'
#'}
#' @note gapply(SparkDataFrame) since 2.0.0
setMethod("gapply",
          signature(x = "SparkDataFrame"),
          function(x, cols, func, schema) {
            grouped <- do.call("groupBy", c(x, cols))
            gapply(grouped, func, schema)
          })

#' gapplyCollect
#'
#' Groups the SparkDataFrame using the specified columns, applies the R function to each
#' group and collects the result back to R as data.frame.
#'
#' @param cols grouping columns.
#' @param func a function to be applied to each group partition specified by grouping
#'             column of the SparkDataFrame. See Details.
#' @return A data.frame.
#' @family SparkDataFrame functions
#' @aliases gapplyCollect,SparkDataFrame-method
#' @rdname gapplyCollect
#' @name gapplyCollect
#' @details
#' \code{func} is a function of two arguments. The first, usually named \code{key}
#' (though this is not enforced) corresponds to the grouping key, will be an
#' unnamed \code{list} of \code{length(cols)} length-one objects corresponding
#' to the grouping columns' values for the current group.
#'
#' The second, herein \code{x}, will be a local \code{\link{data.frame}} with the
#' columns of the input not in \code{cols} for the rows corresponding to \code{key}.
#'
#' The output of \code{func} must be a \code{data.frame} matching \code{schema} --
#' in particular this means the names of the output \code{data.frame} are irrelevant
#'
#' @seealso \link{gapply}
#' @examples
#'
#' \dontrun{
#' # Computes the arithmetic mean of the second column by grouping
#' # on the first and third columns. Output the grouping values and the average.
#'
#' df <- createDataFrame (
#' list(list(1L, 1, "1", 0.1), list(1L, 2, "1", 0.2), list(3L, 3, "3", 0.3)),
#'   c("a", "b", "c", "d"))
#'
#' result <- gapplyCollect(
#'   df,
#'   c("a", "c"),
#'   function(key, x) {
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#'     colnames(y) <- c("key_a", "key_c", "mean_b")
#'     y
#'   })
#'
#' # We can also group the data and afterwards call gapply on GroupedData.
#' # For example:
#' gdf <- group_by(df, "a", "c")
#' result <- gapplyCollect(
#'   gdf,
#'   function(key, x) {
#'     y <- data.frame(key, mean(x$b), stringsAsFactors = FALSE)
#'     colnames(y) <- c("key_a", "key_c", "mean_b")
#'     y
#'   })
#'
#' # Result
#' # ------
#' # key_a key_c mean_b
#' # 3 3 3.0
#' # 1 1 1.5
#'
#' # Fits linear models on iris dataset by grouping on the 'Species' column and
#' # using 'Sepal_Length' as a target variable, 'Sepal_Width', 'Petal_Length'
#' # and 'Petal_Width' as training features.
#'
#' df <- createDataFrame (iris)
#' result <- gapplyCollect(
#'   df,
#'   df$"Species",
#'   function(key, x) {
#'     m <- suppressWarnings(lm(Sepal_Length ~
#'     Sepal_Width + Petal_Length + Petal_Width, x))
#'     data.frame(t(coef(m)))
#'   })
#'
#' # Result
#' # ---------
#' # Model  X.Intercept.  Sepal_Width  Petal_Length  Petal_Width
#' # 1        0.699883    0.3303370    0.9455356    -0.1697527
#' # 2        1.895540    0.3868576    0.9083370    -0.6792238
#' # 3        2.351890    0.6548350    0.2375602     0.2521257
#'
#'}
#' @note gapplyCollect(SparkDataFrame) since 2.0.0
setMethod("gapplyCollect",
          signature(x = "SparkDataFrame"),
          function(x, cols, func) {
            grouped <- do.call("groupBy", c(x, cols))
            gapplyCollect(grouped, func)
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

setColumn <- function(x, c, value) {
  if (class(value) != "Column" && !is.null(value)) {
    if (isAtomicLengthOne(value)) {
      value <- lit(value)
    } else {
      stop("value must be a Column, literal value as atomic in length of 1, or NULL")
    }
  }

  if (is.null(value)) {
    nx <- drop(x, c)
  } else {
    nx <- withColumn(x, c, value)
  }
  nx
}

#' @param name name of a Column (without being wrapped by \code{""}).
#' @rdname select
#' @name $
#' @aliases $,SparkDataFrame-method
#' @note $ since 1.4.0
setMethod("$", signature(x = "SparkDataFrame"),
          function(x, name) {
            getColumn(x, name)
          })

#' @param value a Column or an atomic vector in the length of 1 as literal value, or \code{NULL}.
#'              If \code{NULL}, the specified Column is dropped.
#' @rdname select
#' @name $<-
#' @aliases $<-,SparkDataFrame-method
#' @note $<- since 1.4.0
setMethod("$<-", signature(x = "SparkDataFrame"),
          function(x, name, value) {
            nx <- setColumn(x, name, value)
            x@sdf <- nx@sdf
            x
          })

setClassUnion("numericOrcharacter", c("numeric", "character"))

#' @rdname subset
#' @name [[
#' @aliases [[,SparkDataFrame,numericOrcharacter-method
#' @note [[ since 1.4.0
setMethod("[[", signature(x = "SparkDataFrame", i = "numericOrcharacter"),
          function(x, i) {
            if (length(i) > 1) {
              warning("Subset index has length > 1. Only the first index is used.")
              i <- i[1]
            }
            if (is.numeric(i)) {
              cols <- columns(x)
              i <- cols[[i]]
            }
            getColumn(x, i)
          })

#' @rdname subset
#' @name [[<-
#' @aliases [[<-,SparkDataFrame,numericOrcharacter-method
#' @note [[<- since 2.1.1
setMethod("[[<-", signature(x = "SparkDataFrame", i = "numericOrcharacter"),
          function(x, i, value) {
            if (length(i) > 1) {
              warning("Subset index has length > 1. Only the first index is used.")
              i <- i[1]
            }
            if (is.numeric(i)) {
              cols <- columns(x)
              i <- cols[[i]]
            }
            nx <- setColumn(x, i, value)
            x@sdf <- nx@sdf
            x
          })

#' @rdname subset
#' @name [
#' @aliases [,SparkDataFrame-method
#' @note [ since 1.4.0
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
#' @param x a SparkDataFrame.
#' @param i,subset (Optional) a logical expression to filter on rows.
#'                 For extract operator [[ and replacement operator [[<-, the indexing parameter for
#'                 a single Column.
#' @param j,select expression for the single Column or a list of columns to select from the
#'                 SparkDataFrame.
#' @param drop if TRUE, a Column will be returned if the resulting dataset has only one column.
#'             Otherwise, a SparkDataFrame will always be returned.
#' @param value a Column or an atomic vector in the length of 1 as literal value, or \code{NULL}.
#'              If \code{NULL}, the specified Column is dropped.
#' @param ... currently not used.
#' @return A new SparkDataFrame containing only the rows that meet the condition with selected
#'         columns.
#' @family SparkDataFrame functions
#' @aliases subset,SparkDataFrame-method
#' @seealso \link{withColumn}
#' @rdname subset
#' @name subset
#' @family subsetting functions
#' @examples
#' \dontrun{
#'   # Columns can be selected using [[ and [
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
#'   # Columns can be selected and set
#'   df[["age"]] <- 23
#'   df[[1]] <- df$age
#'   df[[2]] <- NULL # drop column
#' }
#' @note subset since 1.5.0
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
#' @param x a SparkDataFrame.
#' @param col a list of columns or single Column or name.
#' @param ... additional column(s) if only one column is specified in \code{col}.
#'            If more than one column is assigned in \code{col}, \code{...}
#'            should be left empty.
#' @return A new SparkDataFrame with selected columns.
#' @family SparkDataFrame functions
#' @rdname select
#' @aliases select,SparkDataFrame,character-method
#' @name select
#' @family subsetting functions
#' @examples
#' \dontrun{
#'   select(df, "*")
#'   select(df, "col1", "col2")
#'   select(df, df$name, df$age + 1)
#'   select(df, c("col1", "col2"))
#'   select(df, list(df$name, df$age + 1))
#'   # Similar to R data frames columns can also be selected using $
#'   df[,df$age]
#' }
#' @note select(SparkDataFrame, character) since 1.4.0
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

#' @rdname select
#' @aliases select,SparkDataFrame,Column-method
#' @note select(SparkDataFrame, Column) since 1.4.0
setMethod("select", signature(x = "SparkDataFrame", col = "Column"),
          function(x, col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            sdf <- callJMethod(x@sdf, "select", jcols)
            dataFrame(sdf)
          })

#' @rdname select
#' @aliases select,SparkDataFrame,list-method
#' @note select(SparkDataFrame, list) since 1.4.0
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
#' @aliases selectExpr,SparkDataFrame,character-method
#' @rdname selectExpr
#' @name selectExpr
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' selectExpr(df, "col1", "(col2 * 5) as newCol")
#' }
#' @note selectExpr since 1.4.0
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
#' @param x a SparkDataFrame.
#' @param colName a column name.
#' @param col a Column expression (which must refer only to this SparkDataFrame), or an atomic
#' vector in the length of 1 as literal value.
#' @return A SparkDataFrame with the new column added or the existing column replaced.
#' @family SparkDataFrame functions
#' @aliases withColumn,SparkDataFrame,character-method
#' @rdname withColumn
#' @name withColumn
#' @seealso \link{rename} \link{mutate} \link{subset}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- withColumn(df, "newCol", df$col1 * 5)
#' # Replace an existing column
#' newDF2 <- withColumn(newDF, "newCol", newDF$col1)
#' newDF3 <- withColumn(newDF, "newCol", 42)
#' # Use extract operator to set an existing or new column
#' df[["age"]] <- 23
#' df[[2]] <- df$col1
#' df[[2]] <- NULL # drop column
#' }
#' @note withColumn since 1.4.0
setMethod("withColumn",
          signature(x = "SparkDataFrame", colName = "character"),
          function(x, colName, col) {
            if (class(col) != "Column") {
              if (!isAtomicLengthOne(col)) stop("Literal value must be atomic in length of 1")
              col <- lit(col)
            }
            sdf <- callJMethod(x@sdf, "withColumn", colName, col@jc)
            dataFrame(sdf)
          })

#' Mutate
#'
#' Return a new SparkDataFrame with the specified columns added or replaced.
#'
#' @param .data a SparkDataFrame.
#' @param ... additional column argument(s) each in the form name = col.
#' @return A new SparkDataFrame with the new columns added or replaced.
#' @family SparkDataFrame functions
#' @aliases mutate,SparkDataFrame-method
#' @rdname mutate
#' @name mutate
#' @seealso \link{rename} \link{withColumn}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- mutate(df, newCol = df$col1 * 5, newCol2 = df$col1 * 2)
#' names(newDF) # Will contain newCol, newCol2
#' newDF2 <- transform(df, newCol = df$col1 / 5, newCol2 = df$col1 * 2)
#'
#' df <- createDataFrame(list(list("Andy", 30L), list("Justin", 19L)), c("name", "age"))
#' # Replace the "age" column
#' df1 <- mutate(df, age = df$age + 1L)
#' }
#' @note mutate since 1.4.0
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
            for (i in seq_len(length(cols))) {
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

#' @param _data a SparkDataFrame.
#' @rdname mutate
#' @aliases transform,SparkDataFrame-method
#' @name transform
#' @note transform since 1.5.0
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
#' @aliases withColumnRenamed,SparkDataFrame,character,character-method
#' @seealso \link{mutate}
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- withColumnRenamed(df, "col1", "newCol1")
#' }
#' @note withColumnRenamed since 1.4.0
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

#' @param ... A named pair of the form new_column_name = existing_column
#' @rdname rename
#' @name rename
#' @aliases rename,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' newDF <- rename(df, col1 = df$newCol1)
#' }
#' @note rename since 1.4.0
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

setClassUnion("numericOrColumn", c("numeric", "Column"))

#' Arrange Rows by Variables
#'
#' Sort a SparkDataFrame by the specified column(s).
#'
#' @param x a SparkDataFrame to be sorted.
#' @param col a character or Column object indicating the fields to sort on
#' @param ... additional sorting fields
#' @param decreasing a logical argument indicating sorting order for columns when
#'                   a character vector is specified for col
#' @param withinPartitions a logical argument indicating whether to sort only within each partition
#' @return A SparkDataFrame where all elements are sorted.
#' @family SparkDataFrame functions
#' @aliases arrange,SparkDataFrame,Column-method
#' @rdname arrange
#' @name arrange
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' arrange(df, df$col1)
#' arrange(df, asc(df$col1), desc(abs(df$col2)))
#' arrange(df, "col1", decreasing = TRUE)
#' arrange(df, "col1", "col2", decreasing = c(TRUE, FALSE))
#' arrange(df, "col1", "col2", withinPartitions = TRUE)
#' }
#' @note arrange(SparkDataFrame, Column) since 1.4.0
setMethod("arrange",
          signature(x = "SparkDataFrame", col = "Column"),
          function(x, col, ..., withinPartitions = FALSE) {
              jcols <- lapply(list(col, ...), function(c) {
                c@jc
              })

            if (withinPartitions) {
              sdf <- callJMethod(x@sdf, "sortWithinPartitions", jcols)
            } else {
              sdf <- callJMethod(x@sdf, "sort", jcols)
            }
            dataFrame(sdf)
          })

#' @rdname arrange
#' @name arrange
#' @aliases arrange,SparkDataFrame,character-method
#' @note arrange(SparkDataFrame, character) since 1.4.0
setMethod("arrange",
          signature(x = "SparkDataFrame", col = "character"),
          function(x, col, ..., decreasing = FALSE, withinPartitions = FALSE) {

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
            jcols <- lapply(seq_len(length(decreasing)), function(i) {
              if (decreasing[[i]]) {
                desc(getColumn(x, by[[i]]))
              } else {
                asc(getColumn(x, by[[i]]))
              }
            })

            do.call("arrange", c(x, jcols, withinPartitions = withinPartitions))
          })

#' @rdname arrange
#' @aliases orderBy,SparkDataFrame,characterOrColumn-method
#' @note orderBy(SparkDataFrame, characterOrColumn) since 1.4.0
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
#' @aliases filter,SparkDataFrame,characterOrColumn-method
#' @rdname filter
#' @name filter
#' @family subsetting functions
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' filter(df, "col1 > 0")
#' filter(df, df$col2 != "abcdefg")
#' }
#' @note filter since 1.4.0
setMethod("filter",
          signature(x = "SparkDataFrame", condition = "characterOrColumn"),
          function(x, condition) {
            if (class(condition) == "Column") {
              condition <- condition@jc
            }
            sdf <- callJMethod(x@sdf, "filter", condition)
            dataFrame(sdf)
          })

#' @rdname filter
#' @name where
#' @aliases where,SparkDataFrame,characterOrColumn-method
#' @note where since 1.4.0
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
#' @param ... A character vector of column names or string column names.
#'            If the first argument contains a character vector, the followings are ignored.
#' @return A SparkDataFrame with duplicate rows removed.
#' @family SparkDataFrame functions
#' @aliases dropDuplicates,SparkDataFrame-method
#' @rdname dropDuplicates
#' @name dropDuplicates
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' dropDuplicates(df)
#' dropDuplicates(df, "col1", "col2")
#' dropDuplicates(df, c("col1", "col2"))
#' }
#' @note dropDuplicates since 2.0.0
setMethod("dropDuplicates",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            cols <- list(...)
            if (length(cols) == 0) {
              sdf <- callJMethod(x@sdf, "dropDuplicates", as.list(columns(x)))
            } else {
              if (!all(sapply(cols, function(c) { is.character(c) }))) {
                stop("all columns names should be characters")
              }
              col <- cols[[1]]
              if (length(col) > 1) {
                sdf <- callJMethod(x@sdf, "dropDuplicates", as.list(col))
              } else {
                sdf <- callJMethod(x@sdf, "dropDuplicates", cols)
              }
            }
            dataFrame(sdf)
          })

#' Join
#'
#' Joins two SparkDataFrames based on the given join expression.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @param joinExpr (Optional) The expression used to perform the join. joinExpr must be a
#' Column expression. If joinExpr is omitted, the default, inner join is attempted and an error is
#' thrown if it would be a Cartesian Product. For Cartesian join, use crossJoin instead.
#' @param joinType The type of join to perform, default 'inner'.
#' Must be one of: 'inner', 'cross', 'outer', 'full', 'full_outer',
#' 'left', 'left_outer', 'right', 'right_outer', 'left_semi', or 'left_anti'.
#' @return A SparkDataFrame containing the result of the join operation.
#' @family SparkDataFrame functions
#' @aliases join,SparkDataFrame,SparkDataFrame-method
#' @rdname join
#' @name join
#' @seealso \link{merge} \link{crossJoin}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' join(df1, df2, df1$col1 == df2$col2) # Performs an inner join based on expression
#' join(df1, df2, df1$col1 == df2$col2, "right_outer")
#' join(df1, df2) # Attempts an inner join
#' }
#' @note join since 1.4.0
setMethod("join",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y, joinExpr = NULL, joinType = NULL) {
            if (is.null(joinExpr)) {
              # this may not fail until the planner checks for Cartesian join later on.
              sdf <- callJMethod(x@sdf, "join", y@sdf)
            } else {
              if (class(joinExpr) != "Column") stop("joinExpr must be a Column")
              if (is.null(joinType)) {
                sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc)
              } else {
                if (joinType %in% c("inner", "cross",
                    "outer", "full", "fullouter", "full_outer",
                    "left", "leftouter", "left_outer",
                    "right", "rightouter", "right_outer",
                    "left_semi", "leftsemi", "left_anti", "leftanti")) {
                  joinType <- gsub("_", "", joinType)
                  sdf <- callJMethod(x@sdf, "join", y@sdf, joinExpr@jc, joinType)
                } else {
                  stop("joinType must be one of the following types: ",
                       "'inner', 'cross', 'outer', 'full', 'full_outer',",
                       "'left', 'left_outer', 'right', 'right_outer',",
                       "'left_semi', or 'left_anti'.")
                }
              }
            }
            dataFrame(sdf)
          })

#' CrossJoin
#'
#' Returns Cartesian Product on two SparkDataFrames.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the join operation.
#' @family SparkDataFrame functions
#' @aliases crossJoin,SparkDataFrame,SparkDataFrame-method
#' @rdname crossJoin
#' @name crossJoin
#' @seealso \link{merge} \link{join}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' crossJoin(df1, df2) # Performs a Cartesian
#' }
#' @note crossJoin since 2.1.0
setMethod("crossJoin",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            sdf <- callJMethod(x@sdf, "crossJoin", y@sdf)
            dataFrame(sdf)
          })

#' Merges two data frames
#'
#' @name merge
#' @param x the first data frame to be joined.
#' @param y the second data frame to be joined.
#' @param by a character vector specifying the join columns. If by is not
#'   specified, the common column names in \code{x} and \code{y} will be used.
#'   If by or both by.x and by.y are explicitly set to NULL or of length 0, the Cartesian
#'   Product of x and y will be returned.
#' @param by.x a character vector specifying the joining columns for x.
#' @param by.y a character vector specifying the joining columns for y.
#' @param all a boolean value setting \code{all.x} and \code{all.y}
#'            if any of them are unset.
#' @param all.x a boolean value indicating whether all the rows in x should
#'              be including in the join.
#' @param all.y a boolean value indicating whether all the rows in y should
#'              be including in the join.
#' @param sort a logical argument indicating whether the resulting columns should be sorted.
#' @param suffixes a string vector of length 2 used to make colnames of
#'                 \code{x} and \code{y} unique.
#'                 The first element is appended to each colname of \code{x}.
#'                 The second element is appended to each colname of \code{y}.
#' @param ... additional argument(s) passed to the method.
#' @details  If all.x and all.y are set to FALSE, a natural join will be returned. If
#'   all.x is set to TRUE and all.y is set to FALSE, a left outer join will
#'   be returned. If all.x is set to FALSE and all.y is set to TRUE, a right
#'   outer join will be returned. If all.x and all.y are set to TRUE, a full
#'   outer join will be returned.
#' @family SparkDataFrame functions
#' @aliases merge,SparkDataFrame,SparkDataFrame-method
#' @rdname merge
#' @seealso \link{join} \link{crossJoin}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' merge(df1, df2) # Performs an inner join by common columns
#' merge(df1, df2, by = "col1") # Performs an inner join based on expression
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.y = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.x = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all.x = TRUE, all.y = TRUE)
#' merge(df1, df2, by.x = "col1", by.y = "col2", all = TRUE, sort = FALSE)
#' merge(df1, df2, by = "col1", all = TRUE, suffixes = c("-X", "-Y"))
#' merge(df1, df2, by = NULL) # Performs a Cartesian join
#' }
#' @note merge since 1.5.0
setMethod("merge",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y, by = intersect(names(x), names(y)), by.x = by, by.y = by,
                   all = FALSE, all.x = all, all.y = all,
                   sort = TRUE, suffixes = c("_x", "_y"), ...) {

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
              joinRes <- crossJoin(x, y)
              return(joinRes)
            }

            # sets alias for making colnames unique in dataframes 'x' and 'y'
            colsX <- genAliasesForIntersectedCols(x, by, suffixes[1])
            colsY <- genAliasesForIntersectedCols(y, by, suffixes[2])

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

#' Creates a list of columns by replacing the intersected ones with aliases
#'
#' Creates a list of columns by replacing the intersected ones with aliases.
#' The name of the alias column is formed by concatanating the original column name and a suffix.
#'
#' @param x a SparkDataFrame
#' @param intersectedColNames a list of intersected column names of the SparkDataFrame
#' @param suffix a suffix for the column name
#' @return list of columns
#' @noRd
genAliasesForIntersectedCols <- function(x, intersectedColNames, suffix) {
  allColNames <- names(x)
  # sets alias for making colnames unique in dataframe 'x'
  cols <- lapply(allColNames, function(colName) {
    col <- getColumn(x, colName)
    if (colName %in% intersectedColNames) {
      newJoin <- paste(colName, suffix, sep = "")
      if (newJoin %in% allColNames) {
        stop("The following column name: ", newJoin, " occurs more than once in the 'DataFrame'.",
          "Please use different suffixes for the intersected columns.")
      }
      col <- alias(col, newJoin)
    }
    col
  })
  cols
}

#' Return a new SparkDataFrame containing the union of rows
#'
#' Return a new SparkDataFrame containing the union of rows in this SparkDataFrame
#' and another SparkDataFrame. This is equivalent to \code{UNION ALL} in SQL.
#' Input SparkDataFrames can have different schemas (names and data types).
#'
#' Note: This does not remove duplicate rows across the two SparkDataFrames.
#' Also as standard in SQL, this function resolves columns by position (not by name).
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the union.
#' @family SparkDataFrame functions
#' @rdname union
#' @name union
#' @aliases union,SparkDataFrame,SparkDataFrame-method
#' @seealso \link{rbind} \link{unionByName}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' unioned <- union(df, df2)
#' unions <- rbind(df, df2, df3, df4)
#' }
#' @note union since 2.0.0
setMethod("union",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            unioned <- callJMethod(x@sdf, "union", y@sdf)
            dataFrame(unioned)
          })

#' unionAll is deprecated - use union instead
#' @rdname union
#' @name unionAll
#' @aliases unionAll,SparkDataFrame,SparkDataFrame-method
#' @note unionAll since 1.4.0
setMethod("unionAll",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            .Deprecated("union")
            union(x, y)
          })

#' Return a new SparkDataFrame containing the union of rows, matched by column names
#'
#' Return a new SparkDataFrame containing the union of rows in this SparkDataFrame
#' and another SparkDataFrame. This is different from \code{union} function, and both
#' \code{UNION ALL} and \code{UNION DISTINCT} in SQL as column positions are not taken
#' into account. Input SparkDataFrames can have different data types in the schema.
#'
#' Note: This does not remove duplicate rows across the two SparkDataFrames.
#' This function resolves columns by name (not by position).
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the union.
#' @family SparkDataFrame functions
#' @rdname unionByName
#' @name unionByName
#' @aliases unionByName,SparkDataFrame,SparkDataFrame-method
#' @seealso \link{rbind} \link{union}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- select(createDataFrame(mtcars), "carb", "am", "gear")
#' df2 <- select(createDataFrame(mtcars), "am", "gear", "carb")
#' head(unionByName(df1, df2))
#' }
#' @note unionByName since 2.3.0
setMethod("unionByName",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            unioned <- callJMethod(x@sdf, "unionByName", y@sdf)
            dataFrame(unioned)
          })

#' Union two or more SparkDataFrames
#'
#' Union two or more SparkDataFrames by row. As in R's \code{rbind}, this method
#' requires that the input SparkDataFrames have the same column names.
#'
#' Note: This does not remove duplicate rows across the two SparkDataFrames.
#'
#' @param x a SparkDataFrame.
#' @param ... additional SparkDataFrame(s).
#' @param deparse.level currently not used (put here to match the signature of
#'                      the base implementation).
#' @return A SparkDataFrame containing the result of the union.
#' @family SparkDataFrame functions
#' @aliases rbind,SparkDataFrame-method
#' @rdname rbind
#' @name rbind
#' @seealso \link{union} \link{unionByName}
#' @examples
#'\dontrun{
#' sparkR.session()
#' unions <- rbind(df, df2, df3, df4)
#' }
#' @note rbind since 1.5.0
setMethod("rbind",
          signature(... = "SparkDataFrame"),
          function(x, ..., deparse.level = 1) {
            nm <- lapply(list(x, ...), names)
            if (length(unique(nm)) != 1) {
              stop("Names of input data frames are different.")
            }
            if (nargs() == 3) {
              union(x, ...)
            } else {
              union(x, Recall(..., deparse.level = 1))
            }
          })

#' Intersect
#'
#' Return a new SparkDataFrame containing rows only in both this SparkDataFrame
#' and another SparkDataFrame. This is equivalent to \code{INTERSECT} in SQL.
#'
#' @param x A SparkDataFrame
#' @param y A SparkDataFrame
#' @return A SparkDataFrame containing the result of the intersect.
#' @family SparkDataFrame functions
#' @aliases intersect,SparkDataFrame,SparkDataFrame-method
#' @rdname intersect
#' @name intersect
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' intersectDF <- intersect(df, df2)
#' }
#' @note intersect since 1.4.0
setMethod("intersect",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            intersected <- callJMethod(x@sdf, "intersect", y@sdf)
            dataFrame(intersected)
          })

#' intersectAll
#'
#' Return a new SparkDataFrame containing rows in both this SparkDataFrame
#' and another SparkDataFrame while preserving the duplicates.
#' This is equivalent to \code{INTERSECT ALL} in SQL. Also as standard in
#' SQL, this function resolves columns by position (not by name).
#'
#' @param x a SparkDataFrame.
#' @param y a SparkDataFrame.
#' @return A SparkDataFrame containing the result of the intersect all operation.
#' @family SparkDataFrame functions
#' @aliases intersectAll,SparkDataFrame,SparkDataFrame-method
#' @rdname intersectAll
#' @name intersectAll
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' intersectAllDF <- intersectAll(df1, df2)
#' }
#' @note intersectAll since 2.4.0
setMethod("intersectAll",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            intersected <- callJMethod(x@sdf, "intersectAll", y@sdf)
            dataFrame(intersected)
          })

#' except
#'
#' Return a new SparkDataFrame containing rows in this SparkDataFrame
#' but not in another SparkDataFrame. This is equivalent to \code{EXCEPT DISTINCT} in SQL.
#'
#' @param x a SparkDataFrame.
#' @param y a SparkDataFrame.
#' @return A SparkDataFrame containing the result of the except operation.
#' @family SparkDataFrame functions
#' @aliases except,SparkDataFrame,SparkDataFrame-method
#' @rdname except
#' @name except
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' exceptDF <- except(df, df2)
#' }
#' @note except since 1.4.0
setMethod("except",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            excepted <- callJMethod(x@sdf, "except", y@sdf)
            dataFrame(excepted)
          })

#' exceptAll
#'
#' Return a new SparkDataFrame containing rows in this SparkDataFrame
#' but not in another SparkDataFrame while preserving the duplicates.
#' This is equivalent to \code{EXCEPT ALL} in SQL. Also as standard in
#' SQL, this function resolves columns by position (not by name).
#'
#' @param x a SparkDataFrame.
#' @param y a SparkDataFrame.
#' @return A SparkDataFrame containing the result of the except all operation.
#' @family SparkDataFrame functions
#' @aliases exceptAll,SparkDataFrame,SparkDataFrame-method
#' @rdname exceptAll
#' @name exceptAll
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.json(path)
#' df2 <- read.json(path2)
#' exceptAllDF <- exceptAll(df1, df2)
#' }
#' @note exceptAll since 2.4.0
setMethod("exceptAll",
          signature(x = "SparkDataFrame", y = "SparkDataFrame"),
          function(x, y) {
            excepted <- callJMethod(x@sdf, "exceptAll", y@sdf)
            dataFrame(excepted)
          })

#' Save the contents of SparkDataFrame to a data source.
#'
#' The data source is specified by the \code{source} and a set of options (...).
#' If \code{source} is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when data already
#' exists in the data source. There are four modes:
#' \itemize{
#'   \item 'append': Contents of this SparkDataFrame are expected to be appended to existing data.
#'   \item 'overwrite': Existing data is expected to be overwritten by the contents of this
#'         SparkDataFrame.
#'   \item 'error' or 'errorifexists': An exception is expected to be thrown.
#'   \item 'ignore': The save operation is expected to not save the contents of the SparkDataFrame
#'         and to not change the existing data.
#' }
#'
#' @param df a SparkDataFrame.
#' @param path a name for the table.
#' @param source a name for external data source.
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional argument(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @aliases write.df,SparkDataFrame-method
#' @rdname write.df
#' @name write.df
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' write.df(df, "myfile", "parquet", "overwrite")
#' saveDF(df, parquetPath2, "parquet", mode = "append", mergeSchema = TRUE)
#' }
#' @note write.df since 1.4.0
setMethod("write.df",
          signature(df = "SparkDataFrame"),
          function(df, path = NULL, source = NULL, mode = "error", ...) {
            if (!is.null(path) && !is.character(path)) {
              stop("path should be character, NULL or omitted.")
            }
            if (!is.null(source) && !is.character(source)) {
              stop("source should be character, NULL or omitted. It is the datasource specified ",
                   "in 'spark.sql.sources.default' configuration by default.")
            }
            if (!is.character(mode)) {
              stop("mode should be character or omitted. It is 'error' by default.")
            }
            if (is.null(source)) {
              source <- getDefaultSqlSource()
            }
            write <- callJMethod(df@sdf, "write")
            write <- callJMethod(write, "format", source)
            write <- setWriteOptions(write, path = path, mode = mode, ...)
            write <- handledCallJMethod(write, "save")
          })

#' @rdname write.df
#' @name saveDF
#' @aliases saveDF,SparkDataFrame,character-method
#' @note saveDF since 1.4.0
setMethod("saveDF",
          signature(df = "SparkDataFrame", path = "character"),
          function(df, path, source = NULL, mode = "error", ...) {
            write.df(df, path, source, mode, ...)
          })

#' Save the contents of the SparkDataFrame to a data source as a table
#'
#' The data source is specified by the \code{source} and a set of options (...).
#' If \code{source} is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes: \cr
#'  'append': Contents of this SparkDataFrame are expected to be appended to existing data. \cr
#'  'overwrite': Existing data is expected to be overwritten by the contents of this
#'     SparkDataFrame. \cr
#'  'error' or 'errorifexists': An exception is expected to be thrown. \cr
#'  'ignore': The save operation is expected to not save the contents of the SparkDataFrame
#'     and to not change the existing data. \cr
#'
#' @param df a SparkDataFrame.
#' @param tableName a name for the table.
#' @param source a name for external data source.
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional option(s) passed to the method.
#'
#' @family SparkDataFrame functions
#' @aliases saveAsTable,SparkDataFrame,character-method
#' @rdname saveAsTable
#' @name saveAsTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' saveAsTable(df, "myfile")
#' }
#' @note saveAsTable since 1.4.0
setMethod("saveAsTable",
          signature(df = "SparkDataFrame", tableName = "character"),
          function(df, tableName, source = NULL, mode="error", ...) {
            if (is.null(source)) {
              source <- getDefaultSqlSource()
            }
            options <- varargsToStrEnv(...)

            write <- callJMethod(df@sdf, "write")
            write <- callJMethod(write, "format", source)
            write <- setWriteMode(write, mode)
            write <- callJMethod(write, "options", options)
            invisible(callJMethod(write, "saveAsTable", tableName))
          })

#' describe
#'
#' Computes statistics for numeric and string columns.
#' If no columns are given, this function computes statistics for all numerical or string columns.
#'
#' @param x a SparkDataFrame to be computed.
#' @param col a string of name.
#' @param ... additional expressions.
#' @return A SparkDataFrame.
#' @family SparkDataFrame functions
#' @aliases describe,SparkDataFrame,character-method describe,SparkDataFrame,ANY-method
#' @rdname describe
#' @name describe
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' describe(df)
#' describe(df, "col1")
#' describe(df, "col1", "col2")
#' }
#' @seealso See \link{summary} for expanded statistics and control over which statistics to compute.
#' @note describe(SparkDataFrame, character) since 1.4.0
setMethod("describe",
          signature(x = "SparkDataFrame", col = "character"),
          function(x, col, ...) {
            colList <- list(col, ...)
            sdf <- callJMethod(x@sdf, "describe", colList)
            dataFrame(sdf)
          })

#' @rdname describe
#' @name describe
#' @aliases describe,SparkDataFrame-method
#' @note describe(SparkDataFrame) since 1.4.0
setMethod("describe",
          signature(x = "SparkDataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "describe", list())
            dataFrame(sdf)
          })

#' summary
#'
#' Computes specified statistics for numeric and string columns. Available statistics are:
#' \itemize{
#' \item count
#' \item mean
#' \item stddev
#' \item min
#' \item max
#' \item arbitrary approximate percentiles specified as a percentage (eg, "75\%")
#' }
#' If no statistics are given, this function computes count, mean, stddev, min,
#' approximate quartiles (percentiles at 25\%, 50\%, and 75\%), and max.
#' This function is meant for exploratory data analysis, as we make no guarantee about the
#' backward compatibility of the schema of the resulting Dataset. If you want to
#' programmatically compute summary statistics, use the \code{agg} function instead.
#'
#'
#' @param object a SparkDataFrame to be summarized.
#' @param ... (optional) statistics to be computed for all columns.
#' @return A SparkDataFrame.
#' @family SparkDataFrame functions
#' @rdname summary
#' @name summary
#' @aliases summary,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' summary(df)
#' summary(df, "min", "25%", "75%", "max")
#' summary(select(df, "age", "height"))
#' }
#' @note summary(SparkDataFrame) since 1.5.0
#' @note The statistics provided by \code{summary} were change in 2.3.0 use \link{describe} for
#'       previous defaults.
#' @seealso \link{describe}
setMethod("summary",
          signature(object = "SparkDataFrame"),
          function(object, ...) {
            statisticsList <- list(...)
            sdf <- callJMethod(object@sdf, "summary", statisticsList)
            dataFrame(sdf)
          })


#' A set of SparkDataFrame functions working with NA values
#'
#' dropna, na.omit - Returns a new SparkDataFrame omitting rows with null values.
#'
#' @param x a SparkDataFrame.
#' @param how "any" or "all".
#'            if "any", drop a row if it contains any nulls.
#'            if "all", drop a row only if all its values are null.
#'            if \code{minNonNulls} is specified, how is ignored.
#' @param minNonNulls if specified, drop rows that have less than
#'                    \code{minNonNulls} non-null values.
#'                    This overwrites the how parameter.
#' @param cols optional list of column names to consider. In \code{fillna},
#'             columns specified in cols that do not have matching data
#'             type are ignored. For example, if value is a character, and
#'             subset contains a non-character column, then the non-character
#'             column is simply ignored.
#' @return A SparkDataFrame.
#'
#' @family SparkDataFrame functions
#' @rdname nafunctions
#' @aliases dropna,SparkDataFrame-method
#' @name dropna
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' dropna(df)
#' }
#' @note dropna since 1.4.0
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

#' @param object a SparkDataFrame.
#' @param ... further arguments to be passed to or from other methods.
#' @rdname nafunctions
#' @name na.omit
#' @aliases na.omit,SparkDataFrame-method
#' @note na.omit since 1.5.0
setMethod("na.omit",
          signature(object = "SparkDataFrame"),
          function(object, how = c("any", "all"), minNonNulls = NULL, cols = NULL) {
            dropna(object, how, minNonNulls, cols)
          })

#' fillna - Replace null values.
#'
#' @param value value to replace null values with.
#'              Should be an integer, numeric, character or named list.
#'              If the value is a named list, then cols is ignored and
#'              value must be a mapping from column name (character) to
#'              replacement value. The replacement value must be an
#'              integer, numeric or character.
#'
#' @rdname nafunctions
#' @name fillna
#' @aliases fillna,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' fillna(df, 1)
#' fillna(df, list("age" = 20, "name" = "unknown"))
#' }
#' @note fillna since 1.4.0
setMethod("fillna",
          signature(x = "SparkDataFrame"),
          function(x, value, cols = NULL) {
            if (!(class(value) %in% c("integer", "numeric", "character", "list"))) {
              stop("value should be an integer, numeric, character or named list.")
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
                  stop("Each item in value should be an integer, numeric or character.")
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

#' Download data from a SparkDataFrame into a R data.frame
#'
#' This function downloads the contents of a SparkDataFrame into an R's data.frame.
#' Since data.frames are held in memory, ensure that you have enough memory
#' in your system to accommodate the contents.
#'
#' @param x a SparkDataFrame.
#' @param row.names \code{NULL} or a character vector giving the row names for the data frame.
#' @param optional If \code{TRUE}, converting column names is optional.
#' @param ... additional arguments to pass to base::as.data.frame.
#' @return A data.frame.
#' @family SparkDataFrame functions
#' @aliases as.data.frame,SparkDataFrame-method
#' @rdname as.data.frame
#' @examples
#' \dontrun{
#' irisDF <- createDataFrame(iris)
#' df <- as.data.frame(irisDF[irisDF$Species == "setosa", ])
#' }
#' @note as.data.frame since 1.6.0
setMethod("as.data.frame",
          signature(x = "SparkDataFrame"),
          function(x, row.names = NULL, optional = FALSE, ...) {
            as.data.frame(collect(x), row.names, optional, ...)
          })

#' Attach SparkDataFrame to R search path
#'
#' The specified SparkDataFrame is attached to the R search path. This means that
#' the SparkDataFrame is searched by R when evaluating a variable, so columns in
#' the SparkDataFrame can be accessed by simply giving their names.
#'
#' @family SparkDataFrame functions
#' @rdname attach
#' @aliases attach attach,SparkDataFrame-method
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
#' @note attach since 1.6.0
setMethod("attach",
          signature(what = "SparkDataFrame"),
          function(what, pos = 2L, name = deparse(substitute(what), backtick = FALSE),
                   warn.conflicts = TRUE) {
            args <- as.list(environment()) # capture all parameters - this must be the first line
            newEnv <- assignNewEnv(args$what)
            args$what <- newEnv
            do.call(attach, args)
          })

#' Evaluate a R expression in an environment constructed from a SparkDataFrame
#'
#' Evaluate a R expression in an environment constructed from a SparkDataFrame
#' with() allows access to columns of a SparkDataFrame by simply referring to
#' their name. It appends every column of a SparkDataFrame into a new
#' environment. Then, the given expression is evaluated in this new
#' environment.
#'
#' @rdname with
#' @family SparkDataFrame functions
#' @aliases with,SparkDataFrame-method
#' @param data (SparkDataFrame) SparkDataFrame to use for constructing an environment.
#' @param expr (expression) Expression to evaluate.
#' @param ... arguments to be passed to future methods.
#' @examples
#' \dontrun{
#' with(irisDf, nrow(Sepal_Width))
#' }
#' @seealso \link{attach}
#' @note with since 1.6.0
setMethod("with",
          signature(data = "SparkDataFrame"),
          function(data, expr, ...) {
            newEnv <- assignNewEnv(data)
            eval(substitute(expr), envir = newEnv, enclos = newEnv)
          })

#' Compactly display the structure of a dataset
#'
#' Display the structure of a SparkDataFrame, including column names, column types, as well as a
#' a small sample of rows.
#'
#' @name str
#' @rdname str
#' @aliases str,SparkDataFrame-method
#' @family SparkDataFrame functions
#' @param object a SparkDataFrame
#' @examples
#' \dontrun{
#' # Create a SparkDataFrame from the Iris dataset
#' irisDF <- createDataFrame(iris)
#'
#' # Show the structure of the SparkDataFrame
#' str(irisDF)
#' }
#' @note str since 1.6.1
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
              for (i in seq_len(ncol(localDF))) {
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
#' @param x a SparkDataFrame.
#' @param col a character vector of column names or a Column.
#' @param ... further arguments to be passed to or from other methods.
#' @return A SparkDataFrame.
#'
#' @family SparkDataFrame functions
#' @rdname drop
#' @name drop
#' @aliases drop,SparkDataFrame-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' drop(df, "col1")
#' drop(df, c("col1", "col2"))
#' drop(df, df$col1)
#' }
#' @note drop since 2.0.0
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
#' @name drop
#' @rdname drop
#' @aliases drop,ANY-method
setMethod("drop",
          signature(x = "ANY"),
          function(x) {
            base::drop(x)
          })

#' Compute histogram statistics for given column
#'
#' This function computes a histogram for a given SparkR Column.
#'
#' @name histogram
#' @param nbins the number of bins (optional). Default value is 10.
#' @param col the column as Character string or a Column to build the histogram from.
#' @param df the SparkDataFrame containing the Column to build the histogram from.
#' @return a data.frame with the histogram statistics, i.e., counts and centroids.
#' @rdname histogram
#' @aliases histogram,SparkDataFrame,characterOrColumn-method
#' @family SparkDataFrame functions
#' @examples
#' \dontrun{
#'
#' # Create a SparkDataFrame from the Iris dataset
#' irisDF <- createDataFrame(iris)
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
#' @note histogram since 2.0.0
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
              colname <- paste(base::sample(c(letters, LETTERS),
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

#' Save the content of SparkDataFrame to an external database table via JDBC.
#'
#' Save the content of the SparkDataFrame to an external database table via JDBC. Additional JDBC
#' database connection properties can be set (...)
#'
#' Also, mode is used to specify the behavior of the save operation when
#' data already exists in the data source. There are four modes:
#' \itemize{
#'   \item 'append': Contents of this SparkDataFrame are expected to be appended to existing data.
#'   \item 'overwrite': Existing data is expected to be overwritten by the contents of this
#'         SparkDataFrame.
#'   \item 'error' or 'errorifexists': An exception is expected to be thrown.
#'   \item 'ignore': The save operation is expected to not save the contents of the SparkDataFrame
#'         and to not change the existing data.
#' }
#'
#' @param x a SparkDataFrame.
#' @param url JDBC database url of the form \code{jdbc:subprotocol:subname}.
#' @param tableName yhe name of the table in the external database.
#' @param mode one of 'append', 'overwrite', 'error', 'errorifexists', 'ignore'
#'             save mode (it is 'error' by default)
#' @param ... additional JDBC database connection properties.
#' @family SparkDataFrame functions
#' @rdname write.jdbc
#' @name write.jdbc
#' @aliases write.jdbc,SparkDataFrame,character,character-method
#' @examples
#'\dontrun{
#' sparkR.session()
#' jdbcUrl <- "jdbc:mysql://localhost:3306/databasename"
#' write.jdbc(df, jdbcUrl, "table", user = "username", password = "password")
#' }
#' @note write.jdbc since 2.0.0
setMethod("write.jdbc",
          signature(x = "SparkDataFrame", url = "character", tableName = "character"),
          function(x, url, tableName, mode = "error", ...) {
            jprops <- varargsToJProperties(...)
            write <- callJMethod(x@sdf, "write")
            write <- setWriteMode(write, mode)
            invisible(handledCallJMethod(write, "jdbc", url, tableName, jprops))
          })

#' randomSplit
#'
#' Return a list of randomly split dataframes with the provided weights.
#'
#' @param x A SparkDataFrame
#' @param weights A vector of weights for splits, will be normalized if they don't sum to 1
#' @param seed A seed to use for random split
#'
#' @family SparkDataFrame functions
#' @aliases randomSplit,SparkDataFrame,numeric-method
#' @rdname randomSplit
#' @name randomSplit
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createDataFrame(data.frame(id = 1:1000))
#' df_list <- randomSplit(df, c(2, 3, 5), 0)
#' # df_list contains 3 SparkDataFrames with each having about 200, 300 and 500 rows respectively
#' sapply(df_list, count)
#' }
#' @note randomSplit since 2.0.0
setMethod("randomSplit",
          signature(x = "SparkDataFrame", weights = "numeric"),
          function(x, weights, seed) {
            if (!all(sapply(weights, function(c) { c >= 0 }))) {
              stop("all weight values should not be negative")
            }
            normalized_list <- as.list(weights / sum(weights))
            if (!missing(seed)) {
              sdfs <- callJMethod(x@sdf, "randomSplit", normalized_list, as.integer(seed))
            } else {
              sdfs <- callJMethod(x@sdf, "randomSplit", normalized_list)
            }
            sapply(sdfs, dataFrame)
          })

#' getNumPartitions
#'
#' Return the number of partitions
#'
#' @param x A SparkDataFrame
#' @family SparkDataFrame functions
#' @aliases getNumPartitions,SparkDataFrame-method
#' @rdname getNumPartitions
#' @name getNumPartitions
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createDataFrame(cars, numPartitions = 2)
#' getNumPartitions(df)
#' }
#' @note getNumPartitions since 2.1.1
setMethod("getNumPartitions",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(callJMethod(x@sdf, "rdd"), "getNumPartitions")
          })

#' isStreaming
#'
#' Returns TRUE if this SparkDataFrame contains one or more sources that continuously return data
#' as it arrives. A dataset that reads data from a streaming source must be executed as a
#' \code{StreamingQuery} using \code{write.stream}.
#'
#' @param x A SparkDataFrame
#' @return TRUE if this SparkDataFrame is from a streaming source
#' @family SparkDataFrame functions
#' @aliases isStreaming,SparkDataFrame-method
#' @rdname isStreaming
#' @name isStreaming
#' @seealso \link{read.stream} \link{write.stream}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.stream("socket", host = "localhost", port = 9999)
#' isStreaming(df)
#' }
#' @note isStreaming since 2.2.0
#' @note experimental
setMethod("isStreaming",
          signature(x = "SparkDataFrame"),
          function(x) {
            callJMethod(x@sdf, "isStreaming")
          })

#' Write the streaming SparkDataFrame to a data source.
#'
#' The data source is specified by the \code{source} and a set of options (...).
#' If \code{source} is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, \code{outputMode} specifies how data of a streaming SparkDataFrame is written to a
#' output data source. There are three modes:
#' \itemize{
#'   \item append: Only the new rows in the streaming SparkDataFrame will be written out. This
#'                 output mode can be only be used in queries that do not contain any aggregation.
#'   \item complete: All the rows in the streaming SparkDataFrame will be written out every time
#'                   there are some updates. This output mode can only be used in queries that
#'                   contain aggregations.
#'   \item update: Only the rows that were updated in the streaming SparkDataFrame will be written
#'                 out every time there are some updates. If the query doesn't contain aggregations,
#'                 it will be equivalent to \code{append} mode.
#' }
#'
#' @param df a streaming SparkDataFrame.
#' @param source a name for external data source.
#' @param outputMode one of 'append', 'complete', 'update'.
#' @param partitionBy a name or a list of names of columns to partition the output by on the file
#'        system. If specified, the output is laid out on the file system similar to Hive's
#'        partitioning scheme.
#' @param trigger.processingTime a processing time interval as a string, e.g. '5 seconds',
#'        '1 minute'. This is a trigger that runs a query periodically based on the processing
#'        time. If value is '0 seconds', the query will run as fast as possible, this is the
#'        default. Only one trigger can be set.
#' @param trigger.once a logical, must be set to \code{TRUE}. This is a trigger that processes only
#'        one batch of data in a streaming query then terminates the query. Only one trigger can be
#'        set.
#' @param ... additional external data source specific named options.
#'
#' @family SparkDataFrame functions
#' @seealso \link{read.stream}
#' @aliases write.stream,SparkDataFrame-method
#' @rdname write.stream
#' @name write.stream
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.stream("socket", host = "localhost", port = 9999)
#' isStreaming(df)
#' wordCounts <- count(group_by(df, "value"))
#'
#' # console
#' q <- write.stream(wordCounts, "console", outputMode = "complete")
#' # text stream
#' q <- write.stream(df, "text", path = "/home/user/out", checkpointLocation = "/home/user/cp"
#'                   partitionBy = c("year", "month"), trigger.processingTime = "30 seconds")
#' # memory stream
#' q <- write.stream(wordCounts, "memory", queryName = "outs", outputMode = "complete")
#' head(sql("SELECT * from outs"))
#' queryName(q)
#'
#' stopQuery(q)
#' }
#' @note write.stream since 2.2.0
#' @note experimental
setMethod("write.stream",
          signature(df = "SparkDataFrame"),
          function(df, source = NULL, outputMode = NULL, partitionBy = NULL,
                   trigger.processingTime = NULL, trigger.once = NULL, ...) {
            if (!is.null(source) && !is.character(source)) {
              stop("source should be character, NULL or omitted. It is the data source specified ",
                   "in 'spark.sql.sources.default' configuration by default.")
            }
            if (!is.null(outputMode) && !is.character(outputMode)) {
              stop("outputMode should be character or omitted.")
            }
            if (is.null(source)) {
              source <- getDefaultSqlSource()
            }
            cols <- NULL
            if (!is.null(partitionBy)) {
              if (!all(sapply(partitionBy, function(c) { is.character(c) }))) {
                stop("All partitionBy column names should be characters.")
              }
              cols <- as.list(partitionBy)
            }
            jtrigger <- NULL
            if (!is.null(trigger.processingTime) && !is.na(trigger.processingTime)) {
              if (!is.null(trigger.once)) {
                stop("Multiple triggers not allowed.")
              }
              interval <- as.character(trigger.processingTime)
              if (nchar(interval) == 0) {
                stop("Value for trigger.processingTime must be a non-empty string.")
              }
              jtrigger <- handledCallJStatic("org.apache.spark.sql.streaming.Trigger",
                                             "ProcessingTime",
                                             interval)
            } else if (!is.null(trigger.once) && !is.na(trigger.once)) {
              if (!is.logical(trigger.once) || !trigger.once) {
                stop("Value for trigger.once must be TRUE.")
              }
              jtrigger <- callJStatic("org.apache.spark.sql.streaming.Trigger", "Once")
            }
            options <- varargsToStrEnv(...)
            write <- handledCallJMethod(df@sdf, "writeStream")
            write <- callJMethod(write, "format", source)
            if (!is.null(outputMode)) {
              write <- callJMethod(write, "outputMode", outputMode)
            }
            if (!is.null(cols)) {
              write <- callJMethod(write, "partitionBy", cols)
            }
            if (!is.null(jtrigger)) {
              write <- callJMethod(write, "trigger", jtrigger)
            }
            write <- callJMethod(write, "options", options)
            ssq <- handledCallJMethod(write, "start")
            streamingQuery(ssq)
          })

#' checkpoint
#'
#' Returns a checkpointed version of this SparkDataFrame. Checkpointing can be used to truncate the
#' logical plan, which is especially useful in iterative algorithms where the plan may grow
#' exponentially. It will be saved to files inside the checkpoint directory set with
#' \code{setCheckpointDir}
#'
#' @param x A SparkDataFrame
#' @param eager whether to checkpoint this SparkDataFrame immediately
#' @return a new checkpointed SparkDataFrame
#' @family SparkDataFrame functions
#' @aliases checkpoint,SparkDataFrame-method
#' @rdname checkpoint
#' @name checkpoint
#' @seealso \link{setCheckpointDir}
#' @examples
#'\dontrun{
#' setCheckpointDir("/checkpoint")
#' df <- checkpoint(df)
#' }
#' @note checkpoint since 2.2.0
setMethod("checkpoint",
          signature(x = "SparkDataFrame"),
          function(x, eager = TRUE) {
            df <- callJMethod(x@sdf, "checkpoint", as.logical(eager))
            dataFrame(df)
          })

#' localCheckpoint
#'
#' Returns a locally checkpointed version of this SparkDataFrame. Checkpointing can be used to
#' truncate the logical plan, which is especially useful in iterative algorithms where the plan
#' may grow exponentially. Local checkpoints are stored in the executors using the caching
#' subsystem and therefore they are not reliable.
#'
#' @param x A SparkDataFrame
#' @param eager whether to locally checkpoint this SparkDataFrame immediately
#' @return a new locally checkpointed SparkDataFrame
#' @family SparkDataFrame functions
#' @aliases localCheckpoint,SparkDataFrame-method
#' @rdname localCheckpoint
#' @name localCheckpoint
#' @examples
#'\dontrun{
#' df <- localCheckpoint(df)
#' }
#' @note localCheckpoint since 2.3.0
setMethod("localCheckpoint",
          signature(x = "SparkDataFrame"),
          function(x, eager = TRUE) {
            df <- callJMethod(x@sdf, "localCheckpoint", as.logical(eager))
            dataFrame(df)
          })

#' cube
#'
#' Create a multi-dimensional cube for the SparkDataFrame using the specified columns.
#'
#' If grouping expression is missing \code{cube} creates a single global aggregate and is
#' equivalent to direct application of \link{agg}.
#'
#' @param x a SparkDataFrame.
#' @param ... character name(s) or Column(s) to group on.
#' @return A GroupedData.
#' @family SparkDataFrame functions
#' @aliases cube,SparkDataFrame-method
#' @rdname cube
#' @name cube
#' @examples
#' \dontrun{
#' df <- createDataFrame(mtcars)
#' mean(cube(df, "cyl", "gear", "am"), "mpg")
#'
#' # Following calls are equivalent
#' agg(cube(df), mean(df$mpg))
#' agg(df, mean(df$mpg))
#' }
#' @note cube since 2.3.0
#' @seealso \link{agg}, \link{groupBy}, \link{rollup}
setMethod("cube",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            cols <- list(...)
            jcol <- lapply(cols, function(x) if (class(x) == "Column") x@jc else column(x)@jc)
            sgd <- callJMethod(x@sdf, "cube", jcol)
            groupedData(sgd)
          })

#' rollup
#'
#' Create a multi-dimensional rollup for the SparkDataFrame using the specified columns.
#'
#' If grouping expression is missing \code{rollup} creates a single global aggregate and is
#' equivalent to direct application of \link{agg}.
#'
#' @param x a SparkDataFrame.
#' @param ... character name(s) or Column(s) to group on.
#' @return A GroupedData.
#' @family SparkDataFrame functions
#' @aliases rollup,SparkDataFrame-method
#' @rdname rollup
#' @name rollup
#' @examples
#'\dontrun{
#' df <- createDataFrame(mtcars)
#' mean(rollup(df, "cyl", "gear", "am"), "mpg")
#'
#' # Following calls are equivalent
#' agg(rollup(df), mean(df$mpg))
#' agg(df, mean(df$mpg))
#' }
#' @note rollup since 2.3.0
#' @seealso \link{agg}, \link{cube}, \link{groupBy}
setMethod("rollup",
          signature(x = "SparkDataFrame"),
          function(x, ...) {
            cols <- list(...)
            jcol <- lapply(cols, function(x) if (class(x) == "Column") x@jc else column(x)@jc)
            sgd <- callJMethod(x@sdf, "rollup", jcol)
            groupedData(sgd)
          })

#' hint
#'
#' Specifies execution plan hint and return a new SparkDataFrame.
#'
#' @param x a SparkDataFrame.
#' @param name a name of the hint.
#' @param ... optional parameters for the hint.
#' @return A SparkDataFrame.
#' @family SparkDataFrame functions
#' @aliases hint,SparkDataFrame,character-method
#' @rdname hint
#' @name hint
#' @examples
#' \dontrun{
#' df <- createDataFrame(mtcars)
#' avg_mpg <- mean(groupBy(createDataFrame(mtcars), "cyl"), "mpg")
#'
#' head(join(df, hint(avg_mpg, "broadcast"), df$cyl == avg_mpg$cyl))
#' }
#' @note hint since 2.2.0
setMethod("hint",
          signature(x = "SparkDataFrame", name = "character"),
          function(x, name, ...) {
            parameters <- list(...)
            stopifnot(all(sapply(parameters, is.character)))
            jdf <- callJMethod(x@sdf, "hint", name, parameters)
            dataFrame(jdf)
          })

#' alias
#'
#' @aliases alias,SparkDataFrame-method
#' @family SparkDataFrame functions
#' @rdname alias
#' @name alias
#' @examples
#' \dontrun{
#' df <- alias(createDataFrame(mtcars), "mtcars")
#' avg_mpg <- alias(agg(groupBy(df, df$cyl), avg(df$mpg)), "avg_mpg")
#'
#' head(select(df, column("mtcars.mpg")))
#' head(join(df, avg_mpg, column("mtcars.cyl") == column("avg_mpg.cyl")))
#' }
#' @note alias(SparkDataFrame) since 2.3.0
setMethod("alias",
          signature(object = "SparkDataFrame"),
          function(object, data) {
            stopifnot(is.character(data))
            sdf <- callJMethod(object@sdf, "alias", data)
            dataFrame(sdf)
          })

#' broadcast
#'
#' Return a new SparkDataFrame marked as small enough for use in broadcast joins.
#'
#' Equivalent to \code{hint(x, "broadcast")}.
#'
#' @param x a SparkDataFrame.
#' @return a SparkDataFrame.
#'
#' @aliases broadcast,SparkDataFrame-method
#' @family SparkDataFrame functions
#' @rdname broadcast
#' @name broadcast
#' @examples
#' \dontrun{
#' df <- createDataFrame(mtcars)
#' avg_mpg <- mean(groupBy(createDataFrame(mtcars), "cyl"), "mpg")
#'
#' head(join(df, broadcast(avg_mpg), df$cyl == avg_mpg$cyl))
#' }
#' @note broadcast since 2.3.0
setMethod("broadcast",
          signature(x = "SparkDataFrame"),
          function(x) {
            sdf <- callJStatic("org.apache.spark.sql.functions", "broadcast", x@sdf)
            dataFrame(sdf)
          })

#' withWatermark
#'
#' Defines an event time watermark for this streaming SparkDataFrame. A watermark tracks a point in
#' time before which we assume no more late data is going to arrive.
#'
#' Spark will use this watermark for several purposes:
#' \itemize{
#'  \item To know when a given time window aggregation can be finalized and thus can be emitted
#' when using output modes that do not allow updates.
#'  \item To minimize the amount of state that we need to keep for on-going aggregations.
#' }
#' The current watermark is computed by looking at the \code{MAX(eventTime)} seen across
#' all of the partitions in the query minus a user specified \code{delayThreshold}. Due to the cost
#' of coordinating this value across partitions, the actual watermark used is only guaranteed
#' to be at least \code{delayThreshold} behind the actual event time.  In some cases we may still
#' process records that arrive more than \code{delayThreshold} late.
#'
#' @param x a streaming SparkDataFrame
#' @param eventTime a string specifying the name of the Column that contains the event time of the
#'                  row.
#' @param delayThreshold a string specifying the minimum delay to wait to data to arrive late,
#'                       relative to the latest record that has been processed in the form of an
#'                       interval (e.g. "1 minute" or "5 hours"). NOTE: This should not be negative.
#' @return a SparkDataFrame.
#' @aliases withWatermark,SparkDataFrame,character,character-method
#' @family SparkDataFrame functions
#' @rdname withWatermark
#' @name withWatermark
#' @examples
#' \dontrun{
#' sparkR.session()
#' schema <- structType(structField("time", "timestamp"), structField("value", "double"))
#' df <- read.stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = 1)
#' df <- withWatermark(df, "time", "10 minutes")
#' }
#' @note withWatermark since 2.3.0
setMethod("withWatermark",
          signature(x = "SparkDataFrame", eventTime = "character", delayThreshold = "character"),
          function(x, eventTime, delayThreshold) {
            sdf <- callJMethod(x@sdf, "withWatermark", eventTime, delayThreshold)
            dataFrame(sdf)
          })

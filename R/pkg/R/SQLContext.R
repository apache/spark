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

# SQLcontext.R: SQLContext-driven functions


# Map top level R type to SQL type
getInternalType <- function(x) {
  # class of POSIXlt is c("POSIXlt" "POSIXt")
  switch(class(x)[[1]],
         integer = "integer",
         character = "string",
         logical = "boolean",
         double = "double",
         numeric = "double",
         raw = "binary",
         list = "array",
         struct = "struct",
         environment = "map",
         Date = "date",
         POSIXlt = "timestamp",
         POSIXct = "timestamp",
         stop(paste("Unsupported type for DataFrame:", class(x))))
}

#' infer the SQL type
infer_type <- function(x) {
  if (is.null(x)) {
    stop("can not infer type from NULL")
  }

  type <- getInternalType(x)

  if (type == "map") {
    stopifnot(length(x) > 0)
    key <- ls(x)[[1]]
    paste0("map<string,", infer_type(get(key, x)), ">")
  } else if (type == "array") {
    stopifnot(length(x) > 0)

    paste0("array<", infer_type(x[[1]]), ">")
  } else if (type == "struct") {
    stopifnot(length(x) > 0)
    names <- names(x)
    stopifnot(!is.null(names))

    type <- lapply(seq_along(x), function(i) {
      paste0(names[[i]], ":", infer_type(x[[i]]), ",")
    })
    type <- Reduce(paste0, type)
    type <- paste0("struct<", substr(type, 1, nchar(type) - 1), ">")
  } else if (length(x) > 1 && type != "binary") {
    paste0("array<", infer_type(x[[1]]), ">")
  } else {
    type
  }
}

#' Create a DataFrame
#'
#' Converts R data.frame or list into DataFrame.
#'
#' @param sqlContext A SQLContext
#' @param data An RDD or list or data.frame
#' @param schema a list of column names or named list (StructType), optional
#' @return an DataFrame
#' @rdname createDataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- as.DataFrame(sqlContext, iris)
#' df2 <- as.DataFrame(sqlContext, list(3,4,5,6))
#' df3 <- createDataFrame(sqlContext, iris)
#' }

# TODO(davies): support sampling and infer type from NA
createDataFrame <- function(sqlContext, data, schema = NULL, samplingRatio = 1.0) {
  if (is.data.frame(data)) {
      # get the names of columns, they will be put into RDD
      if (is.null(schema)) {
        schema <- names(data)
      }

      # get rid of factor type
      cleanCols <- function(x) {
        if (is.factor(x)) {
          as.character(x)
        } else {
          x
        }
      }

      # drop factors and wrap lists
      data <- setNames(lapply(data, cleanCols), NULL)

      # check if all columns have supported type
      lapply(data, getInternalType)

      # convert to rows
      args <- list(FUN = list, SIMPLIFY = FALSE, USE.NAMES = FALSE)
      data <- do.call(mapply, append(args, data))
  }
  if (is.list(data)) {
    sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sqlContext)
    rdd <- parallelize(sc, data)
  } else if (inherits(data, "RDD")) {
    rdd <- data
  } else {
    stop(paste("unexpected type:", class(data)))
  }

  if (is.null(schema) || (!inherits(schema, "structType") && is.null(names(schema)))) {
    row <- first(rdd)
    names <- if (is.null(schema)) {
      names(row)
    } else {
      as.list(schema)
    }
    if (is.null(names)) {
      names <- lapply(1:length(row), function(x) {
        paste("_", as.character(x), sep = "")
      })
    }

    # SPAKR-SQL does not support '.' in column name, so replace it with '_'
    # TODO(davies): remove this once SPARK-2775 is fixed
    names <- lapply(names, function(n) {
      nn <- gsub("[.]", "_", n)
      if (nn != n) {
        warning(paste("Use", nn, "instead of", n, " as column name"))
      }
      nn
    })

    types <- lapply(row, infer_type)
    fields <- lapply(1:length(row), function(i) {
      structField(names[[i]], types[[i]], TRUE)
    })
    schema <- do.call(structType, fields)
  }

  stopifnot(class(schema) == "structType")

  jrdd <- getJRDD(lapply(rdd, function(x) x), "row")
  srdd <- callJMethod(jrdd, "rdd")
  sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "createDF",
                     srdd, schema$jobj, sqlContext)
  dataFrame(sdf)
}

#' @rdname createDataFrame
#' @aliases createDataFrame
#' @export
as.DataFrame <- function(sqlContext, data, schema = NULL, samplingRatio = 1.0) {
  createDataFrame(sqlContext, data, schema, samplingRatio)
}

#' toDF
#'
#' Converts an RDD to a DataFrame by infer the types.
#'
#' @param x An RDD
#'
#' @rdname DataFrame
#' @noRd
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' rdd <- lapply(parallelize(sc, 1:10), function(x) list(a=x, b=as.character(x)))
#' df <- toDF(rdd)
#'}
setGeneric("toDF", function(x, ...) { standardGeneric("toDF") })

setMethod("toDF", signature(x = "RDD"),
          function(x, ...) {
            sqlContext <- if (exists(".sparkRHivesc", envir = .sparkREnv)) {
              get(".sparkRHivesc", envir = .sparkREnv)
            } else if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
              get(".sparkRSQLsc", envir = .sparkREnv)
            } else {
              stop("no SQL context available")
            }
            createDataFrame(sqlContext, x, ...)
          })

#' Create a DataFrame from a JSON file.
#'
#' Loads a JSON file (one object per line), returning the result as a DataFrame
#' It goes through the entire dataset once to determine the schema.
#'
#' @param sqlContext SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @rdname read.json
#' @name read.json
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' df <- jsonFile(sqlContext, path)
#' }
read.json <- function(sqlContext, path) {
  # Allow the user to have a more flexible definiton of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sqlContext, "read")
  sdf <- callJMethod(read, "json", paths)
  dataFrame(sdf)
}

#' @rdname read.json
#' @name jsonFile
#' @export
jsonFile <- function(sqlContext, path) {
  .Deprecated("read.json")
  read.json(sqlContext, path)
}


#' JSON RDD
#'
#' Loads an RDD storing one JSON object per string as a DataFrame.
#'
#' @param sqlContext SQLContext to use
#' @param rdd An RDD of JSON string
#' @param schema A StructType object to use as schema
#' @param samplingRatio The ratio of simpling used to infer the schema
#' @return A DataFrame
#' @noRd
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' rdd <- texFile(sc, "path/to/json")
#' df <- jsonRDD(sqlContext, rdd)
#'}

# TODO: support schema
jsonRDD <- function(sqlContext, rdd, schema = NULL, samplingRatio = 1.0) {
  rdd <- serializeToString(rdd)
  if (is.null(schema)) {
    sdf <- callJMethod(sqlContext, "jsonRDD", callJMethod(getJRDD(rdd), "rdd"), samplingRatio)
    dataFrame(sdf)
  } else {
    stop("not implemented")
  }
}

#' Create a DataFrame from a Parquet file.
#'
#' Loads a Parquet file, returning the result as a DataFrame.
#'
#' @param sqlContext SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @rdname read.parquet
#' @name read.parquet
#' @export
read.parquet <- function(sqlContext, path) {
  # Allow the user to have a more flexible definiton of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sqlContext, "read")
  sdf <- callJMethod(read, "parquet", paths)
  dataFrame(sdf)
}

#' @rdname read.parquet
#' @name parquetFile
#' @export
# TODO: Implement saveasParquetFile and write examples for both
parquetFile <- function(sqlContext, ...) {
  .Deprecated("read.parquet")
  # Allow the user to have a more flexible definiton of the text file path
  paths <- lapply(list(...), function(x) suppressWarnings(normalizePath(x)))
  sdf <- callJMethod(sqlContext, "parquetFile", paths)
  dataFrame(sdf)
}

#' SQL Query
#'
#' Executes a SQL query using Spark, returning the result as a DataFrame.
#'
#' @param sqlContext SQLContext to use
#' @param sqlQuery A character vector containing the SQL query
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' registerTempTable(df, "table")
#' new_df <- sql(sqlContext, "SELECT * FROM table")
#' }

sql <- function(sqlContext, sqlQuery) {
 sdf <- callJMethod(sqlContext, "sql", sqlQuery)
 dataFrame(sdf)
}

#' Create a DataFrame from a SparkSQL Table
#'
#' Returns the specified Table as a DataFrame.  The Table must have already been registered
#' in the SQLContext.
#'
#' @param sqlContext SQLContext to use
#' @param tableName The SparkSQL Table to convert to a DataFrame.
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' registerTempTable(df, "table")
#' new_df <- table(sqlContext, "table")
#' }

table <- function(sqlContext, tableName) {
  sdf <- callJMethod(sqlContext, "table", tableName)
  dataFrame(sdf)
}


#' Tables
#'
#' Returns a DataFrame containing names of tables in the given database.
#'
#' @param sqlContext SQLContext to use
#' @param databaseName name of the database
#' @return a DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' tables(sqlContext, "hive")
#' }

tables <- function(sqlContext, databaseName = NULL) {
  jdf <- if (is.null(databaseName)) {
    callJMethod(sqlContext, "tables")
  } else {
    callJMethod(sqlContext, "tables", databaseName)
  }
  dataFrame(jdf)
}


#' Table Names
#'
#' Returns the names of tables in the given database as an array.
#'
#' @param sqlContext SQLContext to use
#' @param databaseName name of the database
#' @return a list of table names
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' tableNames(sqlContext, "hive")
#' }

tableNames <- function(sqlContext, databaseName = NULL) {
  if (is.null(databaseName)) {
    callJMethod(sqlContext, "tableNames")
  } else {
    callJMethod(sqlContext, "tableNames", databaseName)
  }
}


#' Cache Table
#'
#' Caches the specified table in-memory.
#'
#' @param sqlContext SQLContext to use
#' @param tableName The name of the table being cached
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' registerTempTable(df, "table")
#' cacheTable(sqlContext, "table")
#' }

cacheTable <- function(sqlContext, tableName) {
  callJMethod(sqlContext, "cacheTable", tableName)
}

#' Uncache Table
#'
#' Removes the specified table from the in-memory cache.
#'
#' @param sqlContext SQLContext to use
#' @param tableName The name of the table being uncached
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- read.json(sqlContext, path)
#' registerTempTable(df, "table")
#' uncacheTable(sqlContext, "table")
#' }

uncacheTable <- function(sqlContext, tableName) {
  callJMethod(sqlContext, "uncacheTable", tableName)
}

#' Clear Cache
#'
#' Removes all cached tables from the in-memory cache.
#'
#' @param sqlContext SQLContext to use
#' @examples
#' \dontrun{
#' clearCache(sqlContext)
#' }

clearCache <- function(sqlContext) {
  callJMethod(sqlContext, "clearCache")
}

#' Drop Temporary Table
#'
#' Drops the temporary table with the given table name in the catalog.
#' If the table has been cached/persisted before, it's also unpersisted.
#'
#' @param sqlContext SQLContext to use
#' @param tableName The name of the SparkSQL table to be dropped.
#' @examples
#' \dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df <- read.df(sqlContext, path, "parquet")
#' registerTempTable(df, "table")
#' dropTempTable(sqlContext, "table")
#' }

dropTempTable <- function(sqlContext, tableName) {
  if (class(tableName) != "character") {
    stop("tableName must be a string.")
  }
  callJMethod(sqlContext, "dropTempTable", tableName)
}

#' Load an DataFrame
#'
#' Returns the dataset in a data source as a DataFrame
#'
#' The data source is specified by the `source` and a set of options(...).
#' If `source` is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param sqlContext SQLContext to use
#' @param path The path of files to load
#' @param source The name of external data source
#' @param schema The data schema defined in structType
#' @return DataFrame
#' @rdname read.df
#' @name read.df
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df1 <- read.df(sqlContext, "path/to/file.json", source = "json")
#' schema <- structType(structField("name", "string"),
#'                      structField("info", "map<string,double>"))
#' df2 <- read.df(sqlContext, mapTypeJsonPath, "json", schema)
#' df3 <- loadDF(sqlContext, "data/test_table", "parquet", mergeSchema = "true")
#' }

read.df <- function(sqlContext, path = NULL, source = NULL, schema = NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
    sqlContext <- get(".sparkRSQLsc", envir = .sparkREnv)
    source <- callJMethod(sqlContext, "getConf", "spark.sql.sources.default",
                          "org.apache.spark.sql.parquet")
  }
  if (!is.null(schema)) {
    stopifnot(class(schema) == "structType")
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "loadDF", sqlContext, source,
                       schema$jobj, options)
  } else {
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "loadDF", sqlContext, source, options)
  }
  dataFrame(sdf)
}

#' @rdname read.df
#' @name loadDF
loadDF <- function(sqlContext, path = NULL, source = NULL, schema = NULL, ...) {
  read.df(sqlContext, path, source, schema, ...)
}

#' Create an external table
#'
#' Creates an external table based on the dataset in a data source,
#' Returns the DataFrame associated with the external table.
#'
#' The data source is specified by the `source` and a set of options(...).
#' If `source` is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param sqlContext SQLContext to use
#' @param tableName A name of the table
#' @param path The path of files to load
#' @param source the name of external data source
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRSQL.init(sc)
#' df <- sparkRSQL.createExternalTable(sqlContext, "myjson", path="path/to/json", source="json")
#' }

createExternalTable <- function(sqlContext, tableName, path = NULL, source = NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  sdf <- callJMethod(sqlContext, "createExternalTable", tableName, source, options)
  dataFrame(sdf)
}

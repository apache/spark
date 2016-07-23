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
         stop(paste("Unsupported type for SparkDataFrame:", class(x))))
}

#' return the SparkSession
#' @noRd
getSparkSession <- function() {
  if (exists(".sparkRsession", envir = .sparkREnv)) {
    get(".sparkRsession", envir = .sparkREnv)
  } else {
    stop("SparkSession not initialized")
  }
}

#' infer the SQL type
#' @noRd
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

#' Get Runtime Config from the current active SparkSession
#'
#' Get Runtime Config from the current active SparkSession.
#' To change SparkSession Runtime Config, please see `sparkR.session()`.
#'
#' @param key (optional) The key of the config to get, if omitted, all config is returned
#' @param defaultValue (optional) The default value of the config to return if they config is not
#' set, if omitted, the call fails if the config key is not set
#' @return a list of config values with keys as their names
#' @rdname sparkR.conf
#' @name sparkR.conf
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' allConfigs <- sparkR.conf()
#' masterValue <- unlist(sparkR.conf("spark.master"))
#' namedConfig <- sparkR.conf("spark.executor.memory", "0g")
#' }
#' @note sparkR.conf since 2.0.0
sparkR.conf <- function(key, defaultValue) {
  sparkSession <- getSparkSession()
  if (missing(key)) {
    m <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getSessionConf", sparkSession)
    as.list(m, all.names = TRUE, sorted = TRUE)
  } else {
    conf <- callJMethod(sparkSession, "conf")
    value <- if (missing(defaultValue)) {
      tryCatch(callJMethod(conf, "get", key),
              error = function(e) {
                if (any(grep("java.util.NoSuchElementException", as.character(e)))) {
                  stop(paste0("Config '", key, "' is not set"))
                } else {
                  stop(paste0("Unknown error: ", as.character(e)))
                }
              })
    } else {
      callJMethod(conf, "get", key, defaultValue)
    }
    l <- setNames(list(value), key)
    l
  }
}

getDefaultSqlSource <- function() {
  l <- sparkR.conf("spark.sql.sources.default", "org.apache.spark.sql.parquet")
  l[["spark.sql.sources.default"]]
}

#' Create a SparkDataFrame
#'
#' Converts R data.frame or list into SparkDataFrame.
#'
#' @param data An RDD or list or data.frame
#' @param schema a list of column names or named list (StructType), optional
#' @return a SparkDataFrame
#' @rdname createDataFrame
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- as.DataFrame(iris)
#' df2 <- as.DataFrame(list(3,4,5,6))
#' df3 <- createDataFrame(iris)
#' }
#' @name createDataFrame
#' @note createDataFrame since 1.4.0
# TODO(davies): support sampling and infer type from NA
createDataFrame <- function(data, schema = NULL, samplingRatio = 1.0) {
  sparkSession <- getSparkSession()
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
    sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)
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
                     srdd, schema$jobj, sparkSession)
  dataFrame(sdf)
}

#' @rdname createDataFrame
#' @aliases createDataFrame
#' @export
#' @note as.DataFrame since 1.6.0
as.DataFrame <- function(data, schema = NULL, samplingRatio = 1.0) {
  createDataFrame(data, schema, samplingRatio)
}

#' toDF
#'
#' Converts an RDD to a SparkDataFrame by infer the types.
#'
#' @param x An RDD
#'
#' @rdname SparkDataFrame
#' @noRd
#' @examples
#'\dontrun{
#' sparkR.session()
#' rdd <- lapply(parallelize(sc, 1:10), function(x) list(a=x, b=as.character(x)))
#' df <- toDF(rdd)
#'}
setGeneric("toDF", function(x, ...) { standardGeneric("toDF") })

setMethod("toDF", signature(x = "RDD"),
          function(x, ...) {
            createDataFrame(x, ...)
          })

#' Create a SparkDataFrame from a JSON file.
#'
#' Loads a JSON file (one object per line), returning the result as a SparkDataFrame
#' It goes through the entire dataset once to determine the schema.
#'
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return SparkDataFrame
#' @rdname read.json
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' df <- jsonFile(path)
#' }
#' @name read.json
#' @note read.json since 1.6.0
read.json <- function(path) {
  sparkSession <- getSparkSession()
  # Allow the user to have a more flexible definiton of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  sdf <- callJMethod(read, "json", paths)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from an ORC file.
#'
#' Loads an ORC file, returning the result as a SparkDataFrame.
#'
#' @param path Path of file to read.
#' @return SparkDataFrame
#' @rdname read.orc
#' @export
#' @name read.orc
#' @note read.orc since 2.0.0
read.orc <- function(path) {
  sparkSession <- getSparkSession()
  # Allow the user to have a more flexible definiton of the ORC file path
  path <- suppressWarnings(normalizePath(path))
  read <- callJMethod(sparkSession, "read")
  sdf <- callJMethod(read, "orc", path)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from a Parquet file.
#'
#' Loads a Parquet file, returning the result as a SparkDataFrame.
#'
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return SparkDataFrame
#' @rdname read.parquet
#' @export
#' @name read.parquet
#' @note read.parquet since 1.6.0
read.parquet <- function(path) {
  sparkSession <- getSparkSession()
  # Allow the user to have a more flexible definiton of the Parquet file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  sdf <- callJMethod(read, "parquet", paths)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from a text file.
#'
#' Loads text files and returns a SparkDataFrame whose schema starts with
#' a string column named "value", and followed by partitioned columns if
#' there are any.
#'
#' Each line in the text file is a new row in the resulting SparkDataFrame.
#'
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return SparkDataFrame
#' @rdname read.text
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.txt"
#' df <- read.text(path)
#' }
#' @name read.text
#' @note read.text since 1.6.1
read.text <- function(path) {
  sparkSession <- getSparkSession()
  # Allow the user to have a more flexible definiton of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  sdf <- callJMethod(read, "text", paths)
  dataFrame(sdf)
}

#' SQL Query
#'
#' Executes a SQL query using Spark, returning the result as a SparkDataFrame.
#'
#' @param sqlQuery A character vector containing the SQL query
#' @return SparkDataFrame
#' @rdname sql
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' new_df <- sql("SELECT * FROM table")
#' }
#' @name sql
#' @note sql since 1.4.0
sql <- function(sqlQuery) {
  sparkSession <- getSparkSession()
  sdf <- callJMethod(sparkSession, "sql", sqlQuery)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from a SparkSQL Table
#'
#' Returns the specified Table as a SparkDataFrame.  The Table must have already been registered
#' in the SparkSession.
#'
#' @param tableName The SparkSQL Table to convert to a SparkDataFrame.
#' @return SparkDataFrame
#' @rdname tableToDF
#' @name tableToDF
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' new_df <- tableToDF("table")
#' }
#' @note tableToDF since 2.0.0
tableToDF <- function(tableName) {
  sparkSession <- getSparkSession()
  sdf <- callJMethod(sparkSession, "table", tableName)
  dataFrame(sdf)
}

#' Tables
#'
#' Returns a SparkDataFrame containing names of tables in the given database.
#'
#' @param databaseName name of the database
#' @return a SparkDataFrame
#' @rdname tables
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' tables("hive")
#' }
#' @name tables
#' @note tables since 1.4.0
tables <- function(databaseName = NULL) {
  sparkSession <- getSparkSession()
  jdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getTables", sparkSession, databaseName)
  dataFrame(jdf)
}

#' Table Names
#'
#' Returns the names of tables in the given database as an array.
#'
#' @param databaseName name of the database
#' @return a list of table names
#' @rdname tableNames
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' tableNames("hive")
#' }
#' @name tableNames
#' @note tableNames since 1.4.0
tableNames <- function(databaseName = NULL) {
  sparkSession <- getSparkSession()
  callJStatic("org.apache.spark.sql.api.r.SQLUtils",
              "getTableNames",
              sparkSession,
              databaseName)
}

#' Cache Table
#'
#' Caches the specified table in-memory.
#'
#' @param tableName The name of the table being cached
#' @return SparkDataFrame
#' @rdname cacheTable
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' cacheTable("table")
#' }
#' @name cacheTable
#' @note cacheTable since 1.4.0
cacheTable <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "cacheTable", tableName)
}

#' Uncache Table
#'
#' Removes the specified table from the in-memory cache.
#'
#' @param tableName The name of the table being uncached
#' @return SparkDataFrame
#' @rdname uncacheTable
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' uncacheTable("table")
#' }
#' @name uncacheTable
#' @note uncacheTable since 1.4.0
uncacheTable <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "uncacheTable", tableName)
}

#' Clear Cache
#'
#' Removes all cached tables from the in-memory cache.
#'
#' @rdname clearCache
#' @export
#' @examples
#' \dontrun{
#' clearCache()
#' }
#' @name clearCache
#' @note clearCache since 1.4.0
clearCache <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "clearCache")
}

#' (Deprecated) Drop Temporary Table
#'
#' Drops the temporary table with the given table name in the catalog.
#' If the table has been cached/persisted before, it's also unpersisted.
#'
#' @param tableName The name of the SparkSQL table to be dropped.
#' @seealso \link{dropTempView}
#' @rdname dropTempTable-deprecated
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempTable("table")
#' }
#' @name dropTempTable
#' @note dropTempTable since 1.4.0
dropTempTable <- function(tableName) {
  if (class(tableName) != "character") {
    stop("tableName must be a string.")
  }
  dropTempView(tableName)
}

#' Drops the temporary view with the given view name in the catalog.
#'
#' Drops the temporary view with the given view name in the catalog.
#' If the view has been cached before, then it will also be uncached.
#'
#' @param viewName the name of the view to be dropped.
#' @rdname dropTempView
#' @name dropTempView
#' @export
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempView("table")
#' }
#' @note since 2.0.0

dropTempView <- function(viewName) {
  sparkSession <- getSparkSession()
  if (class(viewName) != "character") {
    stop("viewName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "dropTempView", viewName)
}

#' Load a SparkDataFrame
#'
#' Returns the dataset in a data source as a SparkDataFrame
#'
#' The data source is specified by the `source` and a set of options(...).
#' If `source` is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used. \cr
#' Similar to R read.csv, when `source` is "csv", by default, a value of "NA" will be interpreted
#' as NA.
#'
#' @param path The path of files to load
#' @param source The name of external data source
#' @param schema The data schema defined in structType
#' @param na.strings Default string value for NA when source is "csv"
#' @return SparkDataFrame
#' @rdname read.df
#' @name read.df
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.df("path/to/file.json", source = "json")
#' schema <- structType(structField("name", "string"),
#'                      structField("info", "map<string,double>"))
#' df2 <- read.df(mapTypeJsonPath, "json", schema)
#' df3 <- loadDF("data/test_table", "parquet", mergeSchema = "true")
#' }
#' @name read.df
#' @note read.df since 1.4.0
read.df <- function(path = NULL, source = NULL, schema = NULL, na.strings = "NA", ...) {
  sparkSession <- getSparkSession()
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  if (source == "csv" && is.null(options[["nullValue"]])) {
    options[["nullValue"]] <- na.strings
  }
  if (!is.null(schema)) {
    stopifnot(class(schema) == "structType")
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "loadDF", sparkSession, source,
                       schema$jobj, options)
  } else {
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                       "loadDF", sparkSession, source, options)
  }
  dataFrame(sdf)
}

#' @rdname read.df
#' @name loadDF
#' @note loadDF since 1.6.0
loadDF <- function(path = NULL, source = NULL, schema = NULL, ...) {
  read.df(path, source, schema, ...)
}

#' Create an external table
#'
#' Creates an external table based on the dataset in a data source,
#' Returns a SparkDataFrame associated with the external table.
#'
#' The data source is specified by the `source` and a set of options(...).
#' If `source` is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param tableName A name of the table
#' @param path The path of files to load
#' @param source the name of external data source
#' @return SparkDataFrame
#' @rdname createExternalTable
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createExternalTable("myjson", path="path/to/json", source="json")
#' }
#' @name createExternalTable
#' @note createExternalTable since 1.4.0
createExternalTable <- function(tableName, path = NULL, source = NULL, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  catalog <- callJMethod(sparkSession, "catalog")
  sdf <- callJMethod(catalog, "createExternalTable", tableName, source, options)
  dataFrame(sdf)
}

#' Create a SparkDataFrame representing the database table accessible via JDBC URL
#'
#' Additional JDBC database connection properties can be set (...)
#'
#' Only one of partitionColumn or predicates should be set. Partitions of the table will be
#' retrieved in parallel based on the `numPartitions` or by the predicates.
#'
#' Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
#' your external database systems.
#'
#' @param url JDBC database url of the form `jdbc:subprotocol:subname`
#' @param tableName the name of the table in the external database
#' @param partitionColumn the name of a column of integral type that will be used for partitioning
#' @param lowerBound the minimum value of `partitionColumn` used to decide partition stride
#' @param upperBound the maximum value of `partitionColumn` used to decide partition stride
#' @param numPartitions the number of partitions, This, along with `lowerBound` (inclusive),
#'                      `upperBound` (exclusive), form partition strides for generated WHERE
#'                      clause expressions used to split the column `partitionColumn` evenly.
#'                      This defaults to SparkContext.defaultParallelism when unset.
#' @param predicates a list of conditions in the where clause; each one defines one partition
#' @return SparkDataFrame
#' @rdname read.jdbc
#' @name read.jdbc
#' @export
#' @examples
#'\dontrun{
#' sparkR.session()
#' jdbcUrl <- "jdbc:mysql://localhost:3306/databasename"
#' df <- read.jdbc(jdbcUrl, "table", predicates = list("field<=123"), user = "username")
#' df2 <- read.jdbc(jdbcUrl, "table2", partitionColumn = "index", lowerBound = 0,
#'                  upperBound = 10000, user = "username", password = "password")
#' }
#' @note read.jdbc since 2.0.0
read.jdbc <- function(url, tableName,
                      partitionColumn = NULL, lowerBound = NULL, upperBound = NULL,
                      numPartitions = 0L, predicates = list(), ...) {
  jprops <- varargsToJProperties(...)
  sparkSession <- getSparkSession()
  read <- callJMethod(sparkSession, "read")
  if (!is.null(partitionColumn)) {
    if (is.null(numPartitions) || numPartitions == 0) {
      sc <- callJMethod(sparkSession, "sparkContext")
      numPartitions <- callJMethod(sc, "defaultParallelism")
    } else {
      numPartitions <- numToInt(numPartitions)
    }
    sdf <- callJMethod(read, "jdbc", url, tableName, as.character(partitionColumn),
                       numToInt(lowerBound), numToInt(upperBound), numPartitions, jprops)
  } else if (length(predicates) > 0) {
    sdf <- callJMethod(read, "jdbc", url, tableName, as.list(as.character(predicates)), jprops)
  } else {
    sdf <- callJMethod(read, "jdbc", url, tableName, jprops)
  }
  dataFrame(sdf)
}

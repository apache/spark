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
         stop("Unsupported type for SparkDataFrame: ", class(x)))
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
#' To change SparkSession Runtime Config, please see \code{sparkR.session()}.
#'
#' @param key (optional) The key of the config to get, if omitted, all config is returned
#' @param defaultValue (optional) The default value of the config to return if they config is not
#' set, if omitted, the call fails if the config key is not set
#' @return a list of config values with keys as their names
#' @rdname sparkR.conf
#' @name sparkR.conf
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
                estr <- as.character(e)
                if (any(grepl("java.util.NoSuchElementException", estr, fixed = TRUE))) {
                  stop("Config '", key, "' is not set")
                } else {
                  stop("Unknown error: ", estr)
                }
              })
    } else {
      callJMethod(conf, "get", key, defaultValue)
    }
    l <- setNames(list(value), key)
    l
  }
}

#' Get version of Spark on which this application is running
#'
#' Get version of Spark on which this application is running.
#'
#' @return a character string of the Spark version
#' @rdname sparkR.version
#' @name sparkR.version
#' @examples
#'\dontrun{
#' sparkR.session()
#' version <- sparkR.version()
#' }
#' @note sparkR.version since 2.0.1
sparkR.version <- function() {
  sparkSession <- getSparkSession()
  callJMethod(sparkSession, "version")
}

getDefaultSqlSource <- function() {
  l <- sparkR.conf("spark.sql.sources.default", "org.apache.spark.sql.parquet")
  l[["spark.sql.sources.default"]]
}

writeToFileInArrow <- function(fileName, rdf, numPartitions) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    numPartitions <- if (!is.null(numPartitions)) {
      numToInt(numPartitions)
    } else {
      1
    }

    rdf_slices <- if (numPartitions > 1) {
      split(rdf, makeSplits(numPartitions, nrow(rdf)))
    } else {
      list(rdf)
    }

    stream_writer <- NULL
    tryCatch({
      for (rdf_slice in rdf_slices) {
        batch <- arrow::record_batch(rdf_slice)
        if (is.null(stream_writer)) {
          stream <- arrow::FileOutputStream$create(fileName)
          schema <- batch$schema
          stream_writer <- arrow::RecordBatchStreamWriter$create(stream, schema)
        }

        stream_writer$write_batch(batch)
      }
    },
    finally = {
      if (!is.null(stream_writer)) {
        stream_writer$close()
      }
    })

  } else {
    stop("'arrow' package should be installed.")
  }
}

getSchema <- function(schema, firstRow = NULL, rdd = NULL) {
  if (is.null(schema) || (!inherits(schema, "structType") && is.null(names(schema)))) {
    if (is.null(firstRow)) {
      stopifnot(!is.null(rdd))
      firstRow <- firstRDD(rdd)
    }
    names <- if (is.null(schema)) {
      names(firstRow)
    } else {
      as.list(schema)
    }
    if (is.null(names)) {
      names <- lapply(seq_len(length(firstRow)), function(x) {
        paste0("_", as.character(x))
      })
    }

    # SPARK-SQL does not support '.' in column name, so replace it with '_'
    # TODO(davies): remove this once SPARK-2775 is fixed
    names <- lapply(names, function(n) {
      nn <- gsub(".", "_", n, fixed = TRUE)
      if (nn != n) {
        warning("Use ", nn, " instead of ", n, " as column name")
      }
      nn
    })

    types <- lapply(firstRow, infer_type)
    fields <- lapply(seq_len(length(firstRow)), function(i) {
      structField(names[[i]], types[[i]], TRUE)
    })
    schema <- do.call(structType, fields)
  } else {
    schema
  }
}

#' Create a SparkDataFrame
#'
#' Converts R data.frame or list into SparkDataFrame.
#'
#' @param data a list or data.frame.
#' @param schema a list of column names or named list (StructType), optional.
#' @param samplingRatio Currently not used.
#' @param numPartitions the number of partitions of the SparkDataFrame. Defaults to 1, this is
#'        limited by length of the list or number of rows of the data.frame
#' @return A SparkDataFrame.
#' @rdname createDataFrame
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- as.DataFrame(iris)
#' df2 <- as.DataFrame(list(3,4,5,6))
#' df3 <- createDataFrame(iris)
#' df4 <- createDataFrame(cars, numPartitions = 2)
#' }
#' @name createDataFrame
#' @note createDataFrame since 1.4.0
# TODO(davies): support sampling and infer type from NA
createDataFrame <- function(data, schema = NULL, samplingRatio = 1.0,
                            numPartitions = NULL) {
  sparkSession <- getSparkSession()
  arrowEnabled <- sparkR.conf("spark.sql.execution.arrow.sparkr.enabled")[[1]] == "true"
  useArrow <- FALSE
  firstRow <- NULL

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
    data[] <- lapply(data, cleanCols)

    args <- list(FUN = list, SIMPLIFY = FALSE, USE.NAMES = FALSE)
    if (arrowEnabled) {
      useArrow <- tryCatch({
        stopifnot(length(data) > 0)
        firstRow <- do.call(mapply, append(args, head(data, 1)))[[1]]
        schema <- getSchema(schema, firstRow = firstRow)
        checkSchemaInArrow(schema)
        fileName <- tempfile(pattern = "sparwriteToFileInArrowk-arrow", fileext = ".tmp")
        tryCatch({
          writeToFileInArrow(fileName, data, numPartitions)
          jrddInArrow <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                                     "readArrowStreamFromFile",
                                     sparkSession,
                                     fileName)
        },
        finally = {
          # File might not be created.
          suppressWarnings(file.remove(fileName))
        })
        TRUE
      },
      error = function(e) {
        warning("createDataFrame attempted Arrow optimization because ",
                "'spark.sql.execution.arrow.sparkr.enabled' is set to true; however, ",
                "failed, attempting non-optimization. Reason: ", e)
        FALSE
      })
    }

    if (!useArrow) {
      # Convert data into a list of rows. Each row is a list.
      # drop factors and wrap lists
      data <- setNames(as.list(data), NULL)

      # check if all columns have supported type
      lapply(data, getInternalType)

      # convert to rows
      data <- do.call(mapply, append(args, data))
      if (length(data) > 0) {
        firstRow <- data[[1]]
      }
    }
  }

  if (useArrow) {
    rdd <- jrddInArrow
  } else if (is.list(data)) {
    sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)
    if (!is.null(numPartitions)) {
      rdd <- parallelize(sc, data, numSlices = numToInt(numPartitions))
    } else {
      rdd <- parallelize(sc, data, numSlices = 1)
    }
  } else if (inherits(data, "RDD")) {
    rdd <- data
  } else {
    stop("unexpected type: ", class(data))
  }

  schema <- getSchema(schema, firstRow, rdd)

  stopifnot(class(schema) == "structType")

  if (useArrow) {
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                       "toDataFrame", rdd, schema$jobj, sparkSession)
  } else {
    jrdd <- getJRDD(lapply(rdd, function(x) x), "row")
    srdd <- callJMethod(jrdd, "rdd")
    sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "createDF",
                       srdd, schema$jobj, sparkSession)
  }
  dataFrame(sdf)
}

#' @rdname createDataFrame
#' @aliases createDataFrame
#' @note as.DataFrame since 1.6.0
as.DataFrame <- function(data, schema = NULL, samplingRatio = 1.0, numPartitions = NULL) {
  createDataFrame(data, schema, samplingRatio, numPartitions)
}

#' toDF
#'
#' Converts an RDD to a SparkDataFrame by infer the types.
#'
#' @param x An RDD
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
#' Loads a JSON file, returning the result as a SparkDataFrame
#' By default, (\href{https://jsonlines.org/}{JSON Lines text format or newline-delimited JSON}
#' ) is supported. For JSON (one record per file), set a named property \code{multiLine} to
#' \code{TRUE}.
#' It goes through the entire dataset once to determine the schema.
#'
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @param ... additional external data source specific named properties.
#'            You can find the JSON-specific options for reading JSON files in
# nolint start
#'            \url{https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option}{Data Source Option} in the version you use.
# nolint end
#' @return SparkDataFrame
#' @rdname read.json
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' df <- read.json(path, multiLine = TRUE)
#' }
#' @name read.json
#' @note read.json since 1.6.0
read.json <- function(path, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  # Allow the user to have a more flexible definition of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "json", paths)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from an ORC file.
#'
#' Loads an ORC file, returning the result as a SparkDataFrame.
#'
#' @param path Path of file to read.
#' @param ... additional external data source specific named properties.
#'            You can find the ORC-specific options for reading ORC files in
# nolint start
#'            \url{https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option}{Data Source Option} in the version you use.
# nolint end
#' @return SparkDataFrame
#' @rdname read.orc
#' @name read.orc
#' @note read.orc since 2.0.0
read.orc <- function(path, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  # Allow the user to have a more flexible definition of the ORC file path
  path <- suppressWarnings(normalizePath(path))
  read <- callJMethod(sparkSession, "read")
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "orc", path)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from a Parquet file.
#'
#' Loads a Parquet file, returning the result as a SparkDataFrame.
#'
#' @param path path of file to read. A vector of multiple paths is allowed.
#' @param ... additional data source specific named properties.
#'            You can find the Parquet-specific options for reading Parquet files in
# nolint start
#'            \url{https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option}{Data Source Option} in the version you use.
# nolint end
#' @return SparkDataFrame
#' @rdname read.parquet
#' @name read.parquet
#' @note read.parquet since 1.6.0
read.parquet <- function(path, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  # Allow the user to have a more flexible definition of the Parquet file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "parquet", paths)
  dataFrame(sdf)
}

#' Create a SparkDataFrame from a text file.
#'
#' Loads text files and returns a SparkDataFrame whose schema starts with
#' a string column named "value", and followed by partitioned columns if
#' there are any. The text files must be encoded as UTF-8.
#'
#' Each line in the text file is a new row in the resulting SparkDataFrame.
#'
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @param ... additional external data source specific named properties.
#'            You can find the text-specific options for reading text files in
# nolint start
#'            \url{https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option}{Data Source Option} in the version you use.
# nolint end
#' @return SparkDataFrame
#' @rdname read.text
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.txt"
#' df <- read.text(path)
#' }
#' @name read.text
#' @note read.text since 1.6.1
read.text <- function(path, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  # Allow the user to have a more flexible definition of the text file path
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- callJMethod(sparkSession, "read")
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "text", paths)
  dataFrame(sdf)
}

#' SQL Query
#'
#' Executes a SQL query using Spark, returning the result as a SparkDataFrame.
#'
#' @param sqlQuery A character vector containing the SQL query
#' @return SparkDataFrame
#' @rdname sql
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

#' Create a SparkDataFrame from a SparkSQL table or view
#'
#' Returns the specified table or view as a SparkDataFrame. The table or view must already exist or
#' have already been registered in the SparkSession.
#'
#' @param tableName the qualified or unqualified name that designates a table or view. If a database
#'                  is specified, it identifies the table/view from the database.
#'                  Otherwise, it first attempts to find a temporary view with the given name
#'                  and then match the table/view from the current database.
#' @return SparkDataFrame
#' @rdname tableToDF
#' @name tableToDF
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

#' Load a SparkDataFrame
#'
#' Returns the dataset in a data source as a SparkDataFrame
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used. \cr
#' Similar to R read.csv, when \code{source} is "csv", by default, a value of "NA" will be
#' interpreted as NA.
#'
#' @param path The path of files to load
#' @param source The name of external data source
#' @param schema The data schema defined in structType or a DDL-formatted string.
#' @param na.strings Default string value for NA when source is "csv"
#' @param ... additional external data source specific named properties.
#' @return SparkDataFrame
#' @rdname read.df
#' @name read.df
#' @seealso \link{read.json}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df1 <- read.df("path/to/file.json", source = "json")
#' schema <- structType(structField("name", "string"),
#'                      structField("info", "map<string,double>"))
#' df2 <- read.df(mapTypeJsonPath, "json", schema, multiLine = TRUE)
#' df3 <- loadDF("data/test_table", "parquet", mergeSchema = "true")
#' stringSchema <- "name STRING, info MAP<STRING, DOUBLE>"
#' df4 <- read.df(mapTypeJsonPath, "json", stringSchema, multiLine = TRUE)
#' }
#' @note read.df since 1.4.0
read.df <- function(path = NULL, source = NULL, schema = NULL, na.strings = "NA", ...) {
  if (!is.null(path) && !is.character(path)) {
    stop("path should be character, NULL or omitted.")
  }
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the datasource specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  if (source == "csv" && is.null(options[["nullValue"]])) {
    options[["nullValue"]] <- na.strings
  }
  read <- callJMethod(sparkSession, "read")
  read <- callJMethod(read, "format", source)
  if (!is.null(schema)) {
    if (class(schema) == "structType") {
      read <- callJMethod(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- callJMethod(read, "schema", schema)
    } else {
      stop("schema should be structType or character.")
    }
  }
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "load")
  dataFrame(sdf)
}

#' @rdname read.df
#' @name loadDF
#' @note loadDF since 1.6.0
loadDF <- function(path = NULL, source = NULL, schema = NULL, ...) {
  read.df(path, source, schema, ...)
}

#' Create a SparkDataFrame representing the database table accessible via JDBC URL
#'
#' Additional JDBC database connection properties can be set (...)
#' You can find the JDBC-specific option and parameter documentation for reading tables via JDBC in
# nolint start
#' \url{https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option}{Data Source Option} in the version you use.
# nolint end
#'
#' Only one of partitionColumn or predicates should be set. Partitions of the table will be
#' retrieved in parallel based on the \code{numPartitions} or by the predicates.
#'
#' Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
#' your external database systems.
#'
#' @param url JDBC database url of the form \code{jdbc:subprotocol:subname}
#' @param tableName the name of the table in the external database
#' @param partitionColumn the name of a column of numeric, date, or timestamp type
#'                        that will be used for partitioning.
#' @param lowerBound the minimum value of \code{partitionColumn} used to decide partition stride
#' @param upperBound the maximum value of \code{partitionColumn} used to decide partition stride
#' @param numPartitions the number of partitions, This, along with \code{lowerBound} (inclusive),
#'                      \code{upperBound} (exclusive), form partition strides for generated WHERE
#'                      clause expressions used to split the column \code{partitionColumn} evenly.
#'                      This defaults to SparkContext.defaultParallelism when unset.
#' @param predicates a list of conditions in the where clause; each one defines one partition
#' @param ... additional JDBC database connection named properties.
#' @return SparkDataFrame
#' @rdname read.jdbc
#' @name read.jdbc
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
    sdf <- handledCallJMethod(read, "jdbc", url, tableName, as.character(partitionColumn),
                              numToInt(lowerBound), numToInt(upperBound), numPartitions, jprops)
  } else if (length(predicates) > 0) {
    sdf <- handledCallJMethod(read, "jdbc", url, tableName, as.list(as.character(predicates)),
                              jprops)
  } else {
    sdf <- handledCallJMethod(read, "jdbc", url, tableName, jprops)
  }
  dataFrame(sdf)
}

#' Load a streaming SparkDataFrame
#'
#' Returns the dataset in a data source as a SparkDataFrame
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param source The name of external data source
#' @param schema The data schema defined in structType or a DDL-formatted string, this is
#'               required for file-based streaming data source
#' @param ... additional external data source specific named options, for instance \code{path} for
#'        file-based streaming data source. \code{timeZone} to indicate a timezone to be used to
#'        parse timestamps in the JSON/CSV data sources or partition values; If it isn't set, it
#'        uses the default value, session local timezone.
#' @return SparkDataFrame
#' @rdname read.stream
#' @name read.stream
#' @seealso \link{write.stream}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- read.stream("socket", host = "localhost", port = 9999)
#' q <- write.stream(df, "text", path = "/home/user/out", checkpointLocation = "/home/user/cp")
#'
#' df <- read.stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = 1)
#' stringSchema <- "name STRING, info MAP<STRING, DOUBLE>"
#' df1 <- read.stream("json", path = jsonDir, schema = stringSchema, maxFilesPerTrigger = 1)
#' }
#' @note read.stream since 2.2.0
#' @note experimental
read.stream <- function(source = NULL, schema = NULL, ...) {
  sparkSession <- getSparkSession()
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the data source specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  options <- varargsToStrEnv(...)
  read <- callJMethod(sparkSession, "readStream")
  read <- callJMethod(read, "format", source)
  if (!is.null(schema)) {
    if (class(schema) == "structType") {
      read <- callJMethod(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- callJMethod(read, "schema", schema)
    } else {
      stop("schema should be structType or character.")
    }
  }
  read <- callJMethod(read, "options", options)
  sdf <- handledCallJMethod(read, "load")
  dataFrame(sdf)
}

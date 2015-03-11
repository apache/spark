# SQLcontext.R: SQLContext-driven functions

infer_type <- function(x) {
  if (is.null(x)) {
    stop("can not infer type from NULL")
  }
  type <- switch(class(x),
                 integer = "long",
                 character = "string",
                 logical = "boolean",
                 double = "double",
                 numeric = "double",
                 raw = "binary",
                 list = "array",
                 environment = "map",
                 stop("Unsupported type for DataFrame"))

  if (type == "map") {
    stopifnot(length(x) > 0)
    key <- ls(x)[[1]]
    list(type = "map",
         keyType = "string",
         valueType = infer_type(get(key, x)),
         valueContainsNull = TRUE)
  } else if (type == "array") {
    stopifnot(length(x) > 0)
    names <- names(x)
    if (is.null(names)) {
      list(type = "array", elementType = infer_type(x[[1]]), containsNull = TRUE)
    } else {
      # StructType
      types <- lapply(x, infer_type)
      fields <- lapply(1:length(x), function(i) {
        list(name = names[[i]], type = types[[i]], nullable = TRUE)
      })
      list(type = "struct", fields = fields)
    }
  } else if (length(x) > 1) {
    list(type = "array", elementType = type, containsNull = TRUE)
  } else {
    type
  }
}


#' Create a DataFrame from an RDD
#'
#' Converts an RDD to a DataFrame by infer the types.
#'
#' @param sqlCtx A SQLContext
#' @param x An RDD
#' @param schema a list of column names or named list (StructType), optional
#' @return an DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' rdd <- lapply(parallelize(sc, 1:10), function(x) list(a=x, b=as.character(x)))
#' df <- createDataFrame(sqlCtx, rdd)
#' }

# TODO(davies): support sampling and infer type from NA
createDataFrame <- function(sqlCtx, rdd, schema = NULL, samplingRatio = 1.0) {
  stopifnot(inherits(rdd, "RDD"))
  if (is.null(schema) || is.null(names(schema))) {
    row <- first(rdd)
    names <- if (is.null(schema)) {
      names(row)
    } else {
      schema
    }
    if (is.null(names)) {
      names <- lapply(1:length(row), function(x) {
       paste("_", as.character(x), sep="")
      })
    }

    types <- lapply(row, infer_type)
    fields <- lapply(1:length(row), function(i) {
      list(name = names[[i]], type = types[[i]], nullable = TRUE)
    })
    schema <- list(type = "struct", fields = fields)
  }

  stopifnot(class(schema) == "list")
  stopifnot(schema$type == "struct")
  stopifnot(class(schema$fields) == "list")
  schemaString <- as.character(jsonlite::toJSON(schema, auto_unbox = TRUE))

  jrdd <- getJRDD(lapply(rdd, function(x) x), "row")
  srdd <- callJMethod(jrdd, "rdd")
  sdf <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "createDF",
                     srdd, schemaString, sqlCtx)
  dataFrame(sdf)
}

#' toDF()
#'
#' Converts an RDD to a DataFrame by infer the types.
#'
#' @param x An RDD
#'
#' @rdname DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' rdd <- lapply(parallelize(sc, 1:10), function(x) list(a=x, b=as.character(x)))
#' df <- toDF(rdd)
#' }

setGeneric("toDF", function(x, ...) { standardGeneric("toDF") })

setMethod("toDF", signature(x = "RDD"),
          function(x, ...) {
            sqlCtx <- if (exists(".sparkRHivesc", envir = .sparkREnv)) {
              get(".sparkRHivesc", envir = .sparkREnv)
            } else if (exists(".sparkRSQLsc", envir = .sparkREnv)) {
              get(".sparkRSQLsc", envir = .sparkREnv)
            } else {
              stop("no SQL context available")
            }
            createDataFrame(sqlCtx, x, ...)
          })

#' Create a DataFrame from a JSON file.
#'
#' Loads a JSON file (one object per line), returning the result as a DataFrame 
#' It goes through the entire dataset once to determine the schema.
#'
#' @param sqlCtx SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' }

jsonFile <- function(sqlCtx, path) {
  # Allow the user to have a more flexible definiton of the text file path
  path <- normalizePath(path)
  # Convert a string vector of paths to a string containing comma separated paths
  path <- paste(path, collapse = ",")
  sdf <- callJMethod(sqlCtx, "jsonFile", path)
  dataFrame(sdf)
}


#' JSON RDD
#'
#' Loads an RDD storing one JSON object per string as a DataFrame.
#'
#' @param sqlCtx SQLContext to use
#' @param rdd An RDD of JSON string
#' @param schema A StructType object to use as schema
#' @param samplingRatio The ratio of simpling used to infer the schema
#' @return A DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' rdd <- texFile(sc, "path/to/json")
#' df <- jsonRDD(sqlCtx, rdd)
#' }

# TODO: support schema
jsonRDD <- function(sqlCtx, rdd, schema = NULL, samplingRatio = 1.0) {
  rdd <- serializeToString(rdd)
  if (is.null(schema)) {
    sdf <- callJMethod(sqlCtx, "jsonRDD", callJMethod(getJRDD(rdd), "rdd"), samplingRatio)
    dataFrame(sdf)
  } else {
    stop("not implemented")
  }
}


#' Create a DataFrame from a Parquet file.
#' 
#' Loads a Parquet file, returning the result as a DataFrame.
#'
#' @param sqlCtx SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @export

# TODO: Implement saveasParquetFile and write examples for both
parquetFile <- function(sqlCtx, path) {
  # Allow the user to have a more flexible definiton of the text file path
  path <- normalizePath(path)
  # Convert a string vector of paths to a string containing comma separated paths
  path <- paste(path, collapse = ",")
  sdf <- callJMethod(sqlCtx, "parquetFile", path)
  dataFrame(sdf)
}

#' SQL Query
#' 
#' Executes a SQL query using Spark, returning the result as a DataFrame.
#'
#' @param sqlCtx SQLContext to use
#' @param sqlQuery A character vector containing the SQL query
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' registerTempTable(df, "table")
#' new_df <- sql(sqlCtx, "SELECT * FROM table")
#' }

sql <- function(sqlCtx, sqlQuery) {
  sdf <- callJMethod(sqlCtx, "sql", sqlQuery)
  dataFrame(sdf)
}


#' Create a DataFrame from a SparkSQL Table
#' 
#' Returns the specified Table as a DataFrame.  The Table must have already been registered
#' in the SQLContext.
#'
#' @param sqlCtx SQLContext to use
#' @param tableName The SparkSQL Table to convert to a DataFrame.
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' registerTempTable(df, "table")
#' new_df <- table(sqlCtx, "table")
#' }

table <- function(sqlCtx, tableName) {
  sdf <- callJMethod(sqlCtx, "table", tableName)
  dataFrame(sdf) 
}


#' Tables
#'
#' Returns a DataFrame containing names of tables in the given database.
#'
#' @param sqlCtx SQLContext to use
#' @param databaseName name of the database
#' @return a DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' tables(sqlCtx, "hive")
#' }

tables <- function(sqlCtx, databaseName = NULL) {
  jdf <- if (is.null(databaseName)) {
    callJMethod(sqlCtx, "tables")
  } else {
    callJMethod(sqlCtx, "tables", databaseName)
  }
  dataFrame(jdf)
}


#' Table Names
#'
#' Returns the names of tables in the given database as an array.
#'
#' @param sqlCtx SQLContext to use
#' @param databaseName name of the database
#' @return a list of table names
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' tableNames(sqlCtx, "hive")
#' }

tableNames <- function(sqlCtx, databaseName = NULL) {
  if (is.null(databaseName)) {
    callJMethod(sqlCtx, "tableNames")
  } else {
    callJMethod(sqlCtx, "tableNames", databaseName)
  }
}


#' Cache Table
#' 
#' Caches the specified table in-memory.
#'
#' @param sqlCtx SQLContext to use
#' @param tableName The name of the table being cached
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' registerTempTable(df, "table")
#' cacheTable(sqlCtx, "table")
#' }

cacheTable <- function(sqlCtx, tableName) {
  callJMethod(sqlCtx, "cacheTable", tableName)  
}

#' Uncache Table
#' 
#' Removes the specified table from the in-memory cache.
#'
#' @param sqlCtx SQLContext to use
#' @param tableName The name of the table being uncached
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' registerTempTable(df, "table")
#' uncacheTable(sqlCtx, "table")
#' }

uncacheTable <- function(sqlCtx, tableName) {
  callJMethod(sqlCtx, "uncacheTable", tableName)
}

#' Clear Cache
#'
#' Removes all cached tables from the in-memory cache.
#'
#' @param sqlCtx SQLContext to use
#' clearCache(sqlCtx)
#' }

clearCache <- function(sqlCtx) {
  callJMethod(sqlCtx, "clearCache")
}

#' Drop Temporary Table
#'
#' Drops the temporary table with the given table name in the catalog.
#' If the table has been cached/persisted before, it's also unpersisted.
#'
#' @param sqlCtx SQLContext to use
#' @param tableName The name of the SparkSQL table to be dropped.
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' df <- loadDF(sqlCtx, path, "parquet")
#' registerTempTable(df, "table")
#' dropTempTable(sqlCtx, "table")
#' }

dropTempTable <- function(sqlCtx, tableName) {
  if (class(tableName) != "character") {
    stop("tableName must be a string.")
  }
  callJMethod(sqlCtx, "dropTempTable", tableName)
}

#' Load an DataFrame
#'
#' Returns the dataset in a data source as a DataFrame
#'
#' The data source is specified by the `source` and a set of options(...).
#' If `source` is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param sqlCtx SQLContext to use
#' @param path The path of files to load
#' @param source the name of external data source
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' df <- load(sqlCtx, "path/to/file.json", source = "json")
#' }

loadDF <- function(sqlCtx, path = NULL, source = NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[['path']] <- path
  }
  sdf <- callJMethod(sqlCtx, "load", source, options)
  dataFrame(sdf)
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
#' @param sqlCtx SQLContext to use
#' @param tableName A name of the table
#' @param path The path of files to load
#' @param source the name of external data source
#' @return DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' df <- sparkRSQL.createExternalTable(sqlCtx, "myjson", path="path/to/json", source="json")
#' }

createExternalTable <- function(sqlCtx, tableName, path = NULL, source = NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[['path']] <- path
  }
  sdf <- callJMethod(sqlCtx, "createExternalTable", tableName, source, options)
  dataFrame(sdf)
}

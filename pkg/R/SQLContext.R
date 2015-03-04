# SQLcontext.R: SQLContext-driven functions

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
  path <- paste(path, collapse=",")
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
  path <- paste(path, collapse=",")
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

tables <- function(sqlCtx, databaseName=NULL) {
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

tableNames <- function(sqlCtx, databaseName=NULL) {
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
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' clearCache(sqlCtx)
#' }

clearCache <- function(sqlCtx) {
  callJMethod(sqlCtx, "clearCache")
}

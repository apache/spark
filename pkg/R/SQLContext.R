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
#' df <- table(sqlCtx, "table")
#' uncacheTable(sqlCtx, "table")
#' }

uncacheTable <- function(sqlCtx, tableName) {
  callJMethod(sqlCtx, "uncacheTable", tableName)
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
#' df <- load(sqlCtx, "path/to/file.json", source="json")
#' }

loadDF <- function(sqlCtx, path=NULL, source=NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[['path']] = path
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

createExternalTable <- function(sqlCtx, tableName, path=NULL, source=NULL, ...) {
  options <- varargsToEnv(...)
  if (!is.null(path)) {
    options[['path']] = path
  }
  sdf <- callJMethod(sqlCtx, "createExternalTable", tableName, source, options)
  dataFrame(sdf)
}

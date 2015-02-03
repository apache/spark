# SQLcontext.R: SQLContext-driven functions

#' Create a DataFrame from a JSON file.
#'
#' Loads a JSON file (one object per line), returning the result as a DataFrame 
#' It goes through the entire dataset once to determine the schema.
#'
#' @param sqlctx SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @export

jsonFile <- function(sqlctx, path) {
  # Allow the user to have a more flexible definiton of the text file path
  path <- normalizePath(path)
  # Convert a string vector of paths to a string containing comma separated paths
  path <- paste(path, collapse=",")
  
  sdf<- callJMethod(sqlctx, "jsonFile", path)
  
  dataFrame(sdf)
}

#' Create a DataFrame from a Parquet file.
#' 
#' Loads a Parquet file, returning the result as a DataFrame.
#'
#' @param sqlctx SQLContext to use
#' @param path Path of file to read. A vector of multiple paths is allowed.
#' @return DataFrame
#' @export

parquetFile <- function(sqlctx, path) {
  # Allow the user to have a more flexible definiton of the text file path
  path <- normalizePath(path)
  # Convert a string vector of paths to a string containing comma separated paths
  path <- paste(path, collapse=",")
  
  sdf <- callJMethod(sqlctx, "parquetFile", path)
  
  dataFrame(sdf)
}

#' SQL Query
#' 
#' Executes a SQL query using Spark, returning the result as a DataFrame.
#'
#' @param sqlctx SQLContext to use
#' @param sqlQuery A character vector containing the SQL query
#' @return DataFrame
#' @export

sql <- function(sqlctx, sqlQuery) {
  
  sdf <- callJMethod(sqlctx, "sql", sqlQuery)
  
  dataFrame(sdf)
}

#' Create a DataFrame from a SparkSQL Table
#' 
#' Returns the specified Table as a DataFrame.  The Table must have already been registered
#' in the SQLContext.
#'
#' @param sqlctx SQLContext to use
#' @param sqlQuery A character vector containing the SQL query
#' @return DataFrame
#' @export

table <- function(sqlctx, tableName) {
  
  sdf <- callJMethod(sqlctx, "table", tableName)
  
  dataFrame(sdf) 
}

#' Cache Table
#' 
#' Caches the specified table in-memory.
#'
#' @param sqlctx SQLContext to use
#' @param tableName The name of the table being cached
#' @return DataFrame
#' @export

cacheTable <- function(sqlctx, tableName) {
  
  callJMethod(sqlctx, "cacheTable", tableName)
  
}

#' Uncache Table
#' 
#' Removes the specified table from the in-memory cache.
#'
#' @param sqlctx SQLContext to use
#' @param tableName The name of the table being uncached
#' @return DataFrame
#' @export

uncacheTable <- function(sqlctx, tableName){
  
  callJMethod(sqlctx, "uncacheTable", tableName)
  
}

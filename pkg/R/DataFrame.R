# DataFrame.R - DataFrame class and methods implemented in S4 OO classes

#' @include jobj.R RDD.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a DataFrame
#' @description DataFrames can be created using functions like
#'              \code{jsonFile}, \code{table} etc.
#' @rdname DataFrame
#' @seealso jsonFile, table
#'
#' @param env An R environment that stores bookkeeping states of the DataFrame
#' @param sdf A Java object reference to the backing Scala SchemaRDD
#' @export

setClass("DataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "DataFrame", function(.Object, sdf) {
  .Object@env <- new.env()
  
  .Object@sdf <- sdf
  .Object
})

#' @rdname DataFrame
#' @export

dataFrame <- function(sdf) {
  new("DataFrame", sdf)
}

############################ DataFrame Methods ##############################################

#' Print Schema of a DataFrame
#' 
#' Prints out the schema in tree format
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname printSchema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' printSchema(df)
#'}

setGeneric("printSchema", function(df) { standardGeneric("printSchema") })

setMethod("printSchema",
          signature(df = "DataFrame"),
          function(df) {
            sdf <- df@sdf
            schemaString <- callJMethod(sdf, "printSchema")
            cat(schemaString)
          })

#' Register Temporary Table
#' 
#' Registers a DataFrame as a Temporary Table in the SQLContext
#' 
#' @param df A SparkSQL DataFrame
#' @param tableName A character vector containing the name of the table
#' 
#' @rdname registerTempTable
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' registerTempTable(df, "json_df")
#' new_df <- sql(sqlCtx, "SELECT * FROM json_df")
#'}

setGeneric("registerTempTable", function(df, tableName) { standardGeneric("registerTempTable") })

setMethod("registerTempTable",
          signature(df = "DataFrame", tableName = "character"),
          function(df, tableName) {
              sdf <- df@sdf
              callJMethod(sdf, "registerTempTable", tableName)
          })

#' Count
#' 
#' Returns the number of rows in a DataFrame
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname count
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' count(df)
#' }

setMethod("count",
          signature(x = "DataFrame"),
          function(x) {
            sdf <- x@sdf
            callJMethod(sdf, "count")
          })

#' Collect elements of a DataFrame
#' 
#' Returns a list of Row objects from a DataFrame
#' 
#' @param df A SparkSQL DataFrame
#' 
#' @rdname collect-methods
#' @export

# TODO: Collect() currently returns a list of Generic Row objects and is WIP.  This will eventually 
# be part of the process to read a DataFrame into R and create a data.frame.
setMethod("collect",
          signature(rdd = "DataFrame"),
          function(rdd){
            sdf <- rdd@sdf
            listObj <- callJMethod(sdf, "collect")
          })

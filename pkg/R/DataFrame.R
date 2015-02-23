# DataFrame.R - DataFrame class and methods implemented in S4 OO classes

#' @include jobj.R RDD.R pairRDD.R
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

# Collects all the elements of a Spark DataFrame and coerces them into an R data.frame.

#' @rdname collect-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' collected <- collect(df)
#' firstName <- collected[[1]]$name
#' }

setMethod("collect",
          signature(rdd = "DataFrame"),
          function(rdd) {
            # listCols is a list of raw vectors, one per column
            listCols <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToCols", rdd@sdf)
            cols <- lapply(listCols, function(col) {
              objRaw <- rawConnection(col)
              numRows <- readInt(objRaw)
              col <- readCol(objRaw, numRows)
              close(objRaw)
              col
            })
            colNames <- callJMethod(rdd@sdf, "columns")
            names(cols) <- colNames
            dfOut <- do.call(cbind.data.frame, cols)
            dfOut
          })

# Take the first NUM elements in a DataFrame and return a named list for each row.

#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' take(df, 2)
#' }

setMethod("take",
          signature(rdd = "DataFrame", num = "numeric"),
          function(rdd, num) {
            rddIn <- toRDD(rdd)
            take(rddIn, num)
          })

#' toRDD()
#' 
#' Converts a Spark DataFrame to an RDD while preserving column names.
#' 
#' @param df A Spark DataFrame
#' 
#' @rdname DataFrame
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' rdd <- toRDD(df)
#' }

setGeneric("toRDD", function(df) { standardGeneric("toRDD") })

setMethod("toRDD",
          signature(df = "DataFrame"),
          function(df) {
            jrdd <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToRowRDD", df@sdf)
            names <- callJMethod(df@sdf, "columns")
            RDD(jrdd, serializedMode = "row", colNames = names)
          })

############################## RDD Map Functions ##################################
# All of the following functions mirror the existing RDD map functions,           #
# but allow for use with DataFrames by first converting to an RRDD before calling #
# the requested map function.                                                     #
###################################################################################

setMethod("lapply",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapply(rdd, FUN)
          })

setMethod("map",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

setMethod("flatMap",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            flatMap(rdd, FUN)
          })

setMethod("lapplyPartition",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapplyPartition(rdd, FUN)
          })

setMethod("mapPartitions",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

setMethod("lapplyPartitionsWithIndex",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN) {
            rdd <- toRDD(X)
            lapplyPartitionsWithIndex(rdd, FUN)
          })

setMethod("mapPartitionsWithIndex",
          signature(X = "DataFrame", FUN = "function"),
          function(X, FUN){
            lapplyPartitionsWithIndex(X, FUN)
          })

setMethod("foreach",
          signature(rdd = "DataFrame", func = "function"),
          function(rdd, func) {
            rddIn <- toRDD(rdd)
            foreach(rddIn, func)
          })

setMethod("foreachPartition",
          signature(rdd = "DataFrame", func = "function"),
          function(rdd, func) {
            rddIn <- toRDD(rdd)
            foreachPartition(rddIn, func)
          })

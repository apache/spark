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
            callJMethod(x@sdf, "count")
          })

#' Collects all the elements of a Spark DataFrame and coerces them into an R data.frame.

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

#' Limit
#' 
#' Limit the resulting DataFrame to the number of rows specified.
#' 
#' @param df A SparkSQL DataFrame
#' @param num The number of rows to return
#' @return A new DataFrame containing the number of rows specified.
#' 
#' @rdname limit
#' @export
#' @examples
#' \dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' limitedDF <- limit(df, 10)
#' }

setGeneric("limit", function(df, num) {standardGeneric("limit") })

setMethod("limit",
          signature(df = "DataFrame", num = "numeric"),
          function(df, num) {
            res <- callJMethod(df@sdf, "limit", as.integer(num))
            dataFrame(res)
            })

# Take the first NUM elements in a DataFrame and return a the results as a data.frame

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
            limited <- limit(rdd, num)
            collect(limited)
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
            colNames <- callJMethod(df@sdf, "columns")
            rdd <- RDD(jrdd, serializedMode = "row")
            lapply(rdd, function(row) {
              names(row) <- colNames
              row
            })
          })


setGeneric("groupBy", function(x, ...) { standardGeneric("groupBy") })

setMethod("groupBy",
           signature(x = "DataFrame"),
           function(x, col, ...) {
             jseq <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "toSeq", list(...))
             sgd <- callJMethod(x@sdf, "groupBy", col, jseq)
             groupedData(sgd)
           })


setGeneric("agg", function (x, ...) { standardGeneric("agg") })

setMethod("agg",
          signature(x = "DataFrame"),
          function(x, ...) {
            cols <- varargsToEnv(...)
            sdf <- callJMethod(x@sdf, "agg", cols)
            dataFrame(sdf)
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


############################## GroupedData ########################################

setClass("GroupedData",
         slots = list(env = "environment",
                      sgd = "jobj"))

setMethod("initialize", "GroupedData", function(.Object, sgd) {
  .Object@env <- new.env()
  .Object@sgd <- sgd
  .Object
})

groupedData <- function(sgd) {
  new("GroupedData", sgd)
}

setMethod("count",
          signature(x = "GroupedData"),
          function(x) {
            dataFrame(callJMethod(x@sgd, "count"))
          })

setMethod("agg",
          signature(x = "GroupedData"),
          function(x, ...) {
            cols <- varargsToEnv(...)
            sdf <- callJMethod(x@sgd, "agg", cols)
            dataFrame(sdf)
          })

#' sum/mean/avg/min/max

metheds <- c("sum", "mean", "avg", "min", "max")

setGeneric("avg", function(x, ...) { standardGeneric("avg") })

createMethod <- function(name) {
  setMethod(name,
            signature(x = "GroupedData"),
            function(x, ...) {
              jseq <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "toSeq", list(...))
              sdf <- callJMethod(x@sgd, name, jseq)
              dataFrame(sdf)
            })
}

for (name in metheds) {
  createMethod(name)
}

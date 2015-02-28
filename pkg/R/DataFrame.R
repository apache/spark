# DataFrame.R - DataFrame class and methods implemented in S4 OO classes

#' @include jobj.R SQLTypes.R RDD.R pairRDD.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a DataFrame
#' @description DataFrames can be created using functions like
#'              \code{jsonFile}, \code{table} etc.
#' @rdname DataFrame
#' @seealso jsonFile, table
#'
#' @param env An R environment that stores bookkeeping states of the DataFrame
#' @param sdf A Java object reference to the backing Scala DataFrame
#' @export

setClass("DataFrame",
         slots = list(env = "environment",
                      sdf = "jobj"))

setMethod("initialize", "DataFrame", function(.Object, sdf, isCached) {
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  
  .Object@sdf <- sdf
  .Object
})

#' @rdname DataFrame
#' @export

dataFrame <- function(sdf, isCached = FALSE) {
  new("DataFrame", sdf, isCached)
}

############################ DataFrame Methods ##############################################

#' Print Schema of a DataFrame
#' 
#' Prints out the schema in tree format
#' 
#' @param x A SparkSQL DataFrame
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

setGeneric("printSchema", function(x) { standardGeneric("printSchema") })

setMethod("printSchema",
          signature(x = "DataFrame"),
          function(x) {
            schemaString <- callJMethod(schema(x)$jobj, "treeString")
            cat(schemaString)
          })

#' Get schema object
#' 
#' Returns the schema of this DataFrame as a structType object.
#' 
#' @param x A SparkSQL DataFrame
#' 
#' @rdname schema
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' dfSchema <- schema(df)
#'}

setGeneric("schema", function(x) { standardGeneric("schema") })

setMethod("schema",
          signature(x = "DataFrame"),
          function(x) {
            structType(callJMethod(x@sdf, "schema"))
          })

#' DataTypes
#' 
#' Return all column names and their data types as a list
#' 
#' @param x A SparkSQL DataFrame
#' 
#' @rdname dtypes
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' dtypes(df)
#'}

setGeneric("dtypes", function(x) { standardGeneric("dtypes") })

setMethod("dtypes",
          signature(x = "DataFrame"),
          function(x) {
            lapply(schema(x)$fields(), function(f) {
              c(f$name(), f$dataType.simpleString())
            })
          })

#' Column names
#' 
#' Return all column names as a list
#' 
#' @param x A SparkSQL DataFrame
#' 
#' @rdname columns
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' columns(df)
#'}
setGeneric("columns", function(x) {standardGeneric("columns") })

setMethod("columns",
          signature(x = "DataFrame"),
          function(x) {
            sapply(schema(x)$fields(), function(f) {
              f$name()
            })
          })

#' @rdname columns
#' @export
setMethod("names",
          signature(x = "DataFrame"),
          function(x) {
            columns(x)
          })

#' Register Temporary Table
#' 
#' Registers a DataFrame as a Temporary Table in the SQLContext
#' 
#' @param x A SparkSQL DataFrame
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

setGeneric("registerTempTable", function(x, tableName) { standardGeneric("registerTempTable") })

setMethod("registerTempTable",
          signature(x = "DataFrame", tableName = "character"),
          function(x, tableName) {
              callJMethod(x@sdf, "registerTempTable", tableName)
          })

#' Cache
#' 
#' Persist with the default storage level (MEMORY_ONLY).
#' 
#' @param x A SparkSQL DataFrame
#' 
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' cache(df)
#'}

setMethod("cache",
          signature(x = "DataFrame"),
          function(x) {
            cached <- callJMethod(x@sdf, "cache")
            x@env$isCached <- TRUE
            x
          })

#' Persist
#'
#' Persist this DataFrame with the specified storage level. For details of the
#' supported storage levels, refer to
#' http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence.
#'
#' @param x The DataFrame to persist
#' @rdname persist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' persist(df, "MEMORY_AND_DISK")
#'}

setMethod("persist",
          signature(x = "DataFrame", newLevel = "character"),
          function(x, newLevel) {
            callJMethod(x@sdf, "persist", getStorageLevel(newLevel))
            x@env$isCached <- TRUE
            x
          })

#' Unpersist
#'
#' Mark this DataFrame as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param x The DataFrame to unpersist
#' @param blocking Whether to block until all blocks are deleted
#' @rdname unpersist-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' persist(df, "MEMORY_AND_DISK")
#' unpersist(df)
#'}

setMethod("unpersist",
          signature(x = "DataFrame"),
          function(x, blocking = TRUE) {
            callJMethod(x@sdf, "unpersist", blocking)
            x@env$isCached <- FALSE
            x
          })

#' Repartition
#'
#' Return a new DataFrame that has exactly numPartitions partitions.
#'
#' @param x A SparkSQL DataFrame
#' @param numPartitions The number of partitions to use.
#' @rdname repartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' newDF <- repartition(df, 2L)
#'}

setGeneric("repartition", function(x, numPartitions) { standardGeneric("repartition") })

#' @rdname repartition
#' @export
setMethod("repartition",
          signature(x = "DataFrame", numPartitions = "integer"),
          function(x, numPartitions) {
            sdf <- callJMethod(x@sdf, "repartition", as.integer(numPartitions))
            dataFrame(sdf)     
          })

#' Distinct
#'
#' Return a new DataFrame containing the distinct rows in this DataFrame.
#'
#' @param x A SparkSQL DataFrame
#' @rdname distinct
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' distinctDF <- distinct(df)
#'}

setMethod("distinct",
          signature(x = "DataFrame"),
          function(x) {
            sdf <- callJMethod(x@sdf, "distinct")
            dataFrame(sdf)
          })

#' SampleDF
#'
#' Return a sampled subset of this DataFrame using a random seed.
#'
#' @param x A SparkSQL DataFrame
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @rdname sampleDF
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' collect(sampleDF(df, FALSE, 0.5)) 
#' collect(sampleDF(df, TRUE, 0.5))
#'}

setGeneric("sampleDF",
           function(x, withReplacement, fraction, seed) {
             standardGeneric("sampleDF")
          })

#' @rdname sampleDF
#' @export

setMethod("sampleDF",
          # TODO : Figure out how to send integer as java.lang.Long to JVM so
          # we can send seed as an argument through callJMethod
          signature(x = "DataFrame", withReplacement = "logical",
                      fraction = "numeric"),
          function(x, withReplacement, fraction) {
            if (fraction < 0.0) stop(cat("Negative fraction value:", fraction))
            sdf <- callJMethod(x@sdf, "sample", withReplacement, fraction)
            dataFrame(sdf)
          })

#' Count
#' 
#' Returns the number of rows in a DataFrame
#' 
#' @param x A SparkSQL DataFrame
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
#'
#' @param x A SparkSQL DataFrame
#' @param stringsAsFactors (Optional) A logical indicating whether or not string columns
#' should be converted to factors. FALSE by default.

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
          signature(x = "DataFrame"),
          function(x, stringsAsFactors = FALSE) {
            # listCols is a list of raw vectors, one per column
            listCols <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToCols", x@sdf)
            cols <- lapply(listCols, function(col) {
              objRaw <- rawConnection(col)
              numRows <- readInt(objRaw)
              col <- readCol(objRaw, numRows)
              close(objRaw)
              col
            })
            colNames <- callJMethod(x@sdf, "columns")
            names(cols) <- colNames
            dfOut <- do.call(cbind.data.frame, list(cols, stringsAsFactors = stringsAsFactors))
            dfOut
          })

#' Limit
#' 
#' Limit the resulting DataFrame to the number of rows specified.
#' 
#' @param x A SparkSQL DataFrame
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

setGeneric("limit", function(x, num) {standardGeneric("limit") })

setMethod("limit",
          signature(x = "DataFrame", num = "numeric"),
          function(x, num) {
            res <- callJMethod(x@sdf, "limit", as.integer(num))
            dataFrame(res)
          })

# Take the first NUM rows of a DataFrame and return a the results as a data.frame

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
          signature(x = "DataFrame", num = "numeric"),
          function(x, num) {
            limited <- limit(x, num)
            collect(limited)
          })

#' Head
#'
#' Return the first NUM rows of a DataFrame as a data.frame. If NUM is NULL, 
#' then head() returns the first 6 rows in keeping with the current data.frame 
#' convention in R.
#'
#' @param x A SparkSQL DataFrame
#' @param num The number of rows to return. Default is 6.
#' @return A data.frame
#'
#' @rdname head
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' head(df)
#' }

setMethod("head",
          signature(x = "DataFrame"),
          function(x, num = 6L) {
          # Default num is 6L in keeping with R's data.frame convention
            take(x, num)
          })

#' Return the first row of a DataFrame
#'
#' @param x A SparkSQL DataFrame
#'
#' @rdname first
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlCtx <- sparkRSQL.init(sc)
#' path <- "path/to/file.json"
#' df <- jsonFile(sqlCtx, path)
#' first(df)
#' }

setGeneric("first", function(x) {standardGeneric("first") })

setMethod("first",
          signature(x = "DataFrame"),
          function(x) {
            take(x, 1)
          })

#' toRDD()
#' 
#' Converts a Spark DataFrame to an RDD while preserving column names.
#' 
#' @param x A Spark DataFrame
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

setGeneric("toRDD", function(x) { standardGeneric("toRDD") })

setMethod("toRDD",
          signature(x = "DataFrame"),
          function(x) {
            jrdd <- callJStatic("edu.berkeley.cs.amplab.sparkr.SQLUtils", "dfToRowRDD", x@sdf)
            colNames <- callJMethod(x@sdf, "columns")
            rdd <- RDD(jrdd, serializedMode = "row")
            lapply(rdd, function(row) {
              names(row) <- colNames
              row
            })
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
          signature(x = "DataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreach(rdd, func)
          })

setMethod("foreachPartition",
          signature(x = "DataFrame", func = "function"),
          function(x, func) {
            rdd <- toRDD(x)
            foreachPartition(rdd, func)
          })

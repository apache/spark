# context.R: SparkContext driven functions

#' Create an RDD from a text file.
#'
#' This function reads a text file from HDFS, a local file system (available on all
#' nodes), or any Hadoop-supported file system URI, and creates an
#' RDD of strings from it.
#'
#' @param sc SparkContext to use
#' @param path Path of file to read
#' @param minSplits Minimum number of splits to be created. If NULL, the default
#'  value is chosen based on available parallelism.
#' @return RDD where each item is of type \code{character}
#' @export
#' @examples
#'\dontrun{
#'  sc <- sparkR.init()
#'  lines <- textFile(sc, "myfile.txt")
#'}

textFile <- function(sc, path, minSplits = NULL) {
  if (is.null(minSplits)) {
    ssc <- .jcall(sc, "Lorg/apache/spark/SparkContext;", "sc")
    defaultParallelism <- .jcall(ssc, "I", "defaultParallelism")
    minSplits <- min(defaultParallelism, 2)
  }
  jrdd <- .jcall(sc, "Lorg/apache/spark/api/java/JavaRDD;", "textFile", path,
                 as.integer(minSplits))
  RDD(jrdd, FALSE)
}

#' Create an RDD from a homogeneous list or vector.
#'
#' This function creates an RDD from a local homogeneous list in R. The elements
#' in the list are split into \code{numSlices} slices and distributed to nodes
#' in the cluster.
#'
#' @param sc SparkContext to use
#' @param coll collection to parallelize
#' @param numSlices number of partitions to create in the RDD
#' @return an RDD created from this collection
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2)
#' # The RDD should contain 10 elements
#' length(rdd)
#'}
parallelize <- function(sc, coll, numSlices = 1) {
  # TODO: bound/safeguard numSlices
  # TODO: unit tests for if the split works for all primitives
  # TODO: support matrix, data frame, etc
  if (!is.list(coll)) {
    if (!is.vector(coll)) {
      message(paste("context.R: parallelize() currently only supports lists and vectors.",
                    "Calling as.list() to coerce coll into a list."))
    }
    coll <- as.list(coll)
  }

  if (numSlices > length(coll))
    numSlices <- length(coll)

  sliceLen <- length(coll) %/% numSlices
  slices <- split(coll, rep(1:(numSlices + 1), each = sliceLen)[1:length(coll)])

  # Serialize each slice: obtain a list of raws, or a list of lists (slices) of
  # 2-tuples of raws
  serializedSlices <- lapply(slices, serialize, connection = NULL)

  javaSerializedSlices <- .jarray(lapply(serializedSlices, .jarray),
                                  contents.class = "[B")

  jrddType = "Lorg/apache/spark/api/java/JavaRDD;"

  jrdd <- .jcall("edu/berkeley/cs/amplab/sparkr/RRDD",
                 jrddType,
                 "createRDDFromArray",
                 sc,
                 javaSerializedSlices)

  RDD(jrdd, TRUE)
}


#' Include this specified package on all workers
#'
#' This function can be used to include a package on all workers before the
#' user's code is executed. This is useful in scenarios where other R package
#' functions are used in a function passed to functions like \code{lapply}.
#' NOTE: The package is assumed to be installed on every node in the Spark
#' cluster.
#'
#' @param sc SparkContext to use
#' @param pkg Package name
#'
#' @export
#' @examples
#'\dontrun{
#'  library(Matrix)
#'
#'  sc <- sparkR.init()
#'  # Include the matrix library we will be using
#'  includePackage(Matrix)
#'
#'  generateSparse <- function(x) {
#'    sparseMatrix(i=c(1, 2, 3), j=c(1, 2, 3), x=c(1, 2, 3))
#'  }
#'
#'  rdd <- lapplyPartition(parallelize(sc, 1:2, 2L), generateSparse)
#'  collect(rdd)
#'}
includePackage <- function(sc, pkg) {
  pkg <- as.character(substitute(pkg))
  if (exists(".packages", .sparkREnv)) {
    packages <- .sparkREnv$.packages
  } else {
    packages <- list()
  }
  packages <- c(packages, pkg)
  .sparkREnv$.packages <- packages
}

#' @title Broadcast a variable to all workers
#'
#' @description
#' Broadcast a read-only variable to the cluster, returning a \code{Broadcast}
#' object for reading it in distributed functions.
#'
#' @param sc Spark Context to use
#' @param object Object to be broadcast
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:2, 2L)
#'
#' # Large Matrix object that we want to broadcast
#' randomMat <- matrix(nrow=100, ncol=10, data=rnorm(1000))
#' randomMatBr <- broadcast(sc, randomMat)
#'
#' # Use the broadcast variable inside the function
#' useBroadcast <- function(x) {
#'   sum(value(randomMatBr) * x)
#' }
#' sumRDD <- lapply(rdd, useBroadcast)
#'}
broadcast <- function(sc, object) {
  objName <- as.character(substitute(object))
  serializedObj <- serialize(object, connection = NULL, ascii = TRUE)
  serializedObjArr <- .jcast(.jarray(serializedObj),
                             new.class="java/lang/Object")
  jBroadcast <- .jcall(sc, "Lorg/apache/spark/broadcast/Broadcast;",
                       "broadcast", serializedObjArr)

  id <- as.character(.jsimplify(.jcall(jBroadcast, "J", "id")))
  Broadcast(id, object, jBroadcast, objName)
}

# context.R: SparkContext driven functions

#' Create an RDD from a text file.
#'
#' This function reads a text file from HDFS, a local file system (available on all
#' nodes), or any Hadoop-supported file system URI, and creates an
#' RDD of strings from it.
#'
#' @param sc SparkContext to use
#' @param name Path of file to read
#' @param minSplits Minimum number of splits to be created. If NULL, the default
#'  value is chosen based on available parallelism.
#' @return RRDD where each item is of type \code{character}
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
  RRDD(jrdd, FALSE)
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
#' @return an RRDD created from this collection
#' @export
#' @examples
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2)
#' # The RDD should contain 10 elements
#' length(rdd)
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

  javaSerializedSlices <- .jarray(lapply(serializedSlices, .jarray), contents.class = "[B")

  jrddType = "Lorg/apache/spark/api/java/JavaRDD;"

  jrdd <- .jcall("org/apache/spark/api/r/RRDD",
                 jrddType,
                 "createRDDFromArray",
                 sc,
                 javaSerializedSlices)

  RRDD(jrdd, TRUE)
}

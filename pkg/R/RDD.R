# RDD in R implemented in S4 OO system.

setOldClass("jobj")

#' @title S4 class that represents an RDD
#' @description RDD can be created using functions like
#'              \code{parallelize}, \code{textFile} etc.
#' @rdname RDD
#' @seealso parallelize, textFile
#'
#' @slot env An R environment that stores bookkeeping states of the RDD
#' @slot jrdd Java object reference to the backing JavaRDD
#' @export
setClass("RDD",
         slots = list(env = "environment",
                      jrdd = "jobj"))

setClass("PipelinedRDD",
         slots = list(prev = "RDD",
                      func = "function",
                      prev_jrdd = "jobj"),
         contains = "RDD")


setMethod("initialize", "RDD", function(.Object, jrdd, serialized,
                                        isCached, isCheckpointed) {
  # We use an environment to store mutable states inside an RDD object.
  # Note that R's call-by-value semantics makes modifying slots inside an
  # object (passed as an argument into a function, such as cache()) difficult:
  # i.e. one needs to make a copy of the RDD object and sets the new slot value
  # there.

  # The slots are inheritable from superclass. Here, both `env' and `jrdd' are
  # inherited from RDD, but only the former is used.
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  .Object@env$serialized <- serialized

  .Object@jrdd <- jrdd
  .Object
})

setMethod("initialize", "PipelinedRDD", function(.Object, prev, func, jrdd_val) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jrdd_val <- jrdd_val
   # This tracks if jrdd_val is serialized
  .Object@env$serialized <- prev@env$serialized

  # NOTE: We use prev_serialized to track if prev_jrdd is serialized
  # prev_serialized is used during the delayed computation of JRDD in getJRDD
  .Object@prev <- prev

  isPipelinable <- function(rdd) {
    e <- rdd@env
    !(e$isCached || e$isCheckpointed)
  }

  if (!inherits(prev, "PipelinedRDD") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- func
    .Object@prev_jrdd <- getJRDD(prev)
    # Since this is the first step in the pipeline, the prev_serialized
    # is same as serialized here.
    .Object@env$prev_serialized <- .Object@env$serialized
  } else {
    pipelinedFunc <- function(split, iterator) {
      func(split, prev@func(split, iterator))
    }
    .Object@func <- pipelinedFunc
    .Object@prev_jrdd <- prev@prev_jrdd # maintain the pipeline
    # Get if the prev_jrdd was serialized from the parent RDD
    .Object@env$prev_serialized <- prev@env$prev_serialized
  }

  .Object
})


#' @rdname RDD
#' @export
#'
#' @param jrdd Java object reference to the backing JavaRDD
#' @param serialized TRUE if the RDD stores data serialized in R
#' @param isCached TRUE if the RDD is cached
#' @param isCheckpointed TRUE if the RDD has been checkpointed
RDD <- function(jrdd, serialized = TRUE, isCached = FALSE,
                isCheckpointed = FALSE) {
  new("RDD", jrdd, serialized, isCached, isCheckpointed)
}

PipelinedRDD <- function(prev, func) {
  new("PipelinedRDD", prev, func, NULL)
}


# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })
setMethod("getJRDD", signature(rdd = "RDD"), function(rdd) rdd@jrdd )
setMethod("getJRDD", signature(rdd = "PipelinedRDD"),
          function(rdd, dataSerialization = TRUE) {
            if (!is.null(rdd@env$jrdd_val)) {
              return(rdd@env$jrdd_val)
            }

            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(split, part) {
              rdd@func(split, part)
            }
            serializedFuncArr <- serialize("computeFunc", connection = NULL,
                                           ascii = TRUE)

            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL,
                                         ascii = TRUE)

            broadcastArr <- lapply(ls(.broadcastNames),
                                   function(name) { get(name, .broadcastNames) })

            depsBin <- getDependencies(computeFunc)

            prev_jrdd <- rdd@prev_jrdd

            if (dataSerialization) {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.RRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            } else {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.StringRRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            }
            # Save the serialization flag after we create a RRDD
            rdd@env$serialized <- dataSerialization
            rdd@env$jrdd_val <- callJMethod(rddRef, "asJavaRDD") # rddRef$asJavaRDD()
            rdd@env$jrdd_val
          })


setValidity("RDD",
            function(object) {
              jrdd <- getJRDD(object)
              cls <- callJMethod(jrdd, "getClass")
              className <- callJMethod(cls, "getName")
              if (grep("spark.api.java.*RDD*", className) == 1) {
                TRUE
              } else {
                paste("Invalid RDD class ", className)
              }
            })


############ Actions and Transformations ############


#' Persist an RDD
#'
#' Persist this RDD with the default storage level (MEMORY_ONLY).
#'
#' @param rdd The RDD to cache
#' @rdname cache-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd)
#'}
setGeneric("cache", function(rdd) { standardGeneric("cache") })

#' @rdname cache-methods
#' @aliases cache,RDD-method
setMethod("cache",
          signature(rdd = "RDD"),
          function(rdd) {
            callJMethod(getJRDD(rdd), "cache")
            rdd@env$isCached <- TRUE
            rdd
          })

#' Persist an RDD
#'
#' Persist this RDD with the specified storage level. For details of the
#' supported storage levels, refer to
#' http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence.
#'
#' @param rdd The RDD to persist
#' @param newLevel The new storage level to be assigned
#' @rdname persist
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' persist(rdd, "MEMORY_AND_DISK")
#'}
setGeneric("persist", function(rdd, newLevel) { standardGeneric("persist") })

#' @rdname persist
#' @aliases persist,RDD-method
setMethod("persist",
          signature(rdd = "RDD", newLevel = "character"),
          function(rdd, newLevel = c("DISK_ONLY",
                                     "DISK_ONLY_2",
                                     "MEMORY_AND_DISK",
                                     "MEMORY_AND_DISK_2",
                                     "MEMORY_AND_DISK_SER",
                                     "MEMORY_AND_DISK_SER_2",
                                     "MEMORY_ONLY",
                                     "MEMORY_ONLY_2",
                                     "MEMORY_ONLY_SER",
                                     "MEMORY_ONLY_SER_2",
                                     "OFF_HEAP")) {
            match.arg(newLevel)
            storageLevel <- switch(newLevel,
              "DISK_ONLY" = callJStatic("org.apache.spark.storage.StorageLevel", "DISK_ONLY"),
              "DISK_ONLY_2" = callJStatic("org.apache.spark.storage.StorageLevel", "DISK_ONLY_2"),
              "MEMORY_AND_DISK" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_AND_DISK"),
              "MEMORY_AND_DISK_2" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_AND_DISK_2"),
              "MEMORY_AND_DISK_SER" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_AND_DISK_SER"),
              "MEMORY_AND_DISK_SER_2" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_AND_DISK_SER_2"),
              "MEMORY_ONLY" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_ONLY"),
              "MEMORY_ONLY_2" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_ONLY_2"),
              "MEMORY_ONLY_SER" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_ONLY_SER"),
              "MEMORY_ONLY_SER_2" = callJStatic("org.apache.spark.storage.StorageLevel", "MEMORY_ONLY_SER_2"),
              "OFF_HEAP" = callJStatic("org.apache.spark.storage.StorageLevel", "OFF_HEAP"))
            
            callJMethod(getJRDD(rdd), "persist", storageLevel)
            rdd@env$isCached <- TRUE
            rdd
          })

#' Unpersist an RDD
#'
#' Mark the RDD as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param rdd The RDD to unpersist
#' @rdname unpersist-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd) # rdd@@env$isCached == TRUE
#' unpersist(rdd) # rdd@@env$isCached == FALSE
#'}
setGeneric("unpersist", function(rdd) { standardGeneric("unpersist") })

#' @rdname unpersist-methods
#' @aliases unpersist,RDD-method
setMethod("unpersist",
          signature(rdd = "RDD"),
          function(rdd) {
            callJMethod(getJRDD(rdd), "unpersist")
            rdd@env$isCached <- FALSE
            rdd
          })


#' Checkpoint an RDD
#'
#' Mark this RDD for checkpointing. It will be saved to a file inside the
#' checkpoint directory set with setCheckpointDir() and all references to its
#' parent RDDs will be removed. This function must be called before any job has
#' been executed on this RDD. It is strongly recommended that this RDD is
#' persisted in memory, otherwise saving it on a file will require recomputation.
#'
#' @param rdd The RDD to checkpoint
#' @rdname checkpoint-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' setCheckpointDir(sc, "checkpoints")
#' rdd <- parallelize(sc, 1:10, 2L)
#' checkpoint(rdd)
#'}
setGeneric("checkpoint", function(rdd) { standardGeneric("checkpoint") })

#' @rdname checkpoint-methods
#' @aliases checkpoint,RDD-method
setMethod("checkpoint",
          signature(rdd = "RDD"),
          function(rdd) {
            jrdd <- getJRDD(rdd)
            callJMethod(jrdd, "checkpoint")
            rdd@env$isCheckpointed <- TRUE
            rdd
          })

#' Gets the number of partitions of an RDD
#'
#' @param rdd A RDD.
#' @return the number of partitions of rdd as an integer.
#' @rdname numPartitions
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' numPartitions(rdd)  # 2L
#'}
setGeneric("numPartitions", function(rdd) { standardGeneric("numPartitions") })

#' @rdname numPartitions
#' @aliases numPartitions,RDD-method
setMethod("numPartitions",
          signature(rdd = "RDD"),
          function(rdd) {
            jrdd <- getJRDD(rdd)
            partitions <- callJMethod(jrdd, "splits")
            callJMethod(partitions, "size")
          })

#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @param rdd The RDD to collect
#' @param ... Other optional arguments to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RDD
#' @rdname collect-methods
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' collect(rdd) # list from 1 to 10
#' collectPartition(rdd, 0L) # list from 1 to 5
#'}
setGeneric("collect", function(rdd, ...) { standardGeneric("collect") })

#' @rdname collect-methods
#' @aliases collect,RDD-method
setMethod("collect",
          signature(rdd = "RDD"),
          function(rdd, flatten = TRUE) {
            # Assumes a pairwise RDD is backed by a JavaPairRDD.
            collected <- callJMethod(getJRDD(rdd), "collect")
            convertJListToRList(collected, flatten)
          })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectPartition} returns a list that contains all of the elements
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
setGeneric("collectPartition",
           function(rdd, partitionId) {
             standardGeneric("collectPartition")
           })

#' @rdname collect-methods
#' @aliases collectPartition,integer,RDD-method
setMethod("collectPartition",
          signature(rdd = "RDD", partitionId = "integer"),
          function(rdd, partitionId) {
            jPartitionsList <- callJMethod(getJRDD(rdd),
                                           "collectPartitions",
                                           as.list(as.integer(partitionId)))

            jList <- jPartitionsList[[1]]
            convertJListToRList(jList, flatten = TRUE)
          })


#' Look up elements of a key in an RDD
#'
#' @description
#' \code{lookup} returns a list of values in this RDD for key key.
#'
#' @param rdd The RDD to collect
#' @param key The key to look up for
#' @return a list of values in this RDD for key key
#' @rdname lookup
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(c(1, 1), c(2, 2), c(1, 3))
#' rdd <- parallelize(sc, pairs)
#' lookup(rdd, 1) # list(1, 3)
#'}
setGeneric("lookup", function(rdd, key) { standardGeneric("lookup") })

#' @rdname lookup
#' @aliases lookup,RDD-method
setMethod("lookup",
          signature(rdd = "RDD", key = "ANY"),
          function(rdd, key) {
            partitionFunc <- function(part) {
              filtered <- part[unlist(lapply(part, function(x) { identical(key, x[[1]]) }))]
              lapply(filtered, function(x) { x[[2]] })
            }
            valsRDD <- lapplyPartition(rdd, partitionFunc)
            collect(valsRDD)
          })

#' Return the number of elements in the RDD.
#'
#' @param x The RDD to count
#' @return number of elements in the RDD.
#' @rdname count
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' count(rdd) # 10
#' length(rdd) # Same as count
#'}
setGeneric("count", function(x) { standardGeneric("count") })

#' @rdname count
#' @aliases count,RDD-method
setMethod("count",
          signature(x = "RDD"),
          function(x) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            valsRDD <- lapplyPartition(x, countPartition)
            vals <- collect(valsRDD)
            sum(as.integer(vals))
          })

#' Return the number of elements in the RDD
#' @export
#' @rdname count
setMethod("length",
          signature(x = "RDD"),
          function(x) {
            count(x)
          })

#' Return the count of each unique value in this RDD as a list of
#' (value, count) pairs.
#'
#' Same as countByValue in Spark.
#'
#' @param rdd The RDD to count
#' @return list of (value, count) pairs, where count is number of each unique
#' value in rdd.
#' @rdname countByValue
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,3,2,1))
#' countByValue(rdd) # (1,2L), (2,2L), (3,1L)
#'}
setGeneric("countByValue", function(rdd) { standardGeneric("countByValue") })

#' @rdname countByValue
#' @aliases countByValue,RDD-method
setMethod("countByValue",
          signature(rdd = "RDD"),
          function(rdd) {
            ones <- lapply(rdd, function(item) { list(item, 1L) })
            collect(reduceByKey(ones, `+`, numPartitions(rdd)))
          })

#' Count the number of elements for each key, and return the result to the
#' master as lists of (key, count) pairs.
#'
#' Same as countByKey in Spark.
#'
#' @param rdd The RDD to count keys.
#' @return list of (key, count) pairs, where count is number of each key in rdd.
#' @rdname countByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(c("a", 1), c("b", 1), c("a", 1)))
#' countByKey(rdd) # ("a", 2L), ("b", 1L)
#'}
setGeneric("countByKey", function(rdd) { standardGeneric("countByKey") })

#' @rdname countByKey
#' @aliases countByKey,RDD-method
setMethod("countByKey",
          signature(rdd = "RDD"),
          function(rdd) {
            keys <- lapply(rdd, function(item) { item[[1]] })
            countByValue(keys)
          })

#' Apply a function to all elements
#'
#' This function creates a new RDD by applying the given transformation to all
#' elements of the given RDD
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
#' @rdname lapply
#' @aliases lapply
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- lapply(rdd, function(x) { x * 2 })
#' collect(multiplyByTwo) # 2,4,6...
#'}
setMethod("lapply",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            func <- function(split, iterator) {
              lapply(iterator, FUN)
            }
            PipelinedRDD(X, func)
          })

#' @rdname lapply
#' @export
setGeneric("map", function(X, FUN) {
           standardGeneric("map") })

#' @rdname lapply
#' @aliases map,RDD,function-method
setMethod("map",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' Flatten results after apply a function to all elements
#'
#' This function return a new RDD by first applying a function to all
#' elements of this RDD, and then flattening the results.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
#' @rdname flatMap
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- flatMap(rdd, function(x) { list(x*2, x*10) })
#' collect(multiplyByTwo) # 2,20,4,40,6,60...
#'}
setGeneric("flatMap", function(X, FUN) {
           standardGeneric("flatMap") })

#' @rdname flatMap
#' @aliases flatMap,RDD,function-method
setMethod("flatMap",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              unlist(
                lapply(part, FUN),
                recursive = F
              )
            }
            lapplyPartition(X, partitionFunc)
          })

#' Apply a function to each partition of an RDD
#'
#' Return a new RDD by applying a function to each partition of this RDD.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition.
#' @return a new RDD created by the transformation.
#' @rdname lapplyPartition
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' partitionSum <- lapplyPartition(rdd, function(part) { Reduce("+", part) })
#' collect(partitionSum) # 15, 40
#'}
setGeneric("lapplyPartition", function(X, FUN) {
           standardGeneric("lapplyPartition") })

#' @rdname lapplyPartition
#' @aliases lapplyPartition,RDD,function-method
setMethod("lapplyPartition",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })

#' mapPartitions is the same as lapplyPartition.
#'
#' @rdname lapplyPartition
#' @export
setGeneric("mapPartitions", function(X, FUN) {
           standardGeneric("mapPartitions") })

#' @rdname lapplyPartition
#' @aliases mapPartitions,RDD,function-method
setMethod("mapPartitions",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

#' Return a new RDD by applying a function to each partition of this RDD, while
#' tracking the index of the original partition.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each partition; takes the partition
#'        index and a list of elements in the particular partition.
#' @return a new RDD created by the transformation.
#' @rdname lapplyPartitionsWithIndex
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 5L)
#' prod <- lapplyPartitionsWithIndex(rdd, function(split, part) {
#'                                          split * Reduce("+", part) })
#' collect(prod, flatten = FALSE) # 0, 7, 22, 45, 76
#'}
setGeneric("lapplyPartitionsWithIndex", function(X, FUN) {
           standardGeneric("lapplyPartitionsWithIndex") })

#' @rdname lapplyPartitionsWithIndex
#' @aliases lapplyPartitionsWithIndex,RDD,function-method
setMethod("lapplyPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            closureCapturingFunc <- function(split, part) {
              FUN(split, part)
            }
            PipelinedRDD(X, closureCapturingFunc)
          })


#' @rdname lapplyPartitionsWithIndex
#' @export
setGeneric("mapPartitionsWithIndex", function(X, FUN) {
           standardGeneric("mapPartitionsWithIndex") })

#' @rdname lapplyPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,RDD,function-method
setMethod("mapPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, FUN)
          })

#' This function returns a new RDD containing only the elements that satisfy
#' a predicate (i.e. returning TRUE in a given logical function).
#' The same as `filter()' in Spark.
#'
#' @param x The RDD to be filtered.
#' @param f A unary predicate function.
#' @rdname filterRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' unlist(collect(filterRDD(rdd, function (x) { x < 3 }))) # c(1, 2)
#'}
setGeneric("filterRDD", 
           function(x, f) { standardGeneric("filterRDD") })

#' @rdname filterRDD
#' @aliases filterRDD,RDD,function-method
setMethod("filterRDD",
          signature(x = "RDD", f = "function"),
          function(x, f) {
            filter.func <- function(part) {
              Filter(f, part)
            }
            lapplyPartition(x, filter.func)
          })

#' @rdname filterRDD
#' @export
#' @aliases Filter
setMethod("Filter",
          signature(f = "function", x = "RDD"),
          function(f, x) {
            filterRDD(x, f)
          })

#' Reduce across elements of an RDD.
#'
#' This function reduces the elements of this RDD using the
#' specified commutative and associative binary operator.
#'
#' @param rdd The RDD to reduce
#' @param func Commutative and associative function to apply on elements
#'             of the RDD.
#' @export
#' @rdname reduce
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' reduce(rdd, "+") # 55
#'}
setGeneric("reduce", function(rdd, func) { standardGeneric("reduce") })

#' @rdname reduce
#' @aliases reduce,RDD,ANY-method
setMethod("reduce",
          signature(rdd = "RDD", func = "ANY"),
          function(rdd, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collect(lapplyPartition(rdd, reducePartition),
                                     flatten = FALSE)
            Reduce(func, partitionList)
          })

#' Get the maximum element of an RDD.
#'
#' @param rdd The RDD to get the maximum element from
#' @export
#' @rdname maximum
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' maximum(rdd) # 10
#'}
setGeneric("maximum", function(rdd) { standardGeneric("maximum") })

#' @rdname maximum
#' @aliases maximum,RDD
setMethod("maximum",
          signature(rdd = "RDD"),
          function(rdd) {
            reduce(rdd, max)
          })

#' Get the minimum element of an RDD.
#'
#' @param rdd The RDD to get the minimum element from
#' @export
#' @rdname minimum
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' minimum(rdd) # 1
#'}
setGeneric("minimum", function(rdd) { standardGeneric("minimum") })

#' @rdname minimum
#' @aliases minimum,RDD
setMethod("minimum",
          signature(rdd = "RDD"),
          function(rdd) {
            reduce(rdd, min)
          })

#' Applies a function to all elements in an RDD, and force evaluation.
#'
#' @param rdd The RDD to apply the function
#' @param func The function to be applied.
#' @return invisible NULL.
#' @export
#' @rdname foreach
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreach(rdd, function(x) { save(x, file=...) })
#'}
setGeneric("foreach", function(rdd, func) { standardGeneric("foreach") })

#' @rdname foreach
#' @aliases foreach,RDD,function-method
setMethod("foreach",
          signature(rdd = "RDD", func = "function"),
          function(rdd, func) {
            partition.func <- function(x) {
              lapply(x, func)
              NULL
            }
            invisible(collect(mapPartitions(rdd, partition.func)))
          })

#' Applies a function to each partition in an RDD, and force evaluation.
#'
#' @export
#' @rdname foreach
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreachPartition(rdd, function(part) { save(part, file=...); NULL })
#'}
setGeneric("foreachPartition", 
           function(rdd, func) { standardGeneric("foreachPartition") })

#' @rdname foreach
#' @aliases foreachPartition,RDD,function-method
setMethod("foreachPartition",
          signature(rdd = "RDD", func = "function"),
          function(rdd, func) {
            invisible(collect(mapPartitions(rdd, func)))
          })

#' Take elements from an RDD.
#'
#' This function takes the first NUM elements in the RDD and
#' returns them in a list.
#'
#' @param rdd The RDD to take elements from
#' @param num Number of elements to take
#' @rdname take
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' take(rdd, 2L) # list(1, 2)
#'}
setGeneric("take", function(rdd, num) { standardGeneric("take") })

#' @rdname take
#' @aliases take,RDD,numeric-method
setMethod("take",
          signature(rdd = "RDD", num = "numeric"),
          function(rdd, num) {
            resList <- list()
            index <- -1
            jrdd <- getJRDD(rdd)
            numPartitions <- numPartitions(rdd)

            # TODO(shivaram): Collect more than one partition based on size
            # estimates similar to the scala version of `take`.
            while (TRUE) {
              index <- index + 1

              if (length(resList) >= num || index >= numPartitions)
                break

              # a JList of byte arrays
              partitionArr <- callJMethod(jrdd, "collectPartitions", as.list(as.integer(index)))
              partition <- partitionArr[[1]]

              size <- num - length(resList)
              # elems is capped to have at most `size` elements
              elems <- convertJListToRList(partition,
                                           flatten = TRUE,
                                           logicalUpperBound = size,
                                           serialized = rdd@env$serialized)
              # TODO: Check if this append is O(n^2)?
              resList <- append(resList, elems)
            }
            resList
          })

#' Removes the duplicates from RDD.
#'
#' This function returns a new RDD containing the distinct elements in the
#' given RDD. The same as `distinct()' in Spark.
#'
#' @param rdd The RDD to remove duplicates from.
#' @param numPartitions Number of partitions to create.
#' @rdname distinct
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,2,3,3,3))
#' sort(unlist(collect(distinct(rdd)))) # c(1, 2, 3)
#'}
setGeneric("distinct",
           function(rdd, numPartitions) { standardGeneric("distinct") })

setClassUnion("missingOrInteger", c("missing", "integer"))
#' @rdname distinct
#' @aliases distinct,RDD,missingOrInteger-method
setMethod("distinct",
          signature(rdd = "RDD", numPartitions = "missingOrInteger"),
          function(rdd, numPartitions) {
            if (missing(numPartitions)) {
              numPartitions <- SparkR::numPartitions(rdd)
            }
            identical.mapped <- lapply(rdd, function(x) { list(x, NULL) })
            reduced <- reduceByKey(identical.mapped,
                                   function(x, y) { x },
                                   numPartitions)
            resRDD <- lapply(reduced, function(x) { x[[1]] })
            resRDD
          })

#' Return an RDD that is a sampled subset of the given RDD.
#'
#' The same as `sample()' in Spark. (We rename it due to signature
#' inconsistencies with the `sample()' function in R's base package.)
#'
#' @param rdd The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#' @rdname sampleRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10) # ensure each num is in its own split
#' collect(sampleRDD(rdd, FALSE, 0.5, 1618L)) # ~5 distinct elements
#' collect(sampleRDD(rdd, TRUE, 0.5, 9L)) # ~5 elements possibly with duplicates
#'}
setGeneric("sampleRDD",
           function(rdd, withReplacement, fraction, seed) {
             standardGeneric("sampleRDD")
           })

#' @rdname sampleRDD
#' @aliases sampleRDD,RDD
setMethod("sampleRDD",
          signature(rdd = "RDD", withReplacement = "logical",
                    fraction = "numeric", seed = "integer"),
          function(rdd, withReplacement, fraction, seed) {

            # The sampler: takes a partition and returns its sampled version.
            samplingFunc <- function(split, part) {
              set.seed(seed)
              res <- vector("list", length(part))
              len <- 0

              # Discards some random values to ensure each partition has a
              # different random seed.
              runif(split)

              for (elem in part) {
                if (withReplacement) {
                  count <- rpois(1, fraction)
                  if (count > 0) {
                    res[(len + 1):(len + count)] <- rep(list(elem), count)
                    len <- len + count
                  }
                } else {
                  if (runif(1) < fraction) {
                    len <- len + 1
                    res[[len]] <- elem
                  }
                }
              }

              # TODO(zongheng): look into the performance of the current
              # implementation. Look into some iterator package? Note that
              # Scala avoids many calls to creating an empty list and PySpark
              # similarly achieves this using `yield'.
              if (len > 0)
                res[1:len]
              else
                list()
            }

            lapplyPartitionsWithIndex(rdd, samplingFunc)
          })


#' Return a list of the elements that are a sampled subset of the given RDD.
#'
#' @param rdd The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param num Number of elements to return
#' @param seed Randomness seed value
#' @rdname takeSample
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:100)
#' # exactly 5 elements sampled, which may not be distinct
#' takeSample(rdd, TRUE, 5L, 1618L)
#' # exactly 5 distinct elements sampled
#' takeSample(rdd, FALSE, 5L, 16181618L)
#'}
setGeneric("takeSample",
           function(rdd, withReplacement, num, seed) {
             standardGeneric("takeSample")
           })
#' @rdname takeSample
#' @aliases takeSample,RDD
setMethod("takeSample", signature(rdd = "RDD", withReplacement = "logical",
                                  num = "integer", seed = "integer"),
          function(rdd, withReplacement, num, seed) {
            # This function is ported from RDD.scala.
            fraction <- 0.0
            total <- 0
            multiplier <- 3.0
            initialCount <- count(rdd)
            maxSelected <- 0
            MAXINT <- .Machine$integer.max

            if (num < 0)
              stop(paste("Negative number of elements requested"))

            if (initialCount > MAXINT - 1) {
              maxSelected <- MAXINT - 1
            } else {
              maxSelected <- initialCount
            }

            if (num > initialCount && !withReplacement) {
              total <- maxSelected
              fraction <- multiplier * (maxSelected + 1) / initialCount
            } else {
              total <- num
              fraction <- multiplier * (num + 1) / initialCount
            }

            set.seed(seed)
            samples <- collect(sampleRDD(rdd, withReplacement, fraction,
                                         as.integer(ceiling(runif(1,
                                                                  -MAXINT,
                                                                  MAXINT)))))
            # If the first sample didn't turn out large enough, keep trying to
            # take samples; this shouldn't happen often because we use a big
            # multiplier for thei initial size
            while (length(samples) < total)
              samples <- collect(sampleRDD(rdd, withReplacement, fraction,
                                           as.integer(ceiling(runif(1,
                                                                    -MAXINT,
                                                                    MAXINT)))))

            # TODO(zongheng): investigate if this call is an in-place shuffle?
            sample(samples)[1:total]
          })

#' Creates tuples of the elements in this RDD by applying a function.
#'
#' @param rdd The RDD.
#' @param func The function to be applied.
#' @rdname keyBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3))
#' collect(keyBy(rdd, function(x) { x*x })) # list(list(1, 1), list(4, 2), list(9, 3))
#'}
setGeneric("keyBy", function(rdd, func) { standardGeneric("keyBy") })

#' @rdname keyBy
#' @aliases keyBy,RDD
setMethod("keyBy",
          signature(rdd = "RDD", func = "function"),
          function(rdd, func) {
            apply.func <- function(x) {
              list(func(x), x)
            }
            lapply(rdd, apply.func)
          })

#' Save this RDD as a SequenceFile of serialized objects.
#'
#' @param rdd The RDD to save
#' @param path The directory where the file is saved
#' @rdname saveAsObjectFile
#' @seealso objectFile
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsObjectFile(rdd, "/tmp/sparkR-tmp")
#'}
setGeneric("saveAsObjectFile", function(rdd, path) { standardGeneric("saveAsObjectFile") })

#' @rdname saveAsObjectFile
#' @aliases saveAsObjectFile,RDD
setMethod("saveAsObjectFile",
          signature(rdd = "RDD", path = "character"),
          function(rdd, path) {
            # If the RDD is in string format, need to serialize it before saving it because when
            # objectFile() is invoked to load the saved file, only serialized format is assumed.
            if (!rdd@env$serialized) {
              rdd <- reserialize(rdd)
            }
            # Return nothing
            invisible(callJMethod(getJRDD(rdd), "saveAsObjectFile", path))
          })

#' Save this RDD as a text file, using string representations of elements.
#'
#' @param rdd The RDD to save
#' @param path The directory where the splits of the text file are saved
#' @rdname saveAsTextFile
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsTextFile(rdd, "/tmp/sparkR-tmp")
#'}
setGeneric("saveAsTextFile", function(rdd, path) { standardGeneric("saveAsTextFile") })

#' @rdname saveAsTextFile
#' @aliases saveAsTextFile,RDD
setMethod("saveAsTextFile",
          signature(rdd = "RDD", path = "character"),
          function(rdd, path) {
            func <- function(x) {
              toString(x)
            }
            stringRdd <- lapply(rdd, func)
            # Return nothing
            invisible(
              callJMethod(getJRDD(stringRdd, dataSerialization = FALSE), "saveAsTextFile", path))
          })

#' Return an RDD with the keys of each tuple.
#'
#' @param rdd The RDD from which the keys of each tuple is returned.
#' @rdname keys
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(keys(rdd)) # list(1, 3)
#'}
setGeneric("keys", function(rdd) { standardGeneric("keys") })

#' @rdname keys
#' @aliases keys,RDD
setMethod("keys",
          signature(rdd = "RDD"),
          function(rdd) {
            func <- function(x) {
              x[[1]]
            }
            lapply(rdd, func)
          })

#' Return an RDD with the values of each tuple.
#'
#' @param rdd The RDD from which the values of each tuple is returned.
#' @rdname values
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)))
#' collect(values(rdd)) # list(2, 4)
#'}
setGeneric("values", function(rdd) { standardGeneric("values") })

#' @rdname values
#' @aliases values,RDD
setMethod("values",
          signature(rdd = "RDD"),
          function(rdd) {
            func <- function(x) {
              x[[2]]
            }
            lapply(rdd, func)
          })

#' Applies a function to all values of the elements, without modifying the keys.
#'
#' The same as `mapValues()' in Spark.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on the value of each element.
#' @return a new RDD created by the transformation.
#' @rdname mapValues
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' makePairs <- lapply(rdd, function(x) { list(x, x) })
#' collect(mapValues(makePairs, function(x) { x * 2) })
#' Output: list(list(1,2), list(2,4), list(3,6), ...)
#'}
setGeneric("mapValues", function(X, FUN) { standardGeneric("mapValues") })

#' @rdname mapValues
#' @aliases mapValues,RDD,function-method
setMethod("mapValues",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            func <- function(x) {
              list(x[[1]], FUN(x[[2]]))
            }
            lapply(X, func)
          })

#' Pass each value in the key-value pair RDD through a flatMap function without
#' changing the keys; this also retains the original RDD's partitioning.
#'
#' The same as 'flatMapValues()' in Spark.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on the value of each element.
#' @return a new RDD created by the transformation.
#' @rdname flatMapValues
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, c(1,2)), list(2, c(3,4))))
#' collect(flatMapValues(rdd, function(x) { x }))
#' Output: list(list(1,1), list(1,2), list(2,3), list(2,4))
#'}
setGeneric("flatMapValues", function(X, FUN) { standardGeneric("flatMapValues") })

#' @rdname flatMapValues
#' @aliases flatMapValues,RDD,function-method
setMethod("flatMapValues",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            flatMapFunc <- function(x) {
              lapply(FUN(x[[2]]), function(v) { list(x[[1]], v) })
            }
            flatMap(X, flatMapFunc)
          })

############ Shuffle Functions ############

#' Partition an RDD by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' For each element of this RDD, the partitioner is used to compute a hash
#' function and the RDD is partitioned using this hash value.
#'
#' @param rdd The RDD to partition. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @param ... Other optional arguments to partitionBy.
#'
#' @param partitionFunc The partition function to use. Uses a default hashCode
#'                      function if not provided
#' @return An RDD partitioned using the specified partitioner.
#' @rdname partitionBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- partitionBy(rdd, 2L)
#' collectPartition(parts, 0L) # First partition should contain list(1, 2) and list(1, 4)
#'}
setGeneric("partitionBy",
           function(rdd, numPartitions, ...) {
             standardGeneric("partitionBy")
           })

#' @rdname partitionBy
#' @aliases partitionBy,RDD,integer-method
setMethod("partitionBy",
          signature(rdd = "RDD", numPartitions = "integer"),
          function(rdd, numPartitions, partitionFunc = hashCode) {

            #if (missing(partitionFunc)) {
            #  partitionFunc <- hashCode
            #}

            depsBinArr <- getDependencies(partitionFunc)

            serializedHashFuncBytes <- serialize(as.character(substitute(partitionFunc)),
                                                 connection = NULL,
                                                 ascii = TRUE)

            packageNamesArr <- serialize(.sparkREnv$.packages,
                                         connection = NULL,
                                         ascii = TRUE)
            broadcastArr <- lapply(ls(.broadcastNames), function(name) {
                                   get(name, .broadcastNames) })
            jrdd <- getJRDD(rdd)

            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs).
            pairwiseRRDD <- newJObject("edu.berkeley.cs.amplab.sparkr.PairwiseRRDD",
                                       callJMethod(jrdd, "rdd"),
                                       as.integer(numPartitions),
                                       serializedHashFuncBytes,
                                       rdd@env$serialized,
                                       depsBinArr,
                                       packageNamesArr,
                                       as.character(.sparkREnv$libname),
                                       broadcastArr,
                                       callJMethod(jrdd, "classTag"))

            # Create a corresponding partitioner.
            rPartitioner <- newJObject("org.apache.spark.HashPartitioner",
                                       as.integer(numPartitions))

            # Call partitionBy on the obtained PairwiseRDD.
            javaPairRDD <- callJMethod(pairwiseRRDD, "asJavaPairRDD")
            javaPairRDD <- callJMethod(javaPairRDD, "partitionBy", rPartitioner)

            # Call .values() on the result to get back the final result, the
            # shuffled acutal content key-val pairs.
            r <- callJMethod(javaPairRDD, "values")

            RDD(r, serialized = TRUE)
          })

#' Group values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and group values for each key in the RDD into a single sequence.
#'
#' @param rdd The RDD to group. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, list(V))
#' @seealso reduceByKey
#' @rdname groupByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- groupByKey(rdd, 2L)
#' grouped <- collect(parts)
#' grouped[[1]] # Should be a list(1, list(2, 4))
#'}
setGeneric("groupByKey",
           function(rdd, numPartitions) {
             standardGeneric("groupByKey")
           })

#' @rdname groupByKey
#' @aliases groupByKey,RDD,integer-method
setMethod("groupByKey",
          signature(rdd = "RDD", numPartitions = "integer"),
          function(rdd, numPartitions) {
            shuffled <- partitionBy(rdd, numPartitions)
            groupVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              # Each item in the partition is list of (K, V)
              lapply(part,
                     function(item) {
                       hashVal <- as.character(hashCode(item[[1]]))
                       if (exists(hashVal, vals)) {
                         acc <- vals[[hashVal]]
                         acc[[length(acc) + 1]] <- item[[2]]
                         vals[[hashVal]] <- acc
                       } else {
                         vals[[hashVal]] <- list(item[[2]])
                         keys[[hashVal]] <- item[[1]]
                       }
                     })
              # Every key in the environment contains a list
              # Convert that to list(K, Seq[V])
              grouped <- lapply(ls(vals),
                                function(name) {
                                  list(keys[[name]], vals[[name]])
                                })
              grouped
            }
            lapplyPartition(shuffled, groupVals)
          })

#' Merge values by key
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function.
#'
#' @param rdd The RDD to reduce by key. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param combineFunc The associative reduce function to use.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, V') where V' is the merged
#'         value
#' @rdname reduceByKey
#' @seealso groupByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- reduceByKey(rdd, "+", 2L)
#' reduced <- collect(parts)
#' reduced[[1]] # Should be a list(1, 6)
#'}
setGeneric("reduceByKey",
           function(rdd, combineFunc, numPartitions) {
             standardGeneric("reduceByKey")
           })

#' @rdname reduceByKey
#' @aliases reduceByKey,RDD,integer-method
setMethod("reduceByKey",
          signature(rdd = "RDD", combineFunc = "ANY", numPartitions = "integer"),
          function(rdd, combineFunc, numPartitions) {
            reduceVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       hashVal <- as.character(hashCode(item[[1]]))
                       if (exists(hashVal, vals)) {
                         vals[[hashVal]] <- do.call(
                           combineFunc, list(vals[[hashVal]], item[[2]]))
                       } else {
                         vals[[hashVal]] <- item[[2]]
                         keys[[hashVal]] <- item[[1]]
                       }
                     })
              combined <- lapply(ls(vals),
                                  function(name) {
                                    list(keys[[name]], vals[[name]])
                                  })
              combined
            }
            locallyReduced <- lapplyPartition(rdd, reduceVals)
            shuffled <- partitionBy(locallyReduced, numPartitions)
            lapplyPartition(shuffled, reduceVals)
          })

#' Combine values by key
#'
#' Generic function to combine the elements for each key using a custom set of
#' aggregation functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)],
#' for a "combined type" C. Note that V and C can be different -- for example, one
#' might group an RDD of type (Int, Int) into an RDD of type (Int, Seq[Int]).

#' Users provide three functions:
#' \itemize{
#'   \item createCombiner, which turns a V into a C (e.g., creates a one-element list)
#'   \item mergeValue, to merge a V into a C (e.g., adds it to the end of a list) -
#'   \item mergeCombiners, to combine two C's into a single one (e.g., concatentates
#'    two lists).
#' }
#'
#' @param rdd The RDD to combine. Should be an RDD where each element is
#'             list(K, V) or c(K, V).
#' @param createCombiner Create a combiner (C) given a value (V)
#' @param mergeValue Merge the given value (V) with an existing combiner (C)
#' @param mergeCombiners Merge two combiners and return a new combiner
#' @param numPartitions Number of partitions to create.
#' @return An RDD where each element is list(K, C) where C is the combined type
#'
#' @rdname combineByKey
#' @seealso groupByKey, reduceByKey
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
#' rdd <- parallelize(sc, pairs)
#' parts <- combineByKey(rdd, function(x) { x }, "+", "+", 2L)
#' combined <- collect(parts)
#' combined[[1]] # Should be a list(1, 6)
#'}
setGeneric("combineByKey",
           function(rdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
             standardGeneric("combineByKey")
           })

#' @rdname combineByKey
#' @aliases combineByKey,RDD,ANY,ANY,ANY,integer-method
setMethod("combineByKey",
          signature(rdd = "RDD", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "integer"),
          function(rdd, createCombiner, mergeValue, mergeCombiners, numPartitions) {
            combineLocally <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       k <- as.character(item[[1]])
                       if (!exists(k, keys)) {
                         combiners[[k]] <- do.call(createCombiner,
                                                   list(item[[2]]))
                         keys[[k]] <- item[[1]]
                       } else {
                         combiners[[k]] <- do.call(mergeValue,
                                                   list(combiners[[k]],
                                                        item[[2]]))
                       }
                     })
              lapply(ls(keys), function(k) {
                      list(keys[[k]], combiners[[k]])
                     })
            }
            locallyCombined <- lapplyPartition(rdd, combineLocally)
            shuffled <- partitionBy(locallyCombined, numPartitions)
            mergeAfterShuffle <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              lapply(part,
                     function(item) {
                       k <- as.character(item[[1]])
                       if (!exists(k, combiners)) {
                         combiners[[k]] <- item[[2]]
                         keys[[k]] <- item[[1]]
                       } else {
                         combiners[[k]] <- do.call(mergeCombiners,
                                                   list(combiners[[k]],
                                                        item[[2]]))
                       }
                     })
              lapply(ls(keys), function(k) {
                      list(keys[[k]], combiners[[k]])
                     })
            }
            combined <-lapplyPartition(shuffled, mergeAfterShuffle)
            combined
          })

############ Binary Functions #############

#' Return the union RDD of two RDDs.
#' The same as union() in Spark.
#'
#' @param x An RDD.
#' @param y An RDD.
#' @return a new RDD created by performing the simple union (witout removing
#' duplicates) of two input RDDs.
#' @rdname unionRDD
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' unionRDD(rdd, rdd) # 1, 2, 3, 1, 2, 3
#'}
setGeneric("unionRDD", function(x, y) { standardGeneric("unionRDD") })

#' @rdname unionRDD
#' @aliases unionRDD,RDD,RDD-method
setMethod("unionRDD",
          signature(x = "RDD", y = "RDD"),
          function(x, y) {
            if (x@env$serialized == y@env$serialized) {
              jrdd <- callJMethod(getJRDD(x), "union", getJRDD(y))
              union.rdd <- RDD(jrdd, x@env$serialized)
            } else {
              # One of the RDDs is not serialized, we need to serialize it first.
              if (!x@env$serialized) {
                x <- reserialize(x)
              } else {
                y <- reserialize(y)
              }
              jrdd <- callJMethod(getJRDD(x), "union", getJRDD(y))
              union.rdd <- RDD(jrdd, TRUE)
            }
            union.rdd
          })

#' Join two RDDs
#'
#' This function joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with matching keys in
#'         two input RDDs.
#' @rdname join
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' join(rdd1, rdd2, 2L) # list(list(1, list(1, 2)), list(1, list(1, 3))
#'}
setGeneric("join", function(rdd1, rdd2, numPartitions) { standardGeneric("join") })

#' @rdname join
#' @aliases join,RDD,RDD-method
setMethod("join",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Left outer join two RDDs
#'
#' This function left-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in rdd1, the resulting RDD will either contain 
#'         all pairs (k, (v, w)) for (k, w) in rdd2, or the pair (k, (v, NULL)) 
#'         if no elements in rdd2 have key k.
#' @rdname leftOuterJoin
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' leftOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
#'}
setGeneric("leftOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("leftOuterJoin") })

#' @rdname leftOuterJoin
#' @aliases leftOuterJoin,RDD,RDD-method
setMethod("leftOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, TRUE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Right outer join two RDDs
#'
#' This function right-outer-joins two RDDs where every element is of the form list(K, V).
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, w) in rdd2, the resulting RDD will either contain
#'         all pairs (k, (v, w)) for (k, v) in rdd1, or the pair (k, (NULL, w))
#'         if no elements in rdd1 have key k.
#' @rdname rightOuterJoin
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rightOuterJoin(rdd1, rdd2, 2L)
#' # list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
#'}
setGeneric("rightOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("rightOuterJoin") })

#' @rdname rightOuterJoin
#' @aliases rightOuterJoin,RDD,RDD-method
setMethod("rightOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' Full outer join two RDDs
#'
#' This function full-outer-joins two RDDs where every element is of the form 
#' list(K, V). 
#' The key types of the two RDDs should be the same.
#'
#' @param rdd1 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param rdd2 An RDD to be joined. Should be an RDD where each element is
#'             list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return For each element (k, v) in rdd1 and (k, w) in rdd2, the resulting RDD
#'         will contain all pairs (k, (v, w)) for both (k, v) in rdd1 and and
#'         (k, w) in rdd2, or the pair (k, (NULL, w))/(k, (v, NULL)) if no elements 
#'         in rdd1/rdd2 have key k.
#' @rdname fullOuterJoin
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 2), list(1, 3), list(3, 3)))
#' rdd2 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' fullOuterJoin(rdd1, rdd2, 2L) # list(list(1, list(2, 1)),
#'                               #      list(1, list(3, 1)),
#'                               #      list(2, list(NULL, 4)))
#'                               #      list(3, list(3, NULL)),
#'}
setGeneric("fullOuterJoin", function(rdd1, rdd2, numPartitions) { standardGeneric("fullOuterJoin") })

#' @rdname fullOuterJoin
#' @aliases fullOuterJoin,RDD,RDD-method

setMethod("fullOuterJoin",
          signature(rdd1 = "RDD", rdd2 = "RDD", numPartitions = "integer"),
          function(rdd1, rdd2, numPartitions) {
            rdd1Tagged <- lapply(rdd1, function(x) { list(x[[1]], list(1L, x[[2]])) })
            rdd2Tagged <- lapply(rdd2, function(x) { list(x[[1]], list(2L, x[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, TRUE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(rdd1Tagged, rdd2Tagged), numPartitions), doJoin)
          })

#' For each key k in several RDDs, return a resulting RDD that
#' whose values are a list of values for the key in all RDDs.
#'
#' @param ... Several RDDs.
#' @param numPartitions Number of partitions to create.
#' @return a new RDD containing all pairs of elements with values in a list
#' in all RDDs.
#' @rdname cogroup
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(list(1, 1), list(2, 4)))
#' rdd2 <- parallelize(sc, list(list(1, 2), list(1, 3)))
#' cogroup(rdd1, rdd2, numPartitions = 2L) 
#' # list(list(1, list(1, list(2, 3))), list(2, list(list(4), list()))
#'}
setGeneric("cogroup", 
           function(..., numPartitions) { standardGeneric("cogroup") },
           signature = "...")

#' @rdname cogroup
#' @aliases cogroup,RDD-method
setMethod("cogroup",
          "RDD",
          function(..., numPartitions) {
            rdds <- list(...)
            rddsLen <- length(rdds)
            for (i in 1:rddsLen) {
              rdds[[i]] <- lapply(rdds[[i]], 
                                  function(x) { list(x[[1]], list(i, x[[2]])) })
              # TODO(hao): As issue [SparkR-142] mentions, the right value of i
              # will not be captured into UDF if getJRDD is not invoked.
              # It should be resolved together with that issue.
              getJRDD(rdds[[i]])  # Capture the closure.
            }
            union.rdd <- Reduce(unionRDD, rdds)
            group.func <- function(vlist) {
              res <- list()
              length(res) <- rddsLen
              for (x in vlist) {
                i <- x[[1]]
                acc <- res[[i]]
                # Create an accumulator.
                if (is.null(acc)) {
                  acc <- SparkR:::initAccumulator()
                }
                SparkR:::addItemToAccumulator(acc, x[[2]])
                res[[i]] <- acc
              }
              lapply(res, function(acc) {
                if (is.null(acc)) {
                  list()
                } else {
                  acc$data
                }
              })
            }
            cogroup.rdd <- mapValues(groupByKey(union.rdd, numPartitions), 
                                     group.func)
          })

# TODO: Consider caching the name in the RDD's environment
#' Return an RDD's name.
#'
#' @param rdd The RDD whose name is returned.
#' @rdname name
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' name(rdd) # NULL (if not set before)
#'}
setGeneric("name", function(rdd) { standardGeneric("name") })

#' @rdname name
#' @aliases name,RDD
setMethod("name",
          signature(rdd = "RDD"),
          function(rdd) {
            callJMethod(getJRDD(rdd), "name")
          })

#' Set an RDD's name.
#'
#' @param rdd The RDD whose name is to be set.
#' @param name The RDD name to be set.
#' @return a new RDD renamed.
#' @rdname setName
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' setName(rdd, "myRDD")
#' name(rdd) # "myRDD"
#'}
setGeneric("setName", function(rdd, name) { standardGeneric("setName") })

#' @rdname setName
#' @aliases setName,RDD
setMethod("setName",
          signature(rdd = "RDD", name = "character"),
          function(rdd, name) {
            callJMethod(getJRDD(rdd), "setName", name)
            rdd
          })

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
                      jrdd = "jobj",
                      colNames = "list"))

setClass("PipelinedRDD",
         slots = list(prev = "RDD",
                      func = "function",
                      prev_jrdd = "jobj"),
         contains = "RDD")


setMethod("initialize", "RDD", function(.Object, jrdd, serialized,
                                        isCached, isCheckpointed, colNames) {
  # Check that RDD constructor is using the correct version of serialized
  stopifnot(class(serialized) == "character") 
  
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

  .Object@colNames <- colNames

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
  
  .Object@colNames <- prev@colNames

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

    .Object@colNames <- prev@colNames
  } else {
    pipelinedFunc <- function(split, iterator) {
      func(split, prev@func(split, iterator))
    }
    .Object@func <- pipelinedFunc
    .Object@prev_jrdd <- prev@prev_jrdd # maintain the pipeline
    # Get if the prev_jrdd was serialized from the parent RDD
    .Object@env$prev_serialized <- prev@env$prev_serialized
    
    .Object@colNames <- prev@colNames
  }

  .Object
})

#' @rdname RDD
#' @export
#'
#' @param jrdd Java object reference to the backing JavaRDD
#' @param serialized "byte" if the RDD stores data serialized in R, "string" if the RDD stores strings, 
#'        and "rows" if the RDD stores the rows of a DataFrame
#' @param isCached TRUE if the RDD is cached
#' @param isCheckpointed TRUE if the RDD has been checkpointed
RDD <- function(jrdd, serialized = "byte", isCached = FALSE,
                isCheckpointed = FALSE, colNames = list()) {
  new("RDD", jrdd, serialized, isCached, isCheckpointed, colNames)
}

PipelinedRDD <- function(prev, func) {
  new("PipelinedRDD", prev, func, NULL)
}

# The jrdd accessor function.
setGeneric("getJRDD", function(rdd, ...) { standardGeneric("getJRDD") })
setMethod("getJRDD", signature(rdd = "RDD"), function(rdd) rdd@jrdd )
setMethod("getJRDD", signature(rdd = "PipelinedRDD"),
          function(rdd, dataSerialization = "byte") {
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

            if (dataSerialization == "string") {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.StringRRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   rdd@colNames,
                                   callJMethod(prev_jrdd, "classTag"))
            } else {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.RRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   rdd@colNames,
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
            convertJListToRList(collected, flatten,
              serialized = rdd@env$serialized, colNames = rdd@colNames)
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
            convertJListToRList(jList, flatten = TRUE,
              serialized = rdd@env$serialized, colNames = rdd@colNames)
          })

#' @rdname collect-methods
#' @export
#' @description
#' \code{collectAsMap} returns a named list as a map that contains all of the elements
#' in a key-value pair RDD. 
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)), 2L)
#' collectAsMap(rdd) # list(`1` = 2, `3` = 4)
#'}
setGeneric("collectAsMap", function(rdd) { standardGeneric("collectAsMap") })

#' @rdname collect-methods
#' @aliases collectAsMap,RDD-method
setMethod("collectAsMap",
          signature(rdd = "RDD"),
          function(rdd) {
            pairList <- collect(rdd)
            map <- new.env()
            lapply(pairList, function(x) { assign(as.character(x[[1]]), x[[2]], envir = map) })
            as.list(map)
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
                                           serialized = rdd@env$serialized,
                                           colNames = rdd@colNames)
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
            if (rdd@env$serialized == "string") {
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
              callJMethod(getJRDD(stringRdd, dataSerialization = "string"), "saveAsTextFile", path))
          })

#' Sort an RDD by the given key function.
#'
#' @param rdd An RDD to be sorted.
#' @param func A function used to compute the sort key for each element.
#' @param ascending A flag to indicate whether the sorting is ascending or descending.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where all elements are sorted.
#' @rdname sortBy
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(3, 2, 1))
#' collect(sortBy(rdd, function(x) { x })) # list (1, 2, 3)
#'}
setGeneric("sortBy", function(rdd,
                              func,
                              ascending = TRUE,
                              numPartitions = 1L) {
                       standardGeneric("sortBy")
                     })

#' @rdname sortBy
#' @aliases sortBy,RDD,RDD-method
setMethod("sortBy",
          signature(rdd = "RDD", func = "function"),
          function(rdd, func, ascending = TRUE, numPartitions = SparkR::numPartitions(rdd)) {          
            values(sortByKey(keyBy(rdd, func), ascending, numPartitions))
          })

# Helper function to get first N elements from an RDD in the specified order.
# Param:
#   rdd An RDD.
#   num Number of elements to return.
#   ascending A flag to indicate whether the sorting is ascending or descending.
# Return:
#   A list of the first N elements from the RDD in the specified order.
#
takeOrderedElem <- function(rdd, num, ascending = TRUE) {          
  if (num <= 0L) {
    return(list())
  }
  
  partitionFunc <- function(part) {
    if (num < length(part)) {
      # R limitation: order works only on primitive types!
      ord <- order(unlist(part, recursive = FALSE), decreasing = !ascending)
      list(part[ord[1:num]])
    } else {
      list(part)
    }
  }

  reduceFunc <- function(elems, part) {
    newElems <- append(elems, part)
    # R limitation: order works only on primitive types!
    ord <- order(unlist(newElems, recursive = FALSE), decreasing = !ascending)
    newElems[ord[1:num]]
  }
  
  newRdd <- mapPartitions(rdd, partitionFunc)
  reduce(newRdd, reduceFunc)
}

#' Returns the first N elements from an RDD in ascending order.
#'
#' @param rdd An RDD.
#' @param num Number of elements to return.
#' @return The first N elements from the RDD in ascending order.
#' @rdname takeOrdered
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' takeOrdered(rdd, 6L) # list(1, 2, 3, 4, 5, 6)
#'}
setGeneric("takeOrdered", function(rdd, num) { standardGeneric("takeOrdered") })

#' @rdname takeOrdered
#' @aliases takeOrdered,RDD,RDD-method
setMethod("takeOrdered",
          signature(rdd = "RDD", num = "integer"),
          function(rdd, num) {          
            takeOrderedElem(rdd, num)
          })

#' Returns the top N elements from an RDD.
#'
#' @param rdd An RDD.
#' @param num Number of elements to return.
#' @return The top N elements from the RDD.
#' @rdname top
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' top(rdd, 6L) # list(10, 9, 7, 6, 5, 4)
#'}
setGeneric("top", function(rdd, num) { standardGeneric("top") })

#' @rdname top
#' @aliases top,RDD,RDD-method
setMethod("top",
          signature(rdd = "RDD", num = "integer"),
          function(rdd, num) {          
            takeOrderedElem(rdd, num, FALSE)
          })

#' Fold an RDD using a given associative function and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using a given associative function and a neutral "zero value".
#' 
#' @param rdd An RDD.
#' @param zeroValue A neutral "zero value".
#' @param op An associative function for the folding operation.
#' @return The folding result.
#' @rdname fold
#' @seealso reduce
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5))
#' fold(rdd, 0, "+") # 15
#'}
setGeneric("fold", function(rdd, zeroValue, op) { standardGeneric("fold") })

#' @rdname fold
#' @aliases fold,RDD,RDD-method
setMethod("fold",
          signature(rdd = "RDD", zeroValue = "ANY", op = "ANY"),
          function(rdd, zeroValue, op) {
            aggregateRDD(rdd, zeroValue, op, op)
          })

#' Aggregate an RDD using the given combine functions and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using given combine functions and a neutral "zero value".
#' 
#' @param rdd An RDD.
#' @param zeroValue A neutral "zero value".
#' @param seqOp A function to aggregate the RDD elements. It may return a different
#'              result type from the type of the RDD elements.
#' @param combOp A function to aggregate results of seqOp.
#' @return The aggregation result.
#' @rdname aggregateRDD
#' @seealso reduce
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4))
#' zeroValue <- list(0, 0)
#' seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
#' combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
#' aggregateRDD(rdd, zeroValue, seqOp, combOp) # list(10, 4)
#'}
setGeneric("aggregateRDD", function(rdd, zeroValue, seqOp, combOp) { standardGeneric("aggregateRDD") })

#' @rdname aggregateRDD
#' @aliases aggregateRDD,RDD,RDD-method
setMethod("aggregateRDD",
          signature(rdd = "RDD", zeroValue = "ANY", seqOp = "ANY", combOp = "ANY"),
          function(rdd, zeroValue, seqOp, combOp) {        
            partitionFunc <- function(part) {
              Reduce(seqOp, part, zeroValue)
            }
            
            partitionList <- collect(lapplyPartition(rdd, partitionFunc),
                                     flatten = FALSE)
            Reduce(combOp, partitionList, zeroValue)
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
              union.rdd <- RDD(jrdd, x@env$serialized, colNames = x@colNames)
            } else {
              # One of the RDDs is not serialized, we need to serialize it first.
              if (x@env$serialized == "string") {
                x <- reserialize(x)
              } else if (y@env$serialized == "string") {
                y <- reserialize(y)
              }
              jrdd <- callJMethod(getJRDD(x), "union", getJRDD(y))
              union.rdd <- RDD(jrdd, "byte")
            }
            union.rdd
          })

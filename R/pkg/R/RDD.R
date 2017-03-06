#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# RDD in R implemented in S4 OO system.

setOldClass("jobj")

#' S4 class that represents an RDD
#'
#' RDD can be created using functions like
#'              \code{parallelize}, \code{textFile} etc.
#'
#' @rdname RDD
#' @seealso parallelize, textFile
#' @slot env An R environment that stores bookkeeping states of the RDD
#' @slot jrdd Java object reference to the backing JavaRDD
#' to an RDD
#' @noRd
setClass("RDD",
         slots = list(env = "environment",
                      jrdd = "jobj"))

setClass("PipelinedRDD",
         slots = list(prev = "RDD",
                      func = "function",
                      prev_jrdd = "jobj"),
         contains = "RDD")

setMethod("initialize", "RDD", function(.Object, jrdd, serializedMode,
                                        isCached, isCheckpointed) {
  # Check that RDD constructor is using the correct version of serializedMode
  stopifnot(class(serializedMode) == "character")
  stopifnot(serializedMode %in% c("byte", "string", "row"))
  # RDD has three serialization types:
  # byte: The RDD stores data serialized in R.
  # string: The RDD stores data as strings.
  # row: The RDD stores the serialized rows of a SparkDataFrame.

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
  .Object@env$serializedMode <- serializedMode

  .Object@jrdd <- jrdd
  .Object
})

setMethod("showRDD", "RDD",
          function(object) {
              cat(paste(callJMethod(getJRDD(object), "toString"), "\n", sep = ""))
          })

setMethod("initialize", "PipelinedRDD", function(.Object, prev, func, jrdd_val) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jrdd_val <- jrdd_val
  if (!is.null(jrdd_val)) {
    # This tracks the serialization mode for jrdd_val
    .Object@env$serializedMode <- prev@env$serializedMode
  }

  .Object@prev <- prev

  isPipelinable <- function(rdd) {
    e <- rdd@env
    # nolint start
    !(e$isCached || e$isCheckpointed)
    # nolint end
  }

  if (!inherits(prev, "PipelinedRDD") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- cleanClosure(func)
    .Object@prev_jrdd <- getJRDD(prev)
    .Object@env$prev_serializedMode <- prev@env$serializedMode
    # NOTE: We use prev_serializedMode to track the serialization mode of prev_JRDD
    # prev_serializedMode is used during the delayed computation of JRDD in getJRDD
  } else {
    pipelinedFunc <- function(partIndex, part) {
      f <- prev@func
      func(partIndex, f(partIndex, part))
    }
    .Object@func <- cleanClosure(pipelinedFunc)
    .Object@prev_jrdd <- prev@prev_jrdd # maintain the pipeline
    # Get the serialization mode of the parent RDD
    .Object@env$prev_serializedMode <- prev@env$prev_serializedMode
  }

  .Object
})

#' @rdname RDD
#' @noRd
#' @param jrdd Java object reference to the backing JavaRDD
#' @param serializedMode Use "byte" if the RDD stores data serialized in R, "string" if the RDD
#' stores strings, and "row" if the RDD stores the rows of a SparkDataFrame
#' @param isCached TRUE if the RDD is cached
#' @param isCheckpointed TRUE if the RDD has been checkpointed
RDD <- function(jrdd, serializedMode = "byte", isCached = FALSE,
                isCheckpointed = FALSE) {
  new("RDD", jrdd, serializedMode, isCached, isCheckpointed)
}

PipelinedRDD <- function(prev, func) {
  new("PipelinedRDD", prev, func, NULL)
}

# Return the serialization mode for an RDD.
setGeneric("getSerializedMode", function(rdd, ...) { standardGeneric("getSerializedMode") })
# For normal RDDs we can directly read the serializedMode
setMethod("getSerializedMode", signature(rdd = "RDD"), function(rdd) rdd@env$serializedMode )
# For pipelined RDDs if jrdd_val is set then serializedMode should exist
# if not we return the defaultSerialization mode of "byte" as we don't know the serialization
# mode at this point in time.
setMethod("getSerializedMode", signature(rdd = "PipelinedRDD"),
          function(rdd) {
            if (!is.null(rdd@env$jrdd_val)) {
              return(rdd@env$serializedMode)
            } else {
              return("byte")
            }
          })

# The jrdd accessor function.
setMethod("getJRDD", signature(rdd = "RDD"), function(rdd) rdd@jrdd )
setMethod("getJRDD", signature(rdd = "PipelinedRDD"),
          function(rdd, serializedMode = "byte") {
            if (!is.null(rdd@env$jrdd_val)) {
              return(rdd@env$jrdd_val)
            }

            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL)

            broadcastArr <- lapply(ls(.broadcastNames),
                                   function(name) { get(name, .broadcastNames) })

            serializedFuncArr <- serialize(rdd@func, connection = NULL)

            prev_jrdd <- rdd@prev_jrdd

            if (serializedMode == "string") {
              rddRef <- newJObject("org.apache.spark.api.r.StringRRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serializedMode,
                                   packageNamesArr,
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            } else {
              rddRef <- newJObject("org.apache.spark.api.r.RRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serializedMode,
                                   serializedMode,
                                   packageNamesArr,
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            }
            # Save the serialization flag after we create a RRDD
            rdd@env$serializedMode <- serializedMode
            rdd@env$jrdd_val <- callJMethod(rddRef, "asJavaRDD")
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
#' @param x The RDD to cache
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd)
#'}
#' @rdname cache-methods
#' @aliases cache,RDD-method
#' @noRd
setMethod("cacheRDD",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "cache")
            x@env$isCached <- TRUE
            x
          })

#' Persist an RDD
#'
#' Persist this RDD with the specified storage level. For details of the
#' supported storage levels, refer to
#'\url{http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence}.
#'
#' @param x The RDD to persist
#' @param newLevel The new storage level to be assigned
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' persistRDD(rdd, "MEMORY_AND_DISK")
#'}
#' @rdname persist
#' @aliases persist,RDD-method
#' @noRd
setMethod("persistRDD",
          signature(x = "RDD", newLevel = "character"),
          function(x, newLevel = "MEMORY_ONLY") {
            callJMethod(getJRDD(x), "persist", getStorageLevel(newLevel))
            x@env$isCached <- TRUE
            x
          })

#' Unpersist an RDD
#'
#' Mark the RDD as non-persistent, and remove all blocks for it from memory and
#' disk.
#'
#' @param x The RDD to unpersist
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' cache(rdd) # rdd@@env$isCached == TRUE
#' unpersistRDD(rdd) # rdd@@env$isCached == FALSE
#'}
#' @rdname unpersist
#' @aliases unpersist,RDD-method
#' @noRd
setMethod("unpersistRDD",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "unpersist")
            x@env$isCached <- FALSE
            x
          })

#' Checkpoint an RDD
#'
#' Mark this RDD for checkpointing. It will be saved to a file inside the
#' checkpoint directory set with setCheckpointDir() and all references to its
#' parent RDDs will be removed. This function must be called before any job has
#' been executed on this RDD. It is strongly recommended that this RDD is
#' persisted in memory, otherwise saving it on a file will require recomputation.
#'
#' @param x The RDD to checkpoint
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' setCheckpointDir(sc, "checkpoint")
#' rdd <- parallelize(sc, 1:10, 2L)
#' checkpoint(rdd)
#'}
#' @rdname checkpoint-methods
#' @aliases checkpoint,RDD-method
#' @noRd
setMethod("checkpoint",
          signature(x = "RDD"),
          function(x) {
            jrdd <- getJRDD(x)
            callJMethod(jrdd, "checkpoint")
            x@env$isCheckpointed <- TRUE
            x
          })

#' Gets the number of partitions of an RDD
#'
#' @param x A RDD.
#' @return the number of partitions of rdd as an integer.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' getNumPartitions(rdd)  # 2L
#'}
#' @rdname getNumPartitions
#' @aliases getNumPartitions,RDD-method
#' @noRd
setMethod("getNumPartitionsRDD",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "getNumPartitions")
          })

#' Gets the number of partitions of an RDD, the same as getNumPartitions.
#' But this function has been deprecated, please use getNumPartitions.
#'
#' @rdname getNumPartitions
#' @aliases numPartitions,RDD-method
#' @noRd
setMethod("numPartitions",
          signature(x = "RDD"),
          function(x) {
            .Deprecated("getNumPartitions")
            getNumPartitionsRDD(x)
          })

#' Collect elements of an RDD
#'
#' @description
#' \code{collect} returns a list that contains all of the elements in this RDD.
#'
#' @param x The RDD to collect
#' @param ... Other optional arguments to collect
#' @param flatten FALSE if the list should not flattened
#' @return a list containing elements in the RDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 2L)
#' collectRDD(rdd) # list from 1 to 10
#' collectPartition(rdd, 0L) # list from 1 to 5
#'}
#' @rdname collect-methods
#' @aliases collect,RDD-method
#' @noRd
setMethod("collectRDD",
          signature(x = "RDD"),
          function(x, flatten = TRUE) {
            # Assumes a pairwise RDD is backed by a JavaPairRDD.
            collected <- callJMethod(getJRDD(x), "collect")
            convertJListToRList(collected, flatten,
              serializedMode = getSerializedMode(x))
          })


#' @description
#' \code{collectPartition} returns a list that contains all of the elements
#' in the specified partition of the RDD.
#' @param partitionId the partition to collect (starts from 0)
#' @rdname collect-methods
#' @aliases collectPartition,integer,RDD-method
#' @noRd
setMethod("collectPartition",
          signature(x = "RDD", partitionId = "integer"),
          function(x, partitionId) {
            jPartitionsList <- callJMethod(getJRDD(x),
                                           "collectPartitions",
                                           as.list(as.integer(partitionId)))

            jList <- jPartitionsList[[1]]
            convertJListToRList(jList, flatten = TRUE,
              serializedMode = getSerializedMode(x))
          })

#' @description
#' \code{collectAsMap} returns a named list as a map that contains all of the elements
#' in a key-value pair RDD.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(list(1, 2), list(3, 4)), 2L)
#' collectAsMap(rdd) # list(`1` = 2, `3` = 4)
#'}
# nolint end
#' @rdname collect-methods
#' @aliases collectAsMap,RDD-method
#' @noRd
setMethod("collectAsMap",
          signature(x = "RDD"),
          function(x) {
            pairList <- collectRDD(x)
            map <- new.env()
            lapply(pairList, function(i) { assign(as.character(i[[1]]), i[[2]], envir = map) })
            as.list(map)
          })

#' Return the number of elements in the RDD.
#'
#' @param x The RDD to count
#' @return number of elements in the RDD.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' countRDD(rdd) # 10
#' length(rdd) # Same as count
#'}
#' @rdname count
#' @aliases count,RDD-method
#' @noRd
setMethod("countRDD",
          signature(x = "RDD"),
          function(x) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            valsRDD <- lapplyPartition(x, countPartition)
            vals <- collectRDD(valsRDD)
            sum(as.integer(vals))
          })

#' Return the number of elements in the RDD
#' @rdname count
#' @noRd
setMethod("lengthRDD",
          signature(x = "RDD"),
          function(x) {
            countRDD(x)
          })

#' Return the count of each unique value in this RDD as a list of
#' (value, count) pairs.
#'
#' Same as countByValue in Spark.
#'
#' @param x The RDD to count
#' @return list of (value, count) pairs, where count is number of each unique
#' value in rdd.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,3,2,1))
#' countByValue(rdd) # (1,2L), (2,2L), (3,1L)
#'}
# nolint end
#' @rdname countByValue
#' @aliases countByValue,RDD-method
#' @noRd
setMethod("countByValue",
          signature(x = "RDD"),
          function(x) {
            ones <- lapply(x, function(item) { list(item, 1L) })
            collectRDD(reduceByKey(ones, `+`, getNumPartitionsRDD(x)))
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
#' @noRd
#' @aliases lapply
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- lapply(rdd, function(x) { x * 2 })
#' collectRDD(multiplyByTwo) # 2,4,6...
#'}
setMethod("lapply",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            func <- function(partIndex, part) {
              lapply(part, FUN)
            }
            lapplyPartitionsWithIndex(X, func)
          })

#' @rdname lapply
#' @aliases map,RDD,function-method
#' @noRd
setMethod("map",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

#' Flatten results after applying a function to all elements
#'
#' This function returns a new RDD by first applying a function to all
#' elements of this RDD, and then flattening the results.
#'
#' @param X The RDD to apply the transformation.
#' @param FUN the transformation to apply on each element
#' @return a new RDD created by the transformation.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' multiplyByTwo <- flatMap(rdd, function(x) { list(x*2, x*10) })
#' collectRDD(multiplyByTwo) # 2,20,4,40,6,60...
#'}
#' @rdname flatMap
#' @aliases flatMap,RDD,function-method
#' @noRd
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
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' partitionSum <- lapplyPartition(rdd, function(part) { Reduce("+", part) })
#' collectRDD(partitionSum) # 15, 40
#'}
#' @rdname lapplyPartition
#' @aliases lapplyPartition,RDD,function-method
#' @noRd
setMethod("lapplyPartition",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })

#' mapPartitions is the same as lapplyPartition.
#'
#' @rdname lapplyPartition
#' @aliases mapPartitions,RDD,function-method
#' @noRd
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
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 5L)
#' prod <- lapplyPartitionsWithIndex(rdd, function(partIndex, part) {
#'                                          partIndex * Reduce("+", part) })
#' collectRDD(prod, flatten = FALSE) # 0, 7, 22, 45, 76
#'}
#' @rdname lapplyPartitionsWithIndex
#' @aliases lapplyPartitionsWithIndex,RDD,function-method
#' @noRd
setMethod("lapplyPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            PipelinedRDD(X, FUN)
          })

#' @rdname lapplyPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,RDD,function-method
#' @noRd
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
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' unlist(collectRDD(filterRDD(rdd, function (x) { x < 3 }))) # c(1, 2)
#'}
# nolint end
#' @rdname filterRDD
#' @aliases filterRDD,RDD,function-method
#' @noRd
setMethod("filterRDD",
          signature(x = "RDD", f = "function"),
          function(x, f) {
            filter.func <- function(part) {
              Filter(f, part)
            }
            lapplyPartition(x, filter.func)
          })

#' @rdname filterRDD
#' @aliases Filter
#' @noRd
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
#' @param x The RDD to reduce
#' @param func Commutative and associative function to apply on elements
#'             of the RDD.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' reduce(rdd, "+") # 55
#'}
#' @rdname reduce
#' @aliases reduce,RDD,ANY-method
#' @noRd
setMethod("reduce",
          signature(x = "RDD", func = "ANY"),
          function(x, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collectRDD(lapplyPartition(x, reducePartition),
                                     flatten = FALSE)
            Reduce(func, partitionList)
          })

#' Get the maximum element of an RDD.
#'
#' @param x The RDD to get the maximum element from
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' maximum(rdd) # 10
#'}
#' @rdname maximum
#' @aliases maximum,RDD
#' @noRd
setMethod("maximum",
          signature(x = "RDD"),
          function(x) {
            reduce(x, max)
          })

#' Get the minimum element of an RDD.
#'
#' @param x The RDD to get the minimum element from
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' minimum(rdd) # 1
#'}
#' @rdname minimum
#' @aliases minimum,RDD
#' @noRd
setMethod("minimum",
          signature(x = "RDD"),
          function(x) {
            reduce(x, min)
          })

#' Add up the elements in an RDD.
#'
#' @param x The RDD to add up the elements in
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' sumRDD(rdd) # 55
#'}
#' @rdname sumRDD
#' @aliases sumRDD,RDD
#' @noRd
setMethod("sumRDD",
          signature(x = "RDD"),
          function(x) {
            reduce(x, "+")
          })

#' Applies a function to all elements in an RDD, and forces evaluation.
#'
#' @param x The RDD to apply the function
#' @param func The function to be applied.
#' @return invisible NULL.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreach(rdd, function(x) { save(x, file=...) })
#'}
#' @rdname foreach
#' @aliases foreach,RDD,function-method
#' @noRd
setMethod("foreach",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            partition.func <- function(x) {
              lapply(x, func)
              NULL
            }
            invisible(collectRDD(mapPartitions(x, partition.func)))
          })

#' Applies a function to each partition in an RDD, and forces evaluation.
#'
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreachPartition(rdd, function(part) { save(part, file=...); NULL })
#'}
#' @rdname foreach
#' @aliases foreachPartition,RDD,function-method
#' @noRd
setMethod("foreachPartition",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            invisible(collectRDD(mapPartitions(x, func)))
          })

#' Take elements from an RDD.
#'
#' This function takes the first NUM elements in the RDD and
#' returns them in a list.
#'
#' @param x The RDD to take elements from
#' @param num Number of elements to take
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' takeRDD(rdd, 2L) # list(1, 2)
#'}
# nolint end
#' @rdname take
#' @aliases take,RDD,numeric-method
#' @noRd
setMethod("takeRDD",
          signature(x = "RDD", num = "numeric"),
          function(x, num) {
            resList <- list()
            index <- -1
            jrdd <- getJRDD(x)
            numPartitions <- getNumPartitionsRDD(x)
            serializedModeRDD <- getSerializedMode(x)

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
                                           serializedMode = serializedModeRDD)

              resList <- append(resList, elems)
            }
            resList
          })


#' First
#'
#' Return the first element of an RDD
#'
#' @rdname first
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' firstRDD(rdd)
#' }
#' @noRd
setMethod("firstRDD",
          signature(x = "RDD"),
          function(x) {
            takeRDD(x, 1)[[1]]
          })

#' Removes the duplicates from RDD.
#'
#' This function returns a new RDD containing the distinct elements in the
#' given RDD. The same as `distinct()' in Spark.
#'
#' @param x The RDD to remove duplicates from.
#' @param numPartitions Number of partitions to create.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, c(1,2,2,3,3,3))
#' sort(unlist(collectRDD(distinctRDD(rdd)))) # c(1, 2, 3)
#'}
# nolint end
#' @rdname distinct
#' @aliases distinct,RDD-method
#' @noRd
setMethod("distinctRDD",
          signature(x = "RDD"),
          function(x, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            identical.mapped <- lapply(x, function(x) { list(x, NULL) })
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
#' @param x The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param fraction The (rough) sample target fraction
#' @param seed Randomness seed value
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' collectRDD(sampleRDD(rdd, FALSE, 0.5, 1618L)) # ~5 distinct elements
#' collectRDD(sampleRDD(rdd, TRUE, 0.5, 9L)) # ~5 elements possibly with duplicates
#'}
#' @rdname sampleRDD
#' @aliases sampleRDD,RDD
#' @noRd
setMethod("sampleRDD",
          signature(x = "RDD", withReplacement = "logical",
                    fraction = "numeric", seed = "integer"),
          function(x, withReplacement, fraction, seed) {

            # The sampler: takes a partition and returns its sampled version.
            samplingFunc <- function(partIndex, part) {
              set.seed(seed)
              res <- vector("list", length(part))
              len <- 0

              # Discards some random values to ensure each partition has a
              # different random seed.
              stats::runif(partIndex)

              for (elem in part) {
                if (withReplacement) {
                  count <- stats::rpois(1, fraction)
                  if (count > 0) {
                    res[ (len + 1) : (len + count) ] <- rep(list(elem), count)
                    len <- len + count
                  }
                } else {
                  if (stats::runif(1) < fraction) {
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

            lapplyPartitionsWithIndex(x, samplingFunc)
          })

#' Return a list of the elements that are a sampled subset of the given RDD.
#'
#' @param x The RDD to sample elements from
#' @param withReplacement Sampling with replacement or not
#' @param num Number of elements to return
#' @param seed Randomness seed value
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:100)
#' # exactly 5 elements sampled, which may not be distinct
#' takeSample(rdd, TRUE, 5L, 1618L)
#' # exactly 5 distinct elements sampled
#' takeSample(rdd, FALSE, 5L, 16181618L)
#'}
#' @rdname takeSample
#' @aliases takeSample,RDD
#' @noRd
setMethod("takeSample", signature(x = "RDD", withReplacement = "logical",
                                  num = "integer", seed = "integer"),
          function(x, withReplacement, num, seed) {
            # This function is ported from RDD.scala.
            fraction <- 0.0
            total <- 0
            multiplier <- 3.0
            initialCount <- countRDD(x)
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
            samples <- collectRDD(sampleRDD(x, withReplacement, fraction,
                                         as.integer(ceiling(stats::runif(1,
                                                                  -MAXINT,
                                                                  MAXINT)))))
            # If the first sample didn't turn out large enough, keep trying to
            # take samples; this shouldn't happen often because we use a big
            # multiplier for thei initial size
            while (length(samples) < total)
              samples <- collectRDD(sampleRDD(x, withReplacement, fraction,
                                           as.integer(ceiling(stats::runif(1,
                                                                    -MAXINT,
                                                                    MAXINT)))))

            # TODO(zongheng): investigate if this call is an in-place shuffle?
            base::sample(samples)[1:total]
          })

#' Creates tuples of the elements in this RDD by applying a function.
#'
#' @param x The RDD.
#' @param func The function to be applied.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3))
#' collectRDD(keyBy(rdd, function(x) { x*x })) # list(list(1, 1), list(4, 2), list(9, 3))
#'}
# nolint end
#' @rdname keyBy
#' @aliases keyBy,RDD
#' @noRd
setMethod("keyBy",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            apply.func <- function(x) {
              list(func(x), x)
            }
            lapply(x, apply.func)
          })

#' Return a new RDD that has exactly numPartitions partitions.
#' Can increase or decrease the level of parallelism in this RDD. Internally,
#' this uses a shuffle to redistribute data.
#' If you are decreasing the number of partitions in this RDD, consider using
#' coalesce, which can avoid performing a shuffle.
#'
#' @param x The RDD.
#' @param numPartitions Number of partitions to create.
#' @seealso coalesce
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5, 6, 7), 4L)
#' getNumPartitions(rdd)                   # 4
#' getNumPartitions(repartitionRDD(rdd, 2L))  # 2
#'}
#' @rdname repartition
#' @aliases repartition,RDD
#' @noRd
setMethod("repartitionRDD",
          signature(x = "RDD"),
          function(x, numPartitions) {
            if (!is.null(numPartitions) && is.numeric(numPartitions)) {
              coalesceRDD(x, numPartitions, TRUE)
            } else {
              stop("Please, specify the number of partitions")
            }
          })

#' Return a new RDD that is reduced into numPartitions partitions.
#'
#' @param x The RDD.
#' @param numPartitions Number of partitions to create.
#' @seealso repartition
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5), 3L)
#' getNumPartitions(rdd)               # 3
#' getNumPartitions(coalesce(rdd, 1L)) # 1
#'}
#' @rdname coalesce
#' @aliases coalesce,RDD
#' @noRd
setMethod("coalesceRDD",
           signature(x = "RDD", numPartitions = "numeric"),
           function(x, numPartitions, shuffle = FALSE) {
             numPartitions <- numToInt(numPartitions)
             if (shuffle || numPartitions > SparkR:::getNumPartitionsRDD(x)) {
               func <- function(partIndex, part) {
                 set.seed(partIndex)  # partIndex as seed
                 start <- as.integer(base::sample(numPartitions, 1) - 1)
                 lapply(seq_along(part),
                        function(i) {
                          pos <- (start + i) %% numPartitions
                          list(pos, part[[i]])
                        })
               }
               shuffled <- lapplyPartitionsWithIndex(x, func)
               repartitioned <- partitionByRDD(shuffled, numPartitions)
               values(repartitioned)
             } else {
               jrdd <- callJMethod(getJRDD(x), "coalesce", numPartitions, shuffle)
               RDD(jrdd)
             }
           })

#' Save this RDD as a SequenceFile of serialized objects.
#'
#' @param x The RDD to save
#' @param path The directory where the file is saved
#' @seealso objectFile
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsObjectFile(rdd, "/tmp/sparkR-tmp")
#'}
#' @rdname saveAsObjectFile
#' @aliases saveAsObjectFile,RDD
#' @noRd
setMethod("saveAsObjectFile",
          signature(x = "RDD", path = "character"),
          function(x, path) {
            # If serializedMode == "string" we need to serialize the data before saving it since
            # objectFile() assumes serializedMode == "byte".
            if (getSerializedMode(x) != "byte") {
              x <- serializeToBytes(x)
            }
            # Return nothing
            invisible(callJMethod(getJRDD(x), "saveAsObjectFile", path))
          })

#' Save this RDD as a text file, using string representations of elements.
#'
#' @param x The RDD to save
#' @param path The directory where the partitions of the text file are saved
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' saveAsTextFile(rdd, "/tmp/sparkR-tmp")
#'}
#' @rdname saveAsTextFile
#' @aliases saveAsTextFile,RDD
#' @noRd
setMethod("saveAsTextFile",
          signature(x = "RDD", path = "character"),
          function(x, path) {
            func <- function(str) {
              toString(str)
            }
            stringRdd <- lapply(x, func)
            # Return nothing
            invisible(
              callJMethod(getJRDD(stringRdd, serializedMode = "string"), "saveAsTextFile", path))
          })

#' Sort an RDD by the given key function.
#'
#' @param x An RDD to be sorted.
#' @param func A function used to compute the sort key for each element.
#' @param ascending A flag to indicate whether the sorting is ascending or descending.
#' @param numPartitions Number of partitions to create.
#' @return An RDD where all elements are sorted.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(3, 2, 1))
#' collectRDD(sortBy(rdd, function(x) { x })) # list (1, 2, 3)
#'}
# nolint end
#' @rdname sortBy
#' @aliases sortBy,RDD,RDD-method
#' @noRd
setMethod("sortBy",
          signature(x = "RDD", func = "function"),
          function(x, func, ascending = TRUE, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            values(sortByKey(keyBy(x, func), ascending, numPartitions))
          })

# Helper function to get first N elements from an RDD in the specified order.
# Param:
#   x An RDD.
#   num Number of elements to return.
#   ascending A flag to indicate whether the sorting is ascending or descending.
# Return:
#   A list of the first N elements from the RDD in the specified order.
#
takeOrderedElem <- function(x, num, ascending = TRUE) {
  if (num <= 0L) {
    return(list())
  }

  partitionFunc <- function(part) {
    if (num < length(part)) {
      # R limitation: order works only on primitive types!
      ord <- order(unlist(part, recursive = FALSE), decreasing = !ascending)
      part[ord[1:num]]
    } else {
      part
    }
  }

  newRdd <- mapPartitions(x, partitionFunc)

  resList <- list()
  index <- -1
  jrdd <- getJRDD(newRdd)
  numPartitions <- getNumPartitionsRDD(newRdd)
  serializedModeRDD <- getSerializedMode(newRdd)

  while (TRUE) {
    index <- index + 1

    if (index >= numPartitions) {
      ord <- order(unlist(resList, recursive = FALSE), decreasing = !ascending)
      resList <- resList[ord[1:num]]
      break
    }

    # a JList of byte arrays
    partitionArr <- callJMethod(jrdd, "collectPartitions", as.list(as.integer(index)))
    partition <- partitionArr[[1]]

    # elems is capped to have at most `num` elements
    elems <- convertJListToRList(partition,
                                 flatten = TRUE,
                                 logicalUpperBound = num,
                                 serializedMode = serializedModeRDD)

    resList <- append(resList, elems)
  }
  resList
}

#' Returns the first N elements from an RDD in ascending order.
#'
#' @param x An RDD.
#' @param num Number of elements to return.
#' @return The first N elements from the RDD in ascending order.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' takeOrdered(rdd, 6L) # list(1, 2, 3, 4, 5, 6)
#'}
# nolint end
#' @rdname takeOrdered
#' @aliases takeOrdered,RDD,RDD-method
#' @noRd
setMethod("takeOrdered",
          signature(x = "RDD", num = "integer"),
          function(x, num) {
            takeOrderedElem(x, num)
          })

#' Returns the top N elements from an RDD.
#'
#' @param x An RDD.
#' @param num Number of elements to return.
#' @return The top N elements from the RDD.
#' @rdname top
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(10, 1, 2, 9, 3, 4, 5, 6, 7))
#' top(rdd, 6L) # list(10, 9, 7, 6, 5, 4)
#'}
# nolint end
#' @aliases top,RDD,RDD-method
#' @noRd
setMethod("top",
          signature(x = "RDD", num = "integer"),
          function(x, num) {
            takeOrderedElem(x, num, FALSE)
          })

#' Fold an RDD using a given associative function and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using a given associative function and a neutral "zero value".
#'
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param op An associative function for the folding operation.
#' @return The folding result.
#' @rdname fold
#' @seealso reduce
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4, 5))
#' fold(rdd, 0, "+") # 15
#'}
#' @aliases fold,RDD,RDD-method
#' @noRd
setMethod("fold",
          signature(x = "RDD", zeroValue = "ANY", op = "ANY"),
          function(x, zeroValue, op) {
            aggregateRDD(x, zeroValue, op, op)
          })

#' Aggregate an RDD using the given combine functions and a neutral "zero value".
#'
#' Aggregate the elements of each partition, and then the results for all the
#' partitions, using given combine functions and a neutral "zero value".
#'
#' @param x An RDD.
#' @param zeroValue A neutral "zero value".
#' @param seqOp A function to aggregate the RDD elements. It may return a different
#'              result type from the type of the RDD elements.
#' @param combOp A function to aggregate results of seqOp.
#' @return The aggregation result.
#' @rdname aggregateRDD
#' @seealso reduce
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1, 2, 3, 4))
#' zeroValue <- list(0, 0)
#' seqOp <- function(x, y) { list(x[[1]] + y, x[[2]] + 1) }
#' combOp <- function(x, y) { list(x[[1]] + y[[1]], x[[2]] + y[[2]]) }
#' aggregateRDD(rdd, zeroValue, seqOp, combOp) # list(10, 4)
#'}
# nolint end
#' @aliases aggregateRDD,RDD,RDD-method
#' @noRd
setMethod("aggregateRDD",
          signature(x = "RDD", zeroValue = "ANY", seqOp = "ANY", combOp = "ANY"),
          function(x, zeroValue, seqOp, combOp) {
            partitionFunc <- function(part) {
              Reduce(seqOp, part, zeroValue)
            }

            partitionList <- collectRDD(lapplyPartition(x, partitionFunc),
                                     flatten = FALSE)
            Reduce(combOp, partitionList, zeroValue)
          })

#' Pipes elements to a forked external process.
#'
#' The same as 'pipe()' in Spark.
#'
#' @param x The RDD whose elements are piped to the forked external process.
#' @param command The command to fork an external process.
#' @param env A named list to set environment variables of the external process.
#' @return A new RDD created by piping all elements to a forked external process.
#' @rdname pipeRDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' pipeRDD(rdd, "more")
#' Output: c("1", "2", ..., "10")
#'}
#' @aliases pipeRDD,RDD,character-method
#' @noRd
setMethod("pipeRDD",
          signature(x = "RDD", command = "character"),
          function(x, command, env = list()) {
            func <- function(part) {
              trim_trailing_func <- function(x) {
                sub("[\r\n]*$", "", toString(x))
              }
              input <- unlist(lapply(part, trim_trailing_func))
              res <- system2(command, stdout = TRUE, input = input, env = env)
              lapply(res, trim_trailing_func)
            }
            lapplyPartition(x, func)
          })

#' TODO: Consider caching the name in the RDD's environment
#' Return an RDD's name.
#'
#' @param x The RDD whose name is returned.
#' @rdname name
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' name(rdd) # NULL (if not set before)
#'}
#' @aliases name,RDD
#' @noRd
setMethod("name",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "name")
          })

#' Set an RDD's name.
#'
#' @param x The RDD whose name is to be set.
#' @param name The RDD name to be set.
#' @return a new RDD renamed.
#' @rdname setName
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list(1,2,3))
#' setName(rdd, "myRDD")
#' name(rdd) # "myRDD"
#'}
#' @aliases setName,RDD
#' @noRd
setMethod("setName",
          signature(x = "RDD", name = "character"),
          function(x, name) {
            callJMethod(getJRDD(x), "setName", name)
            x
          })

#' Zip an RDD with generated unique Long IDs.
#'
#' Items in the kth partition will get ids k, n+k, 2*n+k, ..., where
#' n is the number of partitions. So there may exist gaps, but this
#' method won't trigger a spark job, which is different from
#' zipWithIndex.
#'
#' @param x An RDD to be zipped.
#' @return An RDD with zipped items.
#' @seealso zipWithIndex
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
#' collectRDD(zipWithUniqueId(rdd))
#' # list(list("a", 0), list("b", 3), list("c", 1), list("d", 4), list("e", 2))
#'}
# nolint end
#' @rdname zipWithUniqueId
#' @aliases zipWithUniqueId,RDD
#' @noRd
setMethod("zipWithUniqueId",
          signature(x = "RDD"),
          function(x) {
            n <- getNumPartitionsRDD(x)

            partitionFunc <- function(partIndex, part) {
              mapply(
                function(item, index) {
                  list(item, (index - 1) * n + partIndex)
                },
                part,
                seq_along(part),
                SIMPLIFY = FALSE)
            }

            lapplyPartitionsWithIndex(x, partitionFunc)
          })

#' Zip an RDD with its element indices.
#'
#' The ordering is first based on the partition index and then the
#' ordering of items within each partition. So the first item in
#' the first partition gets index 0, and the last item in the last
#' partition receives the largest index.
#'
#' This method needs to trigger a Spark job when this RDD contains
#' more than one partition.
#'
#' @param x An RDD to be zipped.
#' @return An RDD with zipped items.
#' @seealso zipWithUniqueId
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, list("a", "b", "c", "d", "e"), 3L)
#' collectRDD(zipWithIndex(rdd))
#' # list(list("a", 0), list("b", 1), list("c", 2), list("d", 3), list("e", 4))
#'}
# nolint end
#' @rdname zipWithIndex
#' @aliases zipWithIndex,RDD
#' @noRd
setMethod("zipWithIndex",
          signature(x = "RDD"),
          function(x) {
            n <- getNumPartitionsRDD(x)
            if (n > 1) {
              nums <- collectRDD(lapplyPartition(x,
                                              function(part) {
                                                list(length(part))
                                              }))
              startIndices <- Reduce("+", nums, accumulate = TRUE)
            }

            partitionFunc <- function(partIndex, part) {
              if (partIndex == 0) {
                startIndex <- 0
              } else {
                startIndex <- startIndices[[partIndex]]
              }

              mapply(
                function(item, index) {
                  list(item, index - 1 + startIndex)
                },
                part,
                seq_along(part),
                SIMPLIFY = FALSE)
           }

           lapplyPartitionsWithIndex(x, partitionFunc)
         })

#' Coalesce all elements within each partition of an RDD into a list.
#'
#' @param x An RDD.
#' @return An RDD created by coalescing all elements within
#'         each partition into a list.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, as.list(1:4), 2L)
#' collectRDD(glom(rdd))
#' # list(list(1, 2), list(3, 4))
#'}
# nolint end
#' @rdname glom
#' @aliases glom,RDD
#' @noRd
setMethod("glom",
          signature(x = "RDD"),
          function(x) {
            partitionFunc <- function(part) {
              list(part)
            }

            lapplyPartition(x, partitionFunc)
          })

############ Binary Functions #############

#' Return the union RDD of two RDDs.
#' The same as union() in Spark.
#'
#' @param x An RDD.
#' @param y An RDD.
#' @return a new RDD created by performing the simple union (witout removing
#' duplicates) of two input RDDs.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:3)
#' unionRDD(rdd, rdd) # 1, 2, 3, 1, 2, 3
#'}
#' @rdname unionRDD
#' @aliases unionRDD,RDD,RDD-method
#' @noRd
setMethod("unionRDD",
          signature(x = "RDD", y = "RDD"),
          function(x, y) {
            if (getSerializedMode(x) == getSerializedMode(y)) {
              jrdd <- callJMethod(getJRDD(x), "union", getJRDD(y))
              union.rdd <- RDD(jrdd, getSerializedMode(x))
            } else {
              # One of the RDDs is not serialized, we need to serialize it first.
              if (getSerializedMode(x) != "byte") x <- serializeToBytes(x)
              if (getSerializedMode(y) != "byte") y <- serializeToBytes(y)
              jrdd <- callJMethod(getJRDD(x), "union", getJRDD(y))
              union.rdd <- RDD(jrdd, "byte")
            }
            union.rdd
          })

#' Zip an RDD with another RDD.
#'
#' Zips this RDD with another one, returning key-value pairs with the
#' first element in each RDD second element in each RDD, etc. Assumes
#' that the two RDDs have the same number of partitions and the same
#' number of elements in each partition (e.g. one was made through
#' a map on the other).
#'
#' @param x An RDD to be zipped.
#' @param other Another RDD to be zipped.
#' @return An RDD zipped from the two RDDs.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, 0:4)
#' rdd2 <- parallelize(sc, 1000:1004)
#' collectRDD(zipRDD(rdd1, rdd2))
#' # list(list(0, 1000), list(1, 1001), list(2, 1002), list(3, 1003), list(4, 1004))
#'}
# nolint end
#' @rdname zipRDD
#' @aliases zipRDD,RDD
#' @noRd
setMethod("zipRDD",
          signature(x = "RDD", other = "RDD"),
          function(x, other) {
            n1 <- getNumPartitionsRDD(x)
            n2 <- getNumPartitionsRDD(other)
            if (n1 != n2) {
              stop("Can only zip RDDs which have the same number of partitions.")
            }

            rdds <- appendPartitionLengths(x, other)
            jrdd <- callJMethod(getJRDD(rdds[[1]]), "zip", getJRDD(rdds[[2]]))
            # The jrdd's elements are of scala Tuple2 type. The serialized
            # flag here is used for the elements inside the tuples.
            rdd <- RDD(jrdd, getSerializedMode(rdds[[1]]))

            mergePartitions(rdd, TRUE)
          })

#' Cartesian product of this RDD and another one.
#'
#' Return the Cartesian product of this RDD and another one,
#' that is, the RDD of all pairs of elements (a, b) where a
#' is in this and b is in other.
#'
#' @param x An RDD.
#' @param other An RDD.
#' @return A new RDD which is the Cartesian product of these two RDDs.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:2)
#' sortByKey(cartesian(rdd, rdd))
#' # list(list(1, 1), list(1, 2), list(2, 1), list(2, 2))
#'}
# nolint end
#' @rdname cartesian
#' @aliases cartesian,RDD,RDD-method
#' @noRd
setMethod("cartesian",
          signature(x = "RDD", other = "RDD"),
          function(x, other) {
            rdds <- appendPartitionLengths(x, other)
            jrdd <- callJMethod(getJRDD(rdds[[1]]), "cartesian", getJRDD(rdds[[2]]))
            # The jrdd's elements are of scala Tuple2 type. The serialized
            # flag here is used for the elements inside the tuples.
            rdd <- RDD(jrdd, getSerializedMode(rdds[[1]]))

            mergePartitions(rdd, FALSE)
          })

#' Subtract an RDD with another RDD.
#'
#' Return an RDD with the elements from this that are not in other.
#'
#' @param x An RDD.
#' @param other An RDD.
#' @param numPartitions Number of the partitions in the result RDD.
#' @return An RDD with the elements from this that are not in other.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(1, 1, 2, 2, 3, 4))
#' rdd2 <- parallelize(sc, list(2, 4))
#' collectRDD(subtract(rdd1, rdd2))
#' # list(1, 1, 3)
#'}
# nolint end
#' @rdname subtract
#' @aliases subtract,RDD
#' @noRd
setMethod("subtract",
          signature(x = "RDD", other = "RDD"),
          function(x, other, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            mapFunction <- function(e) { list(e, NA) }
            rdd1 <- map(x, mapFunction)
            rdd2 <- map(other, mapFunction)
            keys(subtractByKey(rdd1, rdd2, numPartitions))
          })

#' Intersection of this RDD and another one.
#'
#' Return the intersection of this RDD and another one.
#' The output will not contain any duplicate elements,
#' even if the input RDDs did. Performs a hash partition
#' across the cluster.
#' Note that this method performs a shuffle internally.
#'
#' @param x An RDD.
#' @param other An RDD.
#' @param numPartitions The number of partitions in the result RDD.
#' @return An RDD which is the intersection of these two RDDs.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, list(1, 10, 2, 3, 4, 5))
#' rdd2 <- parallelize(sc, list(1, 6, 2, 3, 7, 8))
#' collectRDD(sortBy(intersection(rdd1, rdd2), function(x) { x }))
#' # list(1, 2, 3)
#'}
# nolint end
#' @rdname intersection
#' @aliases intersection,RDD
#' @noRd
setMethod("intersection",
          signature(x = "RDD", other = "RDD"),
          function(x, other, numPartitions = SparkR:::getNumPartitionsRDD(x)) {
            rdd1 <- map(x, function(v) { list(v, NA) })
            rdd2 <- map(other, function(v) { list(v, NA) })

            filterFunction <- function(elem) {
              iters <- elem[[2]]
              all(as.vector(
                lapply(iters, function(iter) { length(iter) > 0 }), mode = "logical"))
            }

            keys(filterRDD(cogroup(rdd1, rdd2, numPartitions = numPartitions), filterFunction))
          })

#' Zips an RDD's partitions with one (or more) RDD(s).
#' Same as zipPartitions in Spark.
#'
#' @param ... RDDs to be zipped.
#' @param func A function to transform zipped partitions.
#' @return A new RDD by applying a function to the zipped partitions.
#'         Assumes that all the RDDs have the *same number of partitions*, but
#'         does *not* require them to have the same number of elements in each partition.
#' @examples
# nolint start
#'\dontrun{
#' sc <- sparkR.init()
#' rdd1 <- parallelize(sc, 1:2, 2L)  # 1, 2
#' rdd2 <- parallelize(sc, 1:4, 2L)  # 1:2, 3:4
#' rdd3 <- parallelize(sc, 1:6, 2L)  # 1:3, 4:6
#' collectRDD(zipPartitions(rdd1, rdd2, rdd3,
#'                       func = function(x, y, z) { list(list(x, y, z))} ))
#' # list(list(1, c(1,2), c(1,2,3)), list(2, c(3,4), c(4,5,6)))
#'}
# nolint end
#' @rdname zipRDD
#' @aliases zipPartitions,RDD
#' @noRd
setMethod("zipPartitions",
          "RDD",
          function(..., func) {
            rrdds <- list(...)
            if (length(rrdds) == 1) {
              return(rrdds[[1]])
            }
            nPart <- sapply(rrdds, getNumPartitionsRDD)
            if (length(unique(nPart)) != 1) {
              stop("Can only zipPartitions RDDs which have the same number of partitions.")
            }

            rrdds <- lapply(rrdds, function(rdd) {
              mapPartitionsWithIndex(rdd, function(partIndex, part) {
                print(length(part))
                list(list(partIndex, part))
              })
            })
            union.rdd <- Reduce(unionRDD, rrdds)
            zipped.rdd <- values(groupByKey(union.rdd, numPartitions = nPart[1]))
            res <- mapPartitions(zipped.rdd, function(plist) {
              do.call(func, plist[[1]])
            })
            res
          })

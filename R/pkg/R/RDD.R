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
#'
#' @rdname RDD
#' @seealso parallelize
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
setMethod("getSerializedMode", signature(rdd = "RDD"), function(rdd) rdd@env$serializedMode)

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
setMethod("getJRDD", signature(rdd = "RDD"), function(rdd) rdd@jrdd)
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
            PipelinedRDD(X, FUN)
          })

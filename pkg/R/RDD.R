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

            computeFunc <- function(split, part) {
              rdd@func(split, part)
            }

            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL)

            broadcastArr <- lapply(ls(.broadcastNames),
                                   function(name) { get(name, .broadcastNames) })

            serializedFuncArr <- serialize(computeFunc, connection = NULL)

            prev_jrdd <- rdd@prev_jrdd

            if (dataSerialization) {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.RRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   rdd@env$prev_serialized,
                                   serializedFuncArr,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            } else {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.StringRRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   rdd@env$prev_serialized,
                                   serializedFuncArr,
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


#' @rdname cache-methods
#' @aliases cache,RDD-method
setMethod("cache",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "cache")
            x@env$isCached <- TRUE
            x
          })

#' @rdname persist
#' @aliases persist,RDD-method
setMethod("persist",
          signature(x = "RDD", newLevel = "character"),
          function(x, newLevel = c("DISK_ONLY",
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
            
            callJMethod(getJRDD(x), "persist", storageLevel)
            x@env$isCached <- TRUE
            x
          })

#' @rdname unpersist-methods
#' @aliases unpersist,RDD-method
setMethod("unpersist",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "unpersist")
            x@env$isCached <- FALSE
            x
          })

#' @rdname checkpoint-methods
#' @aliases checkpoint,RDD-method
setMethod("checkpoint",
          signature(x = "RDD"),
          function(x) {
            jrdd <- getJRDD(x)
            callJMethod(jrdd, "checkpoint")
            x@env$isCheckpointed <- TRUE
            x
          })

#' @rdname numPartitions
#' @aliases numPartitions,RDD-method
setMethod("numPartitions",
          signature(x = "RDD"),
          function(x) {
            jrdd <- getJRDD(x)
            partitions <- callJMethod(jrdd, "splits")
            callJMethod(partitions, "size")
          })

#' @rdname collect-methods
#' @aliases collect,RDD-method
setMethod("collect",
          signature(x = "RDD"),
          function(x, flatten = TRUE) {
            # Assumes a pairwise RDD is backed by a JavaPairRDD.
            collected <- callJMethod(getJRDD(x), "collect")
            convertJListToRList(collected, flatten)
          })

#' @rdname collect-methods
#' @aliases collectPartition,integer,RDD-method
setMethod("collectPartition",
          signature(x = "RDD", partitionId = "integer"),
          function(x, partitionId) {
            jPartitionsList <- callJMethod(getJRDD(x),
                                           "collectPartitions",
                                           as.list(as.integer(partitionId)))

            jList <- jPartitionsList[[1]]
            convertJListToRList(jList, flatten = TRUE)
          })

#' @rdname collect-methods
#' @aliases collectAsMap,RDD-method
setMethod("collectAsMap",
          signature(x = "RDD"),
          function(x) {
            pairList <- collect(x)
            map <- new.env()
            lapply(pairList, function(i) { assign(as.character(i[[1]]), i[[2]], envir = map) })
            as.list(map)
          })

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

#' @rdname countByValue
#' @aliases countByValue,RDD-method
setMethod("countByValue",
          signature(x = "RDD"),
          function(x) {
            ones <- lapply(x, function(item) { list(item, 1L) })
            collect(reduceByKey(ones, `+`, numPartitions(x)))
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
            lapplyPartitionsWithIndex(X, func)
          })

#' @rdname lapply
#' @aliases map,RDD,function-method
setMethod("map",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapply(X, FUN)
          })

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

#' @rdname lapplyPartition
#' @aliases lapplyPartition,RDD,function-method
setMethod("lapplyPartition",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })

#' @rdname lapplyPartition
#' @aliases mapPartitions,RDD,function-method
setMethod("mapPartitions",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartition(X, FUN)
          })

#' @rdname lapplyPartitionsWithIndex
#' @aliases lapplyPartitionsWithIndex,RDD,function-method
setMethod("lapplyPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            FUN <- cleanClosure(FUN)
            closureCapturingFunc <- function(split, part) {
              FUN(split, part)
            }
            PipelinedRDD(X, closureCapturingFunc)
          })

#' @rdname lapplyPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,RDD,function-method
setMethod("mapPartitionsWithIndex",
          signature(X = "RDD", FUN = "function"),
          function(X, FUN) {
            lapplyPartitionsWithIndex(X, FUN)
          })

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

#' @rdname reduce
#' @aliases reduce,RDD,ANY-method
setMethod("reduce",
          signature(x = "RDD", func = "ANY"),
          function(x, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collect(lapplyPartition(x, reducePartition),
                                     flatten = FALSE)
            Reduce(func, partitionList)
          })

#' @rdname maximum
#' @aliases maximum,RDD
setMethod("maximum",
          signature(x = "RDD"),
          function(x) {
            reduce(x, max)
          })

#' @rdname minimum
#' @aliases minimum,RDD
setMethod("minimum",
          signature(x = "RDD"),
          function(x) {
            reduce(x, min)
          })

#' @rdname foreach
#' @aliases foreach,RDD,function-method
setMethod("foreach",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            partition.func <- function(x) {
              lapply(x, func)
              NULL
            }
            invisible(collect(mapPartitions(x, partition.func)))
          })

#' @rdname foreach
#' @aliases foreachPartition,RDD,function-method
setMethod("foreachPartition",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            invisible(collect(mapPartitions(x, func)))
          })

#' @rdname take
#' @aliases take,RDD,numeric-method
setMethod("take",
          signature(x = "RDD", num = "numeric"),
          function(x, num) {
            resList <- list()
            index <- -1
            jrdd <- getJRDD(x)
            numPartitions <- numPartitions(x)

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
                                           serialized = x@env$serialized)
              # TODO: Check if this append is O(n^2)?
              resList <- append(resList, elems)
            }
            resList
          })

setClassUnion("missingOrInteger", c("missing", "integer"))
#' @rdname distinct
#' @aliases distinct,RDD,missingOrInteger-method
setMethod("distinct",
          signature(x = "RDD", numPartitions = "missingOrInteger"),
          function(x, numPartitions) {
            if (missing(numPartitions)) {
              numPartitions <- SparkR::numPartitions(x)
            }
            identical.mapped <- lapply(x, function(x) { list(x, NULL) })
            reduced <- reduceByKey(identical.mapped,
                                   function(x, y) { x },
                                   numPartitions)
            resRDD <- lapply(reduced, function(x) { x[[1]] })
            resRDD
          })

#' @rdname sampleRDD
#' @aliases sampleRDD,RDD
setMethod("sampleRDD",
          signature(x = "RDD", withReplacement = "logical",
                    fraction = "numeric", seed = "integer"),
          function(x, withReplacement, fraction, seed) {

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

            lapplyPartitionsWithIndex(x, samplingFunc)
          })

#' @rdname takeSample
#' @aliases takeSample,RDD
setMethod("takeSample", signature(x = "RDD", withReplacement = "logical",
                                  num = "integer", seed = "integer"),
          function(x, withReplacement, num, seed) {
            # This function is ported from RDD.scala.
            fraction <- 0.0
            total <- 0
            multiplier <- 3.0
            initialCount <- count(x)
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
            samples <- collect(sampleRDD(x, withReplacement, fraction,
                                         as.integer(ceiling(runif(1,
                                                                  -MAXINT,
                                                                  MAXINT)))))
            # If the first sample didn't turn out large enough, keep trying to
            # take samples; this shouldn't happen often because we use a big
            # multiplier for thei initial size
            while (length(samples) < total)
              samples <- collect(sampleRDD(x, withReplacement, fraction,
                                           as.integer(ceiling(runif(1,
                                                                    -MAXINT,
                                                                    MAXINT)))))

            # TODO(zongheng): investigate if this call is an in-place shuffle?
            sample(samples)[1:total]
          })

#' @rdname keyBy
#' @aliases keyBy,RDD
setMethod("keyBy",
          signature(x = "RDD", func = "function"),
          function(x, func) {
            apply.func <- function(x) {
              list(func(x), x)
            }
            lapply(x, apply.func)
          })

#' @rdname repartition
#' @aliases repartition,RDD
setMethod("repartition",
          signature(x = "RDD", numPartitions = "numeric"),
          function(x, numPartitions) {
            coalesce(x, numPartitions, TRUE)
          })

#' @rdname coalesce
#' @aliases coalesce,RDD
setMethod("coalesce",
           signature(x = "RDD", numPartitions = "numeric"),
           function(x, numPartitions, shuffle = FALSE) {
             if (as.integer(numPartitions) != numPartitions) {
               warning("Number of partitions should be an integer. Coercing it to integer.")
             }
             numPartitions <- as.integer(numPartitions)
             if (shuffle || numPartitions > SparkR::numPartitions(x)) {
               func <- function(s, part) {
                 set.seed(s)  # split as seed
                 start <- as.integer(sample(numPartitions, 1) - 1)
                 lapply(seq_along(part),
                        function(i) {
                          pos <- (start + i) %% numPartitions
                          list(pos, part[[i]])
                        })
               }
               shuffled <- lapplyPartitionsWithIndex(x, func)
               repartitioned <- partitionBy(shuffled, numPartitions)
               values(repartitioned)
             } else {
               jrdd <- callJMethod(getJRDD(x), "coalesce", numPartitions, shuffle)
               RDD(jrdd)
             }
           })

#' @rdname saveAsObjectFile
#' @aliases saveAsObjectFile,RDD
setMethod("saveAsObjectFile",
          signature(x = "RDD", path = "character"),
          function(x, path) {
            # If the RDD is in string format, need to serialize it before saving it because when
            # objectFile() is invoked to load the saved file, only serialized format is assumed.
            if (!x@env$serialized) {
              x <- reserialize(x)
            }
            # Return nothing
            invisible(callJMethod(getJRDD(x), "saveAsObjectFile", path))
          })

#' @rdname saveAsTextFile
#' @aliases saveAsTextFile,RDD
setMethod("saveAsTextFile",
          signature(x = "RDD", path = "character"),
          function(x, path) {
            func <- function(str) {
              toString(str)
            }
            stringRdd <- lapply(x, func)
            # Return nothing
            invisible(
              callJMethod(getJRDD(stringRdd, dataSerialization = FALSE), "saveAsTextFile", path))
          })

#' @rdname sortBy
#' @aliases sortBy,RDD,RDD-method
setMethod("sortBy",
          signature(x = "RDD", func = "function"),
          function(x, func, ascending = TRUE, numPartitions = SparkR::numPartitions(x)) {          
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
  
  newRdd <- mapPartitions(x, partitionFunc)
  reduce(newRdd, reduceFunc)
}

#' @rdname takeOrdered
#' @aliases takeOrdered,RDD,RDD-method
setMethod("takeOrdered",
          signature(x = "RDD", num = "integer"),
          function(x, num) {          
            takeOrderedElem(x, num)
          })

#' @rdname top
#' @aliases top,RDD,RDD-method
setMethod("top",
          signature(x = "RDD", num = "integer"),
          function(x, num) {          
            takeOrderedElem(x, num, FALSE)
          })

#' @rdname fold
#' @aliases fold,RDD,RDD-method
setMethod("fold",
          signature(x = "RDD", zeroValue = "ANY", op = "ANY"),
          function(x, zeroValue, op) {
            aggregateRDD(x, zeroValue, op, op)
          })

#' @rdname aggregateRDD
#' @aliases aggregateRDD,RDD,RDD-method
setMethod("aggregateRDD",
          signature(x = "RDD", zeroValue = "ANY", seqOp = "ANY", combOp = "ANY"),
          function(x, zeroValue, seqOp, combOp) {        
            partitionFunc <- function(part) {
              Reduce(seqOp, part, zeroValue)
            }
            
            partitionList <- collect(lapplyPartition(x, partitionFunc),
                                     flatten = FALSE)
            Reduce(combOp, partitionList, zeroValue)
          })

#' @rdname pipeRDD
#' @aliases pipeRDD,RDD,character-method
setMethod("pipeRDD",
          signature(x = "RDD", command = "character"),
          function(x, command, env = list()) {
            func <- function(part) {
              trim.trailing.func <- function(x) {
                sub("[\r\n]*$", "", toString(x))
              }
              input <- unlist(lapply(part, trim.trailing.func))
              res <- system2(command, stdout = TRUE, input = input, env = env)
              lapply(res, trim.trailing.func)
            }
            lapplyPartition(x, func)
          })

#' @rdname name
#' @aliases name,RDD
setMethod("name",
          signature(x = "RDD"),
          function(x) {
            callJMethod(getJRDD(x), "name")
          })

#' @rdname setName
#' @aliases setName,RDD
setMethod("setName",
          signature(x = "RDD", name = "character"),
          function(x, name) {
            callJMethod(getJRDD(x), "setName", name)
            x
          })

#' @rdname zipWithUniqueId
#' @aliases zipWithUniqueId,RDD
setMethod("zipWithUniqueId",
          signature(x = "RDD"),
          function(x) {
            n <- numPartitions(x)

            partitionFunc <- function(split, part) {
              mapply(
                function(item, index) {
                  list(item, (index - 1) * n + split)
                },
                part,
                seq_along(part),
                SIMPLIFY = FALSE)
            }

            lapplyPartitionsWithIndex(x, partitionFunc)
          })

#' @rdname zipWithIndex
#' @aliases zipWithIndex,RDD
setMethod("zipWithIndex",
          signature(x = "RDD"),
          function(x) {
            n <- numPartitions(x)
            if (n > 1) {
              nums <- collect(lapplyPartition(x,
                                              function(part) {
                                                list(length(part))
                                              }))
              startIndices <- Reduce("+", nums, accumulate = TRUE)
            }

            partitionFunc <- function(split, part) {
              if (split == 0) {
                startIndex <- 0
              } else {
                startIndex <- startIndices[[split]]
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


############ Binary Functions #############


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

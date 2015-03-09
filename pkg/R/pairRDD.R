# Operations supported on RDDs contains pairs (i.e key, value)

############ Actions and Transformations ############


#' @rdname lookup
#' @aliases lookup,RDD-method
setMethod("lookup",
          signature(x = "RDD", key = "ANY"),
          function(x, key) {
            partitionFunc <- function(part) {
              filtered <- part[unlist(lapply(part, function(i) { identical(key, i[[1]]) }))]
              lapply(filtered, function(i) { i[[2]] })
            }
            valsRDD <- lapplyPartition(x, partitionFunc)
            collect(valsRDD)
          })

#' @rdname countByKey
#' @aliases countByKey,RDD-method
setMethod("countByKey",
          signature(x = "RDD"),
          function(x) {
            keys <- lapply(x, function(item) { item[[1]] })
            countByValue(keys)
          })

#' @rdname keys
#' @aliases keys,RDD
setMethod("keys",
          signature(x = "RDD"),
          function(x) {
            func <- function(k) {
              k[[1]]
            }
            lapply(x, func)
          })

#' @rdname values
#' @aliases values,RDD
setMethod("values",
          signature(x = "RDD"),
          function(x) {
            func <- function(v) {
              v[[2]]
            }
            lapply(x, func)
          })

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


#' @rdname partitionBy
#' @aliases partitionBy,RDD,integer-method
setMethod("partitionBy",
          signature(x = "RDD", numPartitions = "integer"),
          function(x, numPartitions, partitionFunc = hashCode) {

            #if (missing(partitionFunc)) {
            #  partitionFunc <- hashCode
            #}

            partitionFunc <- cleanClosure(partitionFunc)
            serializedHashFuncBytes <- serialize(partitionFunc, connection = NULL)

            packageNamesArr <- serialize(.sparkREnv$.packages,
                                         connection = NULL)
            broadcastArr <- lapply(ls(.broadcastNames), function(name) {
                                   get(name, .broadcastNames) })
            jrdd <- getJRDD(x)

            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs).
            pairwiseRRDD <- newJObject("edu.berkeley.cs.amplab.sparkr.PairwiseRRDD",
                                       callJMethod(jrdd, "rdd"),
                                       as.integer(numPartitions),
                                       x@env$serialized,
                                       serializedHashFuncBytes,
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

#' @rdname groupByKey
#' @aliases groupByKey,RDD,integer-method
setMethod("groupByKey",
          signature(x = "RDD", numPartitions = "integer"),
          function(x, numPartitions) {
            shuffled <- partitionBy(x, numPartitions)
            groupVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              appendList <- function(acc, i) {
                addItemToAccumulator(acc, i)
                acc
              }
              makeList <- function(i) {
                acc <- initAccumulator()
                addItemToAccumulator(acc, i)
                acc
              }
              # Each item in the partition is list of (K, V)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred,
                                          appendList, makeList)
                     })
              # extract out data field
              vals <- eapply(vals,
                             function(i) {
                               length(i$data) <- i$counter
                               i$data
                             })
              # Every key in the environment contains a list
              # Convert that to list(K, Seq[V])
              convertEnvsToList(keys, vals)
            }
            lapplyPartition(shuffled, groupVals)
          })

#' @rdname reduceByKey
#' @aliases reduceByKey,RDD,integer-method
setMethod("reduceByKey",
          signature(x = "RDD", combineFunc = "ANY", numPartitions = "integer"),
          function(x, combineFunc, numPartitions) {
            reduceVals <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred, combineFunc, identity)
                     })
              convertEnvsToList(keys, vals)
            }
            locallyReduced <- lapplyPartition(x, reduceVals)
            shuffled <- partitionBy(locallyReduced, numPartitions)
            lapplyPartition(shuffled, reduceVals)
          })

#' @rdname reduceByKeyLocally
#' @aliases reduceByKeyLocally,RDD,integer-method
setMethod("reduceByKeyLocally",
          signature(x = "RDD", combineFunc = "ANY"),
          function(x, combineFunc) {
            reducePart <- function(part) {
              vals <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(hashCode(item[[1]]))
                       updateOrCreatePair(item, keys, vals, pred, combineFunc, identity)
                     })
              list(list(keys, vals)) # return hash to avoid re-compute in merge
            }
            mergeParts <- function(accum, x) {
              pred <- function(item) {
                exists(item$hash, accum[[1]])
              }
              lapply(ls(x[[1]]),
                     function(name) {
                       item <- list(x[[1]][[name]], x[[2]][[name]])
                       item$hash <- name
                       updateOrCreatePair(item, accum[[1]], accum[[2]], pred, combineFunc, identity)
                     })
              accum
            }
            reduced <- mapPartitions(x, reducePart)
            merged <- reduce(reduced, mergeParts)
            convertEnvsToList(merged[[1]], merged[[2]])
          })

#' @rdname combineByKey
#' @aliases combineByKey,RDD,ANY,ANY,ANY,integer-method
setMethod("combineByKey",
          signature(x = "RDD", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "integer"),
          function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
            combineLocally <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(item[[1]])
                       updateOrCreatePair(item, keys, combiners, pred, mergeValue, createCombiner)
                     })
              convertEnvsToList(keys, combiners)
            }
            locallyCombined <- lapplyPartition(x, combineLocally)
            shuffled <- partitionBy(locallyCombined, numPartitions)
            mergeAfterShuffle <- function(part) {
              combiners <- new.env()
              keys <- new.env()
              pred <- function(item) exists(item$hash, keys)
              lapply(part,
                     function(item) {
                       item$hash <- as.character(item[[1]])
                       updateOrCreatePair(item, keys, combiners, pred, mergeCombiners, identity)
                     })
              convertEnvsToList(keys, combiners)
            }
            lapplyPartition(shuffled, mergeAfterShuffle)
          })

#' @rdname aggregateByKey
#' @aliases aggregateByKey,RDD,ANY,ANY,ANY,integer-method
setMethod("aggregateByKey",
          signature(x = "RDD", zeroValue = "ANY", seqOp = "ANY",
                    combOp = "ANY", numPartitions = "integer"),
          function(x, zeroValue, seqOp, combOp, numPartitions) {
            createCombiner <- function(v) {
              do.call(seqOp, list(zeroValue, v))
            }

            combineByKey(x, createCombiner, seqOp, combOp, numPartitions)
          })

#' @rdname foldByKey
#' @aliases foldByKey,RDD,ANY,ANY,integer-method
setMethod("foldByKey",
          signature(x = "RDD", zeroValue = "ANY",
                    func = "ANY", numPartitions = "integer"),
          function(x, zeroValue, func, numPartitions) {
            aggregateByKey(x, zeroValue, func, func, numPartitions)
          })

############ Binary Functions #############


#' @rdname join-methods
#' @aliases join,RDD,RDD-method
setMethod("join",
          signature(x = "RDD", y = "RDD", numPartitions = "integer"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' @rdname join-methods
#' @aliases leftOuterJoin,RDD,RDD-method
setMethod("leftOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "integer"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(FALSE, TRUE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' @rdname join-methods
#' @aliases rightOuterJoin,RDD,RDD-method
setMethod("rightOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "integer"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })
            
            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, FALSE))
            }
            
            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

#' @rdname join-methods
#' @aliases fullOuterJoin,RDD,RDD-method

setMethod("fullOuterJoin",
          signature(x = "RDD", y = "RDD", numPartitions = "integer"),
          function(x, y, numPartitions) {
            xTagged <- lapply(x, function(i) { list(i[[1]], list(1L, i[[2]])) })
            yTagged <- lapply(y, function(i) { list(i[[1]], list(2L, i[[2]])) })

            doJoin <- function(v) {
              joinTaggedList(v, list(TRUE, TRUE))
            }

            joined <- flatMapValues(groupByKey(unionRDD(xTagged, yTagged), numPartitions), doJoin)
          })

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

#' @rdname sortByKey
#' @aliases sortByKey,RDD,RDD-method
setMethod("sortByKey",
          signature(x = "RDD"),
          function(x, ascending = TRUE, numPartitions = SparkR::numPartitions(x)) {
            rangeBounds <- list()
            
            if (numPartitions > 1) {
              rddSize <- count(x)
              # constant from Spark's RangePartitioner
              maxSampleSize <- numPartitions * 20
              fraction <- min(maxSampleSize / max(rddSize, 1), 1.0)
              
              samples <- collect(keys(sampleRDD(x, FALSE, fraction, 1L)))
              
              # Note: the built-in R sort() function only works on atomic vectors
              samples <- sort(unlist(samples, recursive = FALSE), decreasing = !ascending)
              
              if (length(samples) > 0) {
                rangeBounds <- lapply(seq_len(numPartitions - 1),
                                      function(i) {
                                        j <- ceiling(length(samples) * i / numPartitions)
                                        samples[j]
                                      })
              }
            }

            rangePartitionFunc <- function(key) {
              partition <- 0
              
              # TODO: Use binary search instead of linear search, similar with Spark
              while (partition < length(rangeBounds) && key > rangeBounds[[partition + 1]]) {
                partition <- partition + 1
              }
              
              if (ascending) {
                partition
              } else {
                numPartitions - partition - 1
              }
            }
            
            partitionFunc <- function(part) {
              sortKeyValueList(part, decreasing = !ascending)
            }
            
            newRDD <- partitionBy(x, numPartitions, rangePartitionFunc)
            lapplyPartition(newRDD, partitionFunc)
          })
          

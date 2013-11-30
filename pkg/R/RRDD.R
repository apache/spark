# RRDD (RDD in R) class implemented in S4 OO system.

#setOldClass("jobjRef")

setClass("RRDD", slots = list(jrdd = "jobjRef",
                              serialized = "logical"))

setValidity("RRDD",
            function(object) {
              cls <- object@jrdd$getClass()
              className <- cls$getName()
              if (grep("spark.api.java.*RDD*", className) == 1) {
                TRUE
              } else {
                paste("Invalid RDD class ", className)
              }
            })

# Constructor of the RRDD class.
RRDD <- function(jrdd, serialized = TRUE) {
  new("RRDD", jrdd = jrdd, serialized = serialized)
}


setGeneric("cache", function(rrdd) { standardGeneric("cache") })
setMethod("cache",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            .jcall(rrdd@jrdd, "Lorg/apache/spark/api/java/JavaRDD;", "cache")
            rrdd
          })


# collect(): Return a list that contains all of the elements in this RRDD.
# NOTE: supports only RRDD[Array[Byte]] and RRDD[primitive java type] for now.
setGeneric("collect", function(rrdd, ...) { standardGeneric("collect") })
setMethod("collect",
          signature(rrdd = "RRDD"),
          function(rrdd, flatten = TRUE) {
            # Assumes a pairwise RRDD is backed by a JavaPairRDD.
            isPairwise <- grep("spark.api.java.JavaPairRDD",
                               rrdd@jrdd$getClass()$getName())
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            convertJListToRList(collected, if (length(isPairwise) == 1) FALSE
                                           else flatten)
          })


setGeneric("count", function(rrdd) { standardGeneric("count") })
setMethod("count",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            valsRDD <- lapplyPartition(rrdd, countPartition)
            vals <- collect(valsRDD)
            sum(as.integer(vals))
          })

setMethod("length",
          signature(x = "RRDD"),
          function(x) {
            count(x)
          })


setMethod("lapply",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            partitionFunc <- function(part) {
              lapply(part, FUN)
            }

            lapplyPartition(X, partitionFunc)
          })

setGeneric("lapplyPartition", function(X, FUN) {
           standardGeneric("lapplyPartition") })
setMethod("lapplyPartition",
          signature(X = "RRDD", FUN = "function"),
          function(X, FUN) {
            serializedFunc <- serialize(FUN, connection = NULL, ascii = TRUE)
            serializedFuncArr <- .jarray(serializedFunc)

            depsBin <- getDependencies(FUN)
            depsBinArr <- .jarray(depsBin)
            rrddRef <- new(J("org.apache.spark.api.r.RRDD"),
                           X@jrdd$rdd(),
                           serializedFuncArr,
                           X@serialized,
                           depsBinArr,
                           X@jrdd$classManifest())
            jrdd <- rrddRef$asJavaRDD()
            RRDD(jrdd, TRUE)
          })

setGeneric("reduce", function(rrdd, func) { standardGeneric("reduce") })
setMethod("reduce",
          signature(rrdd = "RRDD", func = "ANY"),
          function(rrdd, func) {

            reducePartition <- function(part) {
              Reduce(func, part)
            }

            partitionList <- collect(lapplyPartition(rrdd, reducePartition),
                                     flatten=FALSE)
            Reduce(func, partitionList)
          })

# Take the first NUM elements in the RRDD and returns them in a list.
setGeneric("take", function(rrdd, num) { standardGeneric("take") })
setMethod("take",
          signature(rrdd = "RRDD", num = "numeric"),
          function(rrdd, num) {
            resList <- list()
            index <- -1
            numPartitions <- .jcall(rrdd@jrdd, "I", "numPartitions")
            while (TRUE) {
              index <- index + 1

              if (length(resList) >= num || index >= numPartitions)
                break

              # a JList of byte arrays
              partition <- .jcall(rrdd@jrdd,
                                  "Ljava/util/List;",
                                  "collectPartition",
                                  as.integer(index))
              elems <- convertJListToRList(partition, flatten = TRUE)
              resList <- append(resList, head(elems, n = num - length(resList))) # O(n^2)?
            }
            resList
          })

############ Shuffle Functions ############

.address <- function(x) {
  # http://stackoverflow.com/questions/10912729/r-object-identity
  substring(capture.output(.Internal(inspect(x)))[1], 2, 10)
}

setGeneric("partitionBy",
           function(rrdd, numPartitions, partitionFunc) {
             standardGeneric("partitionBy")
           })
setMethod("partitionBy",
          signature(rrdd = "RRDD", numPartitions = "integer", partitionFunc = "function"),
          function(rrdd, numPartitions, partitionFunc) {

            hashPairsToEnvir <- function(pairs) {
              # pairs: list(list(_, _), ..)
              res <- new.env()
              HACK <- environment(partitionFunc) # FIXME
              for (tuple in pairs) {
                hashVal = partitionFunc(tuple[[1]])
                bucket = as.character(hashVal %% numPartitions)
                acc <- res[[bucket]]
                # TODO?: http://stackoverflow.com/questions/2436688/append-an-object-to-a-list-in-r-in-amortized-constant-time
                acc[[length(acc) + 1]] <- tuple
                res[[bucket]] <- acc
              }
              res
            }

            serializedHashFunc <- serialize(hashPairsToEnvir,
                                            connection = NULL,
                                            ascii = TRUE)
            serializedHashFuncBytes <- .jarray(serializedHashFunc)
            depsBin <- getDependencies(hashPairsToEnvir)
            depsBinArr <- .jarray(depsBin)

            # At this point, we have RDD[(Array[Byte], Array[Byte])],
            # which contains the actual content, in unknown partitions order.
            # We create a PairwiseRRDD that extends RDD[(Array[Byte],
            # Array[Byte])], where the key is the hashed split, the value is
            # the content (key-val pairs). It does not matter how the data are
            # stored in what partitions at this point.

            pairwiseRRDD <- new(J("org.apache.spark.api.r.PairwiseRRDD"),
                                rrdd@jrdd,
                                as.integer(numPartitions),
                                serializedHashFuncBytes,
                                rrdd@serialized,
                                depsBinArr)

            # Create a corresponding RPartitioner.
            rPartitioner <- new(J("org.apache.spark.api.r.RPartitioner"),
                                as.integer(numPartitions),
                                .address(partitionFunc)) # TODO: does this work?

            # Call partitionBy on the obtained PairwiseRDD.
            javaPairRDD <- pairwiseRRDD$asJavaPairRDD()$partitionBy(rPartitioner)

            # Call .values() on the result to get back the final result, the
            # shuffled acutal content key-val pairs.
            r <- javaPairRDD$values()

            RRDD(r, rrdd@serialized)
          })


setGeneric("collectPartition",
           function(rrdd, partitionId) {
             standardGeneric("collectPartition")
           })
setMethod("collectPartition",
          signature(rrdd = "RRDD", partitionId = "integer"),
          function(rrdd, partitionId) {
            jList <- .jcall(rrdd@jrdd,
                            "Ljava/util/List;",
                            "collectPartition",
                            as.integer(partitionId))
            convertJListToRList(jList, flatten = TRUE)
          })


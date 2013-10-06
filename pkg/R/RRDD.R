# RRDD (RDD in R) class implemented in S4 OO system.

#setOldClass("jobjRef")

setClass("RRDD", slots = list(jrdd = "jobjRef", serialized = "logical"))

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
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            JavaListToRList(collected, flatten)
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

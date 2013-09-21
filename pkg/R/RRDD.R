# RRDD (RDD in R) class implemented in S4 OO system.

setOldClass("jobjRef")

setClass("RRDD", slots = list(jrdd = "jobjRef"))

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
RRDD <- function(jrdd) {
  new("RRDD", jrdd = jrdd)
}

# collect(): Return a vector that contains all of the elements in this RRDD.
# TODO
setGeneric("collect", function(x) { standardGeneric("collect") })
setMethod("collect",
          signature(x = "RRDD"),
          function(x) { 
            collected <- .jcall(x@jrdd, "Ljava/util/List;", "collect")
            JavaListToRList(collected)
          })
# For JavaRDD[Array[Byte]]:
# x <- jrdd$collect()
# byteArrays <- .jevalArray(x$toArray())
# unserialize(.jevalArray(byteArrays[[1]]))

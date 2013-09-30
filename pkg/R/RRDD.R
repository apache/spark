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

# collect(): Return a list that contains all of the elements in this RRDD.
# NOTE: supports only RRDD[Array[Byte]] and RRDD[primitive java type] for now.
setGeneric("collect", function(rrdd) { standardGeneric("collect") })
setMethod("collect",
          signature(rrdd = "RRDD"),
          function(rrdd) {
            collected <- .jcall(rrdd@jrdd, "Ljava/util/List;", "collect")
            JavaListToRList(collected, flatten = TRUE)
          })


# RRDD (RDD in R) class implemented in S4 OO system.

# TODO: do we really want to mix S3 and S4?
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

# collect()
setGeneric("collect", function(x) { standardGeneric("collect") })
setMethod("collect", signature(x = "RRDD"),
          function(x) { 
            collected <- x@jrdd$collect()
            JavaListToRList(collected)
          })

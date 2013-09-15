# Utilities and Helpers

"JavaListToRList" <- function(jList) {
  size <- .jcall(jList, "I", "size")

  # FIXME: each call to the lambda gives the error, most likely due to type erasure in List<E>:
  # Error in .jfield(x, "Ljava/lang/Class;", "TYPE") :
    # trying to generate an object from a virtual class ("jobjRef")
  # Perhaps: 
    # calling toString() on every object and deal with strings?
    # mimick PySpark: write custom Scala serializers for SparkR
  lapply(0:(size - 1), 
         function(index) {
           .jcall(jList, "Ljava/lang/Object;", "get", as.integer(index))
         })
}
          

# Utilities and Helpers

"JavaListToRList" <- function(jList) {
  size <- .jcall(jList, "I", "size")

  # FIXME: each call to the lambda gives the error:
  # Error in .jfield(x, "Ljava/lang/Class;", "TYPE") :
    # trying to generate an object from a virtual class ("jobjRef")
  lapply(0:(size - 1), 
         function(index) {
           # FIXME: convert to .jcall? How to specify return type?
           jList$get(as.integer(index))
         })
}
          

# Utilities and Helpers

# TODO: test with RRDD[T] where T is not String
# Given a List<T>, returns an R list.
"JavaListToRList" <- function(jList, flatten = FALSE) {
  size <- .jcall(jList, "I", "size")
  results <-
    lapply(0:(size - 1),
           function(index) {
             jElem <- .jcall(jList, "Ljava/lang/Object;", "get", as.integer(index))

             # Either an R object or a Java obj ref
             obj <- .jsimplify(jElem)

             # RRDD[Array[Byte]]: call unserialize() and be sure to flatten
             if (class(obj) == "jobjRef" && .jinstanceof(obj, "[B")) {
               rRaw <- .jevalArray(.jcastToArray(jElem))
               res <- unserialize(rRaw)
             }

             # FIXME?
             if (class(obj) == "jobjRef" && !.jinstanceof(obj, "[B")) {
               stop(paste("utils.R: JavaListToRList: does not support any",
                          "RRDD[Array[T]] where T != Byte, for now"))
             }

             # jElem is of a primitive Java type, is simplified to R's corresponding type
             if (class(obj) != "jobjRef")
               res <- obj

             res
           })
  if (flatten) {
    as.list(unlist(results))
  } else {
    as.list(results)
  }
}


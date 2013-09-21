# Utilities and Helpers

"JavaListToRList" <- function(jList) {
  # size <- .jcall(jList, "I", "size")
  # # TODO: test with RRDD[T] where T is not String
  # lapply(0:(size - 1), 
         # function(index) {
           # jElem <- .jcall(jList, "Ljava/lang/Object;", "get", as.integer(index))
           # .jsimplify(jElem)
         # })
  .jevalArray(.jcall(jList, "[Ljava/lang/Object;", "toArray"))
}
          

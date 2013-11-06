# Utilities and Helpers

# Given a JList<T>, returns an R list containing the same elements.  Takes care
# of deserializations and type conversions.
convertJListToRList <- function(jList, flatten) {
  size <- .jcall(jList, "I", "size")
  results <-
    lapply(0:(size - 1),
           function(index) {
             jElem <- .jcall(jList,
                             "Ljava/lang/Object;",
                             "get",
                             as.integer(index))

             # Assume it is either an R object or a Java obj ref.
             obj <- .jsimplify(jElem)

             if (class(obj) == "jobjRef" && .jinstanceof(obj, "[B")) {
               # RRDD[Array[Byte]].

               rRaw <- .jevalArray(.jcastToArray(jElem))
               res <- unserialize(rRaw)

             } else if (class(obj) == "jobjRef" &&
                        .jinstanceof(obj, "scala.Tuple2")) {
               # JavaPairRDD[Array[Byte], Array[Byte]].

               keyBytes = .jcall(obj, "Ljava/lang/Object;", "_1")
               valBytes = .jcall(obj, "Ljava/lang/Object;", "_2")
               res <- list(unserialize(.jevalArray(keyBytes)),
                           unserialize(.jevalArray(valBytes)))

             } else if (class(obj) == "jobjRef" && !.jinstanceof(obj, "[B")) {
               stop(paste("utils.R: convertJListToRList only supports",
                          "RRDD[Array[Byte]] and",
                          "JavaPairRDD[Array[Byte], Array[Byte]] for now"))
             }

             # jElem is of a primitive Java type, is simplified to R's
             # corresponding type.
             if (class(obj) != "jobjRef")
               res <- obj

             res
           })

  if (flatten) {
    as.list(unlist(results, recursive = FALSE))
  } else {
    as.list(results)
  }

}

isRRDD <- function(name, env) {
  obj <- get(name, envir=env)
  class(obj) == "RRDD"
}

getDependencies <- function(name) {
  fileName <- tempfile(pattern="spark-utils", fileext=".deps")
  funcEnv <- environment(name)
  varsToSave <- ls(funcEnv)
  filteredVars <- Filter(function(x) { !isRRDD(x, funcEnv) }, varsToSave)

  save(list=filteredVars, file=fileName, envir=funcEnv)
  fileSize <- file.info(fileName)$size
  binData <- readBin(fileName, raw(), fileSize, endian="big")

  unlink(fileName)
  binData
}


# Utilities and Helpers

# Given a JList<T>, returns an R list containing the same elements.  Takes care
# of deserializations and type conversions.
convertJListToRList <- function(jList, flatten) {
  size <- .jcall(jList, "I", "size")
  results <- if (size > 0) {
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
               res <- list(obj)

             res
           })
  } else {
    list()
  }

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

isSparkFunction <- function(name) {
  if (is.function(name)) {
    fun <- name
  } else {
    if (!(is.character(name) && length(name) == 1L || is.symbol(name))) {
      fun <- eval.parent(substitute(substitute(name)))
      if (!is.symbol(fun))
        stop(gettextf("'%s' is not a function, character or symbol",
                      deparse(fun)), domain = NA)
    } else {
      fun <- name
    }
    envir <- parent.frame(2)
    if (!exists(as.character(fun), mode = "function", envir=envir)) {
      return(FALSE)
    }
    fun <- get(as.character(fun), mode = "function", envir=envir)
  }
  packageName(environment(fun)) == "SparkR"
}

getDependencies <- function(name) {
  fileName <- tempfile(pattern="spark-utils", fileext=".deps")
  funcEnv <- environment(name)
  varsToSave <- ls(funcEnv)

  #print(varsToSave)
  filteredVars <- varsToSave
  filteredVars <- Filter(function(x) { !isRRDD(x, funcEnv) }, varsToSave)
  #filteredVars <- Filter(function(x) { !isSparkFunction(x) }, filteredVars)

  save(list=filteredVars, file=fileName, envir=funcEnv)
  fileSize <- file.info(fileName)$size
  binData <- readBin(fileName, raw(), fileSize, endian="big")

  unlink(fileName)
  binData
}

# Utilities and Helpers

# Given a JList<T>, returns an R list containing the same elements, the number
# of which is optionally upper bounded by `size` (by default, return all elements).
# Takes care of deserializations and type conversions.
convertJListToRList <- function(jList, flatten, size = NULL) {
  arrSize <- .jcall(jList, "I", "size")
  results <- if (arrSize > 0) {
    lapply(0:(arrSize - 1),
           function(index) {
             jElem <- .jcall(jList,
                             "Ljava/lang/Object;",
                             "get",
                             as.integer(index))

             # Assume it is either an R object or a Java obj ref.
             obj <- .jsimplify(jElem)

             if (inherits(obj, "jobjRef") && .jinstanceof(obj, "[B")) {
               # RDD[Array[Byte]]. `obj` is a whole partition.

               rRaw <- .jevalArray(.jcastToArray(jElem))
               res <- unserialize(rRaw)

             } else if (inherits(obj, "jobjRef") &&
                        .jinstanceof(obj, "scala.Tuple2")) {
               # JavaPairRDD[Array[Byte], Array[Byte]].

               keyBytes = .jcall(obj, "Ljava/lang/Object;", "_1")
               valBytes = .jcall(obj, "Ljava/lang/Object;", "_2")
               res <- list(unserialize(.jevalArray(keyBytes)),
                           unserialize(.jevalArray(valBytes)))

             } else if (inherits(obj, "jobjRef") && !.jinstanceof(obj, "[B")) {
               stop(paste("utils.R: convertJListToRList only supports",
                          "RDD[Array[Byte]] and",
                          "JavaPairRDD[Array[Byte], Array[Byte]] for now"))
             }

             # jElem is of a primitive Java type, is simplified to R's
             # corresponding type.
             if (!inherits(obj, "jobjRef")) {
               res <- list(obj)
             }

             res
           })
  } else {
    list()
  }

  if (flatten) {
    r <- as.list(unlist(results, recursive = FALSE))
  } else {
    r <- as.list(results)
  }

  if (!is.null(size)) {
    # Invariant: whenever `size` is passed in, it applies to the
    # logical representation of the data, namely the user doesn't
    # and shouldn't think about byte arrays and/or serde. Hence we
    # apply the upper bound directly after the flatten semantics.
    r <- head(r, n = size)
  } else {
    r
  }
}

# Given a Java array of byte arrays, deserilize each, returning an R list of
# the deserialized elements.
deserializeByteArrays <- function(byteArrs) {
  arrs <- .jevalArray(byteArrs)
  lapply(arrs, function(bs) { unlist(unserialize(.jevalArray(bs))) })
}

# Returns TRUE if `name` refers to an RDD in the given environment `env`
isRDD <- function(name, env) {
  obj <- get(name, envir=env)
  inherits(obj, "RDD")
}

# Returns TRUE if `name` is a function in the SparkR package.
# TODO: Handle package-private functions as well ?
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

# Serialize the dependencies of the given function and return them as a raw
# vector. Filters out RDDs before serializing the dependencies
getDependencies <- function(name) {
  varsToSave <- c()
  closureEnv <- environment(name)

  currentEnv <- closureEnv
  while (TRUE) {
    # Don't serialize namespaces
    if (!isNamespace(currentEnv)) {
      varsToSave <- c(varsToSave, ls(currentEnv))
    }

    # Everything below globalenv are packages, search path stuff etc.
    if (identical(currentEnv, globalenv()))
       break
    currentEnv <- parent.env(currentEnv)
  }
  filteredVars <- Filter(function(x) { !isRDD(x, closureEnv) }, varsToSave)

  # TODO: A better way to exclude variables that have been broadcast
  # would be to actually list all the variables used in every function using
  # `all.vars` and then walking through functions etc.
  filteredVars <- Filter(
                    function(x) { !exists(x, .broadcastNames, inherits=FALSE) },
                    filteredVars)

  fileName <- tempfile(pattern="spark-utils", fileext=".deps")
  save(list=filteredVars, file=fileName, envir=closureEnv)
  fileSize <- file.info(fileName)$size
  binData <- readBin(fileName, raw(), fileSize, endian="big")

  unlink(fileName)
  binData
}

# Helper function used to wrap a 'numeric' value to integer bounds.
# Useful for implementing C-like integer arithmetic
wrapInt <- function(value) {
  if (value > .Machine$integer.max) {
    value <- value - 2 * .Machine$integer.max - 2
  } else if (value < -1 * .Machine$integer.max) {
    value <- 2 * .Machine$integer.max + value + 2
  }
  value
}

# Multiply `val` by 31 and add `addVal` to the result. Ensures that
# integer-overflows are handled at every step.
mult31AndAdd <- function(val, addVal) {
  vec <- c(bitwShiftL(val, c(4,3,2,1,0)), addVal)
  Reduce(function(a, b) {
          wrapInt(as.numeric(a) + as.numeric(b))
         },
         vec)
}

#' Compute the hashCode of an object
#'
#' Java-style function to compute the hashCode for the given object. Returns
#' an integer value.
#'
#' @details
#' This only works for integer, numeric and character types right now.
#'
#' @param key the object to be hashed
#' @return the hash code as an integer
#' @export
#' @examples
#' hashCode(1L) # 1
#' hashCode(1.0) # 1072693248
#' hashCode("1") # 49
hashCode <- function(key) {
  if (class(key) == "integer") {
    as.integer(key[[1]])
  } else if (class(key) == "numeric") {
    # Convert the double to long and then calculate the hash code
    rawVec <- writeBin(key[[1]], con=raw())
    intBits <- packBits(rawToBits(rawVec), "integer")
    as.integer(bitwXor(intBits[2], intBits[1]))
  } else if (class(key) == "character") {
    n <- nchar(key)
    if (n == 0) {
      0L
    } else {
      asciiVals <- sapply(charToRaw(key), function(x) { strtoi(x, 16L) })
      hashC <- 0
      for (k in 1:length(asciiVals)) {
        hashC <- mult31AndAdd(hashC, asciiVals[k])
      }
      as.integer(hashC)
    }
  } else {
    warning(paste("Could not hash object, returning 0", sep=""))
    as.integer(0)
  }
}

# Create a new RDD in serialized form.
# Return itself if already in serialized form.
reserialize <- function(rdd) {
  if (!inherits(rdd, "RDD")) {
    stop("Argument 'rdd' is not an RDD type.")
  }
  if (rdd@env$serialized) {
    return(rdd)
  } else {
    ser.rdd <- lapply(rdd, function(x) { x })
    return(ser.rdd)
  }
}

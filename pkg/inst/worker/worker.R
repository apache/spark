# Worker class

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
# Set libPaths to include SparkR package as loadNamespace needs this
# TODO: Figure out if we can avoid this by not loading any objects that require
# SparkR namespace
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(port = port, blocking = TRUE, open = "rb")
outputCon <- socketConnection(port = port, blocking = TRUE, open = "wb")

# read the index of the current partition inside the RDD
splitIndex <- SparkR:::readInt(inputCon)

# read the isInputSerialized bit flag
isInputSerialized <- SparkR:::readInt(inputCon)

# read the isOutputSerialized bit flag
isOutputSerialized <- SparkR:::readInt(inputCon)

# Include packages as required
packageNames <- unserialize(SparkR:::readRaw(inputCon))
for (pkg in packageNames) {
  suppressPackageStartupMessages(require(as.character(pkg), character.only=TRUE))
}

# read function dependencies
funcLen <- SparkR:::readInt(inputCon)
computeFunc <- unserialize(SparkR:::readRawLen(inputCon, funcLen))
env <- environment(computeFunc)
parent.env(env) <- .GlobalEnv  # Attach under global environment.

# Read and set broadcast variables
numBroadcastVars <- SparkR:::readInt(inputCon)
if (numBroadcastVars > 0) {
  for (bcast in seq(1:numBroadcastVars)) {
    bcastId <- SparkR:::readInt(inputCon)
    value <- unserialize(SparkR:::readRaw(inputCon))
    setBroadcastValue(bcastId, value)
  }
}

# If -1: read as normal RDD; if >= 0, treat as pairwise RDD and treat the int
# as number of partitions to create.
numPartitions <- SparkR:::readInt(inputCon)

isEmpty <- SparkR:::readInt(inputCon)

if (isEmpty != 0) {

  if (numPartitions == -1) {
    if (isInputSerialized) {
      # Now read as many characters as described in funcLen
      data <- SparkR:::readDeserialize(inputCon)
    } else {
      data <- readLines(inputCon)
    }
    output <- computeFunc(splitIndex, data)
    if (isOutputSerialized) {
      SparkR:::writeRawSerialize(outputCon, output)
    } else {
      SparkR:::writeStrings(outputCon, output)
    }
  } else {
    if (isInputSerialized) {
      # Now read as many characters as described in funcLen
      data <- SparkR:::readDeserialize(inputCon)
    } else {
      data <- readLines(inputCon)
    }

    res <- new.env()

    # Step 1: hash the data to an environment
    hashTupleToEnvir <- function(tuple) {
      # NOTE: execFunction is the hash function here
      hashVal <- computeFunc(tuple[[1]])
      bucket <- as.character(hashVal %% numPartitions)
      acc <- res[[bucket]]
      # Create a new accumulator
      if (is.null(acc)) {
        acc <- SparkR:::initAccumulator()
      }
      SparkR:::addItemToAccumulator(acc, tuple)
      res[[bucket]] <- acc
    }
    invisible(lapply(data, hashTupleToEnvir))

    # Step 2: write out all of the environment as key-value pairs.
    for (name in ls(res)) {
      SparkR:::writeInt(outputCon, 2L)
      SparkR:::writeInt(outputCon, as.integer(name))
      # Truncate the accumulator list to the number of elements we have
      length(res[[name]]$data) <- res[[name]]$counter
      SparkR:::writeRawSerialize(outputCon, res[[name]]$data)
    }
  }
}

# End of output
if (isOutputSerialized) {
  SparkR:::writeInt(outputCon, 0L)
}

close(outputCon)
close(inputCon)

# Worker class

# NOTE: We use "stdin" to get the process stdin instead of the command line
inputConStdin  <- file("stdin", open = "rb")

outputFileName <- readLines(inputConStdin, n = 1)
outputCon <- file(outputFileName, open="wb")

# Set libPaths to include SparkR package as loadNamespace needs this
# TODO: Figure out if we can avoid this by not loading any objects that require
# SparkR namespace
rLibDir <- readLines(inputConStdin, n = 1)
.libPaths(c(rLibDir, .libPaths()))

suppressPackageStartupMessages(library(SparkR))

inFileName <- readLines(inputConStdin, n = 1)

inputCon <- file(inFileName, open = "rb")

# read the index of the current partition inside the RDD
splitIndex <- SparkR:::readInt(inputCon)

# read the function; if used for pairwise RRDD, this is the hash function.
execLen <- SparkR:::readInt(inputCon)
execFunctionName <- unserialize(SparkR:::readRawLen(inputCon, execLen))

# read the isInputSerialized bit flag
isInputSerialized <- SparkR:::readInt(inputCon)

# read the isOutputSerialized bit flag
isOutputSerialized <- SparkR:::readInt(inputCon)

# Redirect stdout to stderr to prevent print statements from
# interfering with outputStream
sink(stderr())

# Include packages as required
packageNames <- unserialize(SparkR:::readRaw(inputCon))
for (pkg in packageNames) {
  suppressPackageStartupMessages(require(as.character(pkg), character.only=TRUE))
}

# read function dependencies
depsLen <- SparkR:::readInt(inputCon)
if (depsLen > 0) {
  execFunctionDeps <- SparkR:::readRawLen(inputCon, depsLen)
  # load the dependencies into current environment
  load(rawConnection(execFunctionDeps, open='rb'))
}

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
    output <- do.call(execFunctionName, list(splitIndex, data))
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
      hashVal <- do.call(execFunctionName, list(tuple[[1]]))
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
unlink(inFileName)

# Restore stdout
sink()

# Finally print the name of the output file
cat(outputFileName, "\n")

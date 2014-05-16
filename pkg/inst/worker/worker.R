# Worker class

source_local <- function(fname) {
  argv <- commandArgs(trailingOnly = FALSE)
  base_dir <- dirname(substring(argv[grep("--file=", argv)], 8))
  source(paste(base_dir, fname, sep="/"))
}

source_local("serialize.R")

# NOTE: We use "stdin" to get the process stdin instead of the command line
inputCon  <- file("stdin", open = "rb")

outputFileName <- readLines(inputCon, n = 1)
outputCon <- file(outputFileName, open="wb")

# Set libPaths to include SparkR package as loadNamespace needs this
# TODO: Figure out if we can avoid this by not loading any objects that require
# SparkR namespace
rLibDir <- readLines(inputCon, n = 1)
.libPaths(c(rLibDir, .libPaths()))

suppressPackageStartupMessages(library(SparkR))

# read the index of the current partition inside the RDD
splitIndex <- readInt(inputCon)

# read the function; if used for pairwise RRDD, this is the hash function.
execLen <- readInt(inputCon)
execFunctionName <- unserialize(readRawLen(inputCon, execLen))

# read the isSerialized bit flag
isSerialized <- readInt(inputCon)

# Redirect stdout to stderr to prevent print statements from
# interfering with outputStream
sink(stderr())

# read function dependencies
depsLen <- readInt(inputCon)
if (depsLen > 0) {
  execFunctionDeps <- readRawLen(inputCon, depsLen)

  # load the dependencies into current environment
  depsFileName <- tempfile(pattern="spark-exec", fileext=".deps")
  depsFile <- file(depsFileName, open="wb")
  writeBin(execFunctionDeps, depsFile, endian="big")
  close(depsFile)
}

# Include packages as required
packageNames <- unserialize(readRaw(inputCon))
for (pkg in packageNames) {
  suppressPackageStartupMessages(require(as.character(pkg), character.only=TRUE))
}

if (depsLen > 0) {
	load(depsFileName)
	unlink(depsFileName)
}

# Read and set broadcast variables
numBroadcastVars <- readInt(inputCon)
if (numBroadcastVars > 0) {
  for (bcast in seq(1:numBroadcastVars)) {
    bcastId <- readInt(inputCon)
    value <- unserialize(readRaw(inputCon))
    setBroadcastValue(bcastId, value)
  }
}

# If -1: read as normal RDD; if >= 0, treat as pairwise RDD and treat the int
# as number of partitions to create.
numPartitions <- readInt(inputCon)

isEmpty <- readInt(inputCon)

if (isEmpty != 0) {

  if (numPartitions == -1) {
    if (isSerialized) {
      # Now read as many characters as described in funcLen
      data <- readDeserialize(inputCon)
    } else {
      data <- readLines(inputCon)
    }
    output <- do.call(execFunctionName, list(splitIndex, data))
    writeRaw(outputCon, output)
  } else {
    if (isSerialized) {
      # Now read as many characters as described in funcLen
      data <- readDeserialize(inputCon)
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
      # TODO?: http://stackoverflow.com/questions/2436688/append-an-object-to-a-list-in-r-in-amortized-constant-time
      acc[[length(acc) + 1]] <- tuple
      res[[bucket]] <- acc
    }
    invisible(lapply(data, hashTupleToEnvir))

    # Step 2: write out all of the environment as key-value pairs.
    for (name in ls(res)) {
      writeInt(outputCon, 2L)
      writeInt(outputCon, as.integer(name))
      writeRaw(outputCon, res[[name]])
    }
  }
}

# End of output
writeInt(outputCon, 0L)

close(outputCon)

# Restore stdout
sink()

# Finally print the name of the output file
cat(outputFileName, "\n")

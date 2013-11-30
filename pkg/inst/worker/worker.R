# Worker class

source_local <- function(fname) {
  argv <- commandArgs(trailingOnly = FALSE)
  base_dir <- dirname(substring(argv[grep("--file=", argv)], 8))
  source(paste(base_dir, fname, sep="/"))
}

source_local("serialize.R")

# Set libPaths to include SparkR package as loadNamespace needs this
# TODO: Figure out if we can avoid this by not loading any objects that require
# SparkR namespace
sparkHome <- Sys.getenv("SPARK_HOME")
.libPaths(c( .libPaths(), paste(sparkHome,"/R/lib", sep="")))

# NOTE: We use "stdin" to get the process stdin instead of the command line
inputCon  <- file("stdin", open = "rb")

outputFileName <- readLines(inputCon, n = 1)
outputCon <- file(outputFileName, open="wb")

# First read the function; if used for pairwise RRDD, this is the hash function.
execFunction <- unserialize(readRaw(inputCon))

# read the isSerialized bit flag
isSerialized <- readInt(inputCon)

# read function dependencies
execFunctionDeps <- readRaw(inputCon)

# load the dependencies into current environment
depsFileName <- tempfile(pattern="spark-exec", fileext=".deps")
depsFile <- file(depsFileName, open="wb")
writeBin(execFunctionDeps, depsFile, endian="big")
close(depsFile)
load(depsFileName, envir=environment(execFunction))
unlink(depsFileName)

# Redirect stdout to stderr to prevent print statements from
# interfering with outputStream
sink(stderr())

# If -1: read as normal RDD; if >= 0, treat as pairwise RDD and treat the int
# as number of elements to read next.
dataLen <- readInt(inputCon)

if (dataLen == -1) {

  if (isSerialized) {
    # Now read as many characters as described in funcLen
    data <- unserialize(readRaw(inputCon))
  } else {
    data <- readLines(inputCon)
  }
  output <- execFunction(data)
  writeRaw(outputCon, output)

} else {

  keyValPairs = list()

  for (i in 1:dataLen) {
    if (isSerialized) {
      key <- unserialize(readRaw(inputCon))
      val <- unserialize(readRaw(inputCon))
      keyValPairs[[length(keyValPairs) + 1]] <- list(key, val)
    } else {
      # FIXME?
      data <- readLines(inputCon)
    }
  }

  # Step 1: convert the environment into a list of lists.
  envirList <- as.list(execFunction(keyValPairs))
  keyed = list()
  for (key in names(envirList)) {
    bucketList <- list(as.integer(key), envirList[[key]])
    keyed[[length(keyed) + 1]] <- bucketList
  }

  # Step 2: write out all of the keyed list.
  for (keyedEntry in keyed) {
    writeInt(outputCon, 2L)
    writeRaw(outputCon, keyedEntry[[1]])
    writeRaw(outputCon, keyedEntry[[2]])
  }

}

# End of output
writeInt(outputCon, 0L)

close(outputCon)

# Restore stdout
sink()

# Finally print the name of the output file
cat(outputFileName, "\n")

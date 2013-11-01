# Worker class

# FIXME: refactor me and worker.R to reduce code duplication

source_local <- function(fname) {
  argv <- commandArgs(trailingOnly = FALSE)
  base_dir <- dirname(substring(argv[grep("--file=", argv)], 8))
  source(paste(base_dir, fname, sep="/"))
}

source_local("serialize.R")

# cat("***** In pairwiseWorder\n")

# Set libPaths to include SparkR package as loadNamespace needs this
# TODO: Figure out if we can avoid this by not loading any objects that require
# SparkR namespace
sparkHome <- Sys.getenv("SPARK_HOME")
.libPaths(c( .libPaths(), paste(sparkHome,"/R/lib", sep="")))

# NOTE: We use "stdin" to get the process stdin instead of the command line
inputCon  <- file("stdin", open = "rb")
#outputFileName <- tempfile(pattern="spark-exec", fileext=".out")

# cat("***** About to read outputFileName\n")

outputFileName <- readLines(inputCon, n = 1)
outputCon <- file(outputFileName, open="wb")

# cat("***** read name:\n")
# cat(outputFileName) # TODO: remove
# cat("\n")

# (1) read the hash function
hashFunc <- unserialize(readRaw(inputCon))
# (2) read the isSerialized bit flag
isSerialized <- readInt(inputCon)
# (3) read function dependencies
execFunctionDeps <- readRaw(inputCon)
depsFileName <- tempfile(pattern="spark-exec", fileext=".deps")
depsFile <- file(depsFileName, open="wb")
writeBin(execFunctionDeps, depsFile, endian="big")
close(depsFile)
load(depsFileName, envir=environment(hashFunc))
unlink(depsFileName)

# FIXME?: put this before sink?
# (4) read # of elements to read next

# Redirect stdout to stderr to prevent print statements from
# interfering with outputStream
sink(stderr())

keyValPairs = list()

dataLen <- readInt(inputCon)
while (dataLen > 0) {
# for (i in 1:dataLen) {
  if (isSerialized) {
    key <- unserialize(readBin(con, raw(), as.integer(dataLen), endian="big"))
    # key <- unserialize(readRaw(inputCon))
    val <- unserialize(readRaw(inputCon))
    keyValPairs[[length(keyValPairs) + 1]] <- list(key, val)
  } else {
    # FIXME
    data <- readLines(inputCon)
  }
  dataLen <- readInt(inputCon)
}

# Redirect stdout to stderr to prevent print statements from
# interfering with outputStream
sink(stderr())

# Step 1: turn the environment into a list of lists, starting with hashFunc.
envirList <- as.list(hashFunc(keyValPairs))
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
writeInt(outputCon, 0L) # End of output

#sink(stderr())
#print(execFunction)
#sink()

close(outputCon)

# Restore stdout
sink()

# Finally print the name of the output file
cat(outputFileName, "\n")

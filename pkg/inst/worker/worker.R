# Worker class

source_local <- function(fname) {
  argv <- commandArgs(trailingOnly = FALSE)
  base_dir <- dirname(substring(argv[grep("--file=", argv)], 8))
  source(paste(base_dir, fname, sep="/"))
}

source_local("serialize.R")

# NOTE: We use "stdin" to get the process stdin instead of the command line
inputCon  <- file("stdin", open = "rb")
outputFileName <- tempfile(pattern="spark-exec", fileext=".out")
outputCon <- file(outputFileName, open="wb")

# First read the function
execFunction <- unserialize(readRaw(inputCon))

isSerialized <- readInt(inputCon)

execFunctionDeps <- readRaw(inputCon)
depsFileName <- tempfile(pattern="spark-exec", fileext=".deps")
depsFile <- file(depsFileName, open="wb")
writeBin(execFunctionDeps, depsFile, endian="big")
close(depsFile)

load(depsFileName, envir=environment(execFunction))
unlink(depsFileName)

# Redirect stdout to stderr to prevent print statements from 
# interfering with outputStream
sink(stderr())

if (isSerialized) {
  # Now read as many characters as described in funcLen
  data <- unserialize(readRaw(inputCon))
} else {
  data <- readLines(inputCon)
}


#sink(stderr())
#print(execFunction)
#sink()

output <- execFunction(data)

writeRaw(outputCon, output)
writeInt(outputCon, 0L)

close(outputCon)

# Restore stdout
sink()

# Finally print the name of the output file
cat(outputFileName, "\n")

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Worker class

# Get current system time
currentTimeSecs <- function() {
  as.numeric(Sys.time())
}

# Get elapsed time
elapsedSecs <- function() {
  proc.time()[3]
}

# Constants
specialLengths <- list(END_OF_STERAM = 0L, TIMING_DATA = -1L)

# Timing R process boot
bootTime <- currentTimeSecs()
bootElap <- elapsedSecs()

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
partition <- SparkR:::readInt(inputCon)

deserializer <- SparkR:::readString(inputCon)
serializer <- SparkR:::readString(inputCon)

# Include packages as required
packageNames <- unserialize(SparkR:::readRaw(inputCon))
for (pkg in packageNames) {
  suppressPackageStartupMessages(library(as.character(pkg), character.only=TRUE))
}

# read function dependencies
funcLen <- SparkR:::readInt(inputCon)
computeFunc <- unserialize(SparkR:::readRawLen(inputCon, funcLen))
env <- environment(computeFunc)
parent.env(env) <- .GlobalEnv  # Attach under global environment.

# Timing init envs for computing
initElap <- elapsedSecs()

# Read and set broadcast variables
numBroadcastVars <- SparkR:::readInt(inputCon)
if (numBroadcastVars > 0) {
  for (bcast in seq(1:numBroadcastVars)) {
    bcastId <- SparkR:::readInt(inputCon)
    value <- unserialize(SparkR:::readRaw(inputCon))
    SparkR:::setBroadcastValue(bcastId, value)
  }
}

# Timing broadcast
broadcastElap <- elapsedSecs()

# If -1: read as normal RDD; if >= 0, treat as pairwise RDD and treat the int
# as number of partitions to create.
numPartitions <- SparkR:::readInt(inputCon)

isEmpty <- SparkR:::readInt(inputCon)

if (isEmpty != 0) {

  if (numPartitions == -1) {
    if (deserializer == "byte") {
      # Now read as many characters as described in funcLen
      data <- SparkR:::readDeserialize(inputCon)
    } else if (deserializer == "string") {
      data <- as.list(readLines(inputCon))
    } else if (deserializer == "row") {
      data <- SparkR:::readDeserializeRows(inputCon)
    }
    # Timing reading input data for execution
    inputElap <- elapsedSecs()

    output <- computeFunc(partition, data)
    # Timing computing
    computeElap <- elapsedSecs()

    if (serializer == "byte") {
      SparkR:::writeRawSerialize(outputCon, output)
    } else if (serializer == "row") {
      SparkR:::writeRowSerialize(outputCon, output)
    } else {
      # write lines one-by-one with flag
      lapply(output, function(line) SparkR:::writeString(outputCon, line))
    }
    # Timing output
    outputElap <- elapsedSecs()
  } else {
    if (deserializer == "byte") {
      # Now read as many characters as described in funcLen
      data <- SparkR:::readDeserialize(inputCon)
    } else if (deserializer == "string") {
      data <- readLines(inputCon)
    } else if (deserializer == "row") {
      data <- SparkR:::readDeserializeRows(inputCon)
    }
    # Timing reading input data for execution
    inputElap <- elapsedSecs()

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
    # Timing computing
    computeElap <- elapsedSecs()

    # Step 2: write out all of the environment as key-value pairs.
    for (name in ls(res)) {
      SparkR:::writeInt(outputCon, 2L)
      SparkR:::writeInt(outputCon, as.integer(name))
      # Truncate the accumulator list to the number of elements we have
      length(res[[name]]$data) <- res[[name]]$counter
      SparkR:::writeRawSerialize(outputCon, res[[name]]$data)
    }
    # Timing output
    outputElap <- elapsedSecs()
  }
} else {
  inputElap <- broadcastElap
  computeElap <- broadcastElap
  outputElap <- broadcastElap
}

# Report timing
SparkR:::writeInt(outputCon, specialLengths$TIMING_DATA)
SparkR:::writeDouble(outputCon, bootTime)
SparkR:::writeDouble(outputCon, initElap - bootElap)        # init
SparkR:::writeDouble(outputCon, broadcastElap - initElap)   # broadcast
SparkR:::writeDouble(outputCon, inputElap - broadcastElap)  # input
SparkR:::writeDouble(outputCon, computeElap - inputElap)    # compute
SparkR:::writeDouble(outputCon, outputElap - computeElap)   # output

# End of output
SparkR:::writeInt(outputCon, specialLengths$END_OF_STERAM)

close(outputCon)
close(inputCon)

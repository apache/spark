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

# Worker daemon

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
connectionTimeout <- as.integer(Sys.getenv("SPARKR_BACKEND_CONNECTION_TIMEOUT", "6000"))
dirs <- strsplit(rLibDir, ",")[[1]]
script <- file.path(dirs[[1]], "SparkR", "worker", "worker.R")

# preload SparkR package, speedup worker
.libPaths(c(dirs, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(
    port = port, open = "wb", blocking = TRUE, timeout = connectionTimeout)

SparkR:::doServerAuth(inputCon, Sys.getenv("SPARKR_WORKER_SECRET"))

# Waits indefinitely for a socket connecion by default.
selectTimeout <- NULL

while (TRUE) {
  ready <- socketSelect(list(inputCon), timeout = selectTimeout)

  # Note that the children should be terminated in the parent. If each child terminates
  # itself, it appears that the resource is not released properly, that causes an unexpected
  # termination of this daemon due to, for example, running out of file descriptors
  # (see SPARK-21093). Therefore, the current implementation tries to retrieve children
  # that are exited (but not terminated) and then sends a kill signal to terminate them properly
  # in the parent.
  #
  # There are two paths that it attempts to send a signal to terminate the children in the parent.
  #
  #   1. Every second if any socket connection is not available and if there are child workers
  #     running.
  #   2. Right after a socket connection is available.
  #
  # In other words, the parent attempts to send the signal to the children every second if
  # any worker is running or right before launching other worker children from the following
  # new socket connection.

  # The process IDs of exited children are returned below.
  children <- parallel:::selectChildren(timeout = 0)

  if (is.integer(children)) {
    lapply(children, function(child) {
      # This should be the PIDs of exited children. Otherwise, this returns raw bytes if any data
      # was sent from this child. In this case, we discard it.
      pid <- parallel:::readChild(child)
      if (is.integer(pid)) {
        # This checks if the data from this child is the same pid of this selected child.
        if (child == pid) {
          # If so, we terminate this child.
          tools::pskill(child, tools::SIGUSR1)
        }
      }
    })
  } else if (is.null(children)) {
    # If it is NULL, there are no children. Waits indefinitely for a socket connecion.
    selectTimeout <- NULL
  }

  if (ready) {
    port <- SparkR:::readInt(inputCon)
    # There is a small chance that it could be interrupted by signal, retry one time
    if (length(port) == 0) {
      port <- SparkR:::readInt(inputCon)
      if (length(port) == 0) {
        cat("quitting daemon\n")
        quit(save = "no")
      }
    }
    p <- parallel:::mcfork()
    if (inherits(p, "masterProcess")) {
      # Reach here because this is a child process.
      close(inputCon)
      Sys.setenv(SPARKR_WORKER_PORT = port)
      try(source(script))
      # Note that this mcexit does not fully terminate this child.
      parallel:::mcexit(0L)
    } else {
      # Forking succeeded and we need to check if they finished their jobs every second.
      selectTimeout <- 1
    }
  }
}

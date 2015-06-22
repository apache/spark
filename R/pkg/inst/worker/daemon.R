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
script <- paste(rLibDir, "SparkR/worker/worker.R", sep = "/")

# preload SparkR package, speedup worker
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(port = port, open = "rb", blocking = TRUE, timeout = 3600)

while (TRUE) {
  ready <- socketSelect(list(inputCon))
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
      close(inputCon)
      Sys.setenv(SPARKR_WORKER_PORT = port)
      source(script)
      # Set SIGUSR1 so that child can exit
      tools::pskill(Sys.getpid(), tools::SIGUSR1)
      parallel:::mcexit(0L)
    }
  }
}

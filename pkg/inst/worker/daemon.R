# Worker daemon

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
script <- paste(rLibDir, "SparkR/worker/worker.R", sep="/")

# preload SparkR package, speedup worker
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(port = port, blocking = TRUE, open = "rb")

while (TRUE) {
  inport <- SparkR:::readInt(inputCon)
  if (length(inport) != 1) {
    quit(save="no")
  }
  p <- parallel:::mcfork()
  if (inherits(p, "masterProcess")) {
    close(inputCon)
    Sys.setenv(SPARKR_WORKER_PORT = inport)
    source(script)
    parallel:::mcexit(0)
  }
}

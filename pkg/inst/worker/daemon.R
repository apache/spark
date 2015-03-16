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

# Worker daemon

# try to load `fork`, or install
FORK_URI <- "http://cran.r-project.org/src/contrib/Archive/fork/fork_1.2.4.tar.gz"
tryCatch(library(fork), error = function(err) {
  install.packages(FORK_URI, repos = NULL, type = "source")
})

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
script <- paste(rLibDir, "SparkR/worker/worker.R", sep = "/")

# preload SparkR package, speedup worker
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(port = port, blocking = TRUE, open = "rb")

while (TRUE) {
  ready <- socketSelect(list(inputCon), timeout = 1)
  if (ready) {
    inport <- SparkR:::readInt(inputCon)
    if (length(inport) != 1) {
      quit(save="no")
    }
    p <- fork::fork(NULL)
    if (p == 0) {
      close(inputCon)
      Sys.setenv(SPARKR_WORKER_PORT = inport)
      source(script)
      fork::exit(0)
    }
  }
  # cleanup all the child process
  status <- fork::wait(0, nohang = TRUE)
  while (status[1] > 0) {
    status <- fork::wait(0, nohang = TRUE)
  }
}

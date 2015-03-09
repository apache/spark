# Worker daemon

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
script <- paste(rLibDir, "SparkR/worker/worker.R", sep = "/")

# preload SparkR package, speedup worker
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(port = port, open = "rb", blocking = TRUE, timeout = 3600)

# Read from connection, retrying to read exactly 'size' bytes
readBinFull <- function(con, size) {
  # readBin() could be interruptted by signal, we have no way to tell
  # if it's interruptted or the socket is closed, so we retry at most
  # 20 times, to avoid the deadloop if the socket is closed
  c <- 1
  data <- readBin(con, raw(), size)
  while (length(data) < size && c < 20) {
    extra <- readBin(con, raw(), size - length(data))
    data <- c(data, extra)
    c <- c + 1
  }
  data
}

# Utility function to read the port with retry
# Returns -1 if the socket was closed
readPort <- function(con) {
  data <- readBinFull(con, 4L)
  if (length(data) != 4) {
    -1
  } else {
    rc <- rawConnection(data)
    ret <- SparkR:::readInt(rc)
    close(rc)
    ret
  }
}

while (TRUE) {
  port <- readPort(inputCon)
  if (port < 0) {
    cat("quitting daemon\n")
    quit(save = "no")
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

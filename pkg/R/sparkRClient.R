# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
init <- function(hostname, port, timeout=60) {
  if (exists(".sparkRcon", envir=.sparkREnv)) {
    cat("SparkRBackend client connection already exists\n")
    return(get(".sparkRcon", envir=.sparkREnv))
  }

  con <- socketConnection(host=hostname, port=port, server=FALSE,
                          blocking=TRUE, open="wb", timeout=timeout)

  assign(".sparkRCon", con, envir=.sparkREnv)
  get(".sparkRCon", envir=.sparkREnv)
}

launchBackend <- function(
    jar, 
    mainClass, 
    args, 
    javaOpts="-Xms2g -Xmx2g",
    javaHome=Sys.getenv("JAVA_HOME")) {
  if (javaHome != "") {
    java_bin <- paste(javaHome, "bin", "java", sep="/")
  } else {
    java_bin <- "java"
  }
  command <- paste(java_bin, javaOpts, "-cp", jar, mainClass, args, sep=" ")
  cat("Launching java with command ", command, "\n")
  invisible(system(command, intern=FALSE, ignore.stdout=F, ignore.stderr=F, wait=F))
}

invokeJava <- function(rpcName, ...) {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }
  
  rc <- rawConnection(raw(0), "r+")
  
  writeString(rpcName)
  writeList(list(...))

  bytesToSend <- rawConnectionValue(rc)
  conn <- get(".sparkRCon", .sparkREnv)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)
  
  # TODO: check the status code to output error information
  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)
  readObject(conn)
} 
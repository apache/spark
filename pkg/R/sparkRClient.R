# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
connectBackend <- function(hostname, port, timeout=60) {
  if (exists(".sparkRcon", envir=.sparkREnv)) {
    cat("SparkRBackend client connection already exists\n")
    return(get(".sparkRcon", envir=.sparkREnv))
  }

  con <- socketConnection(host=hostname, port=port, server=FALSE,
                          blocking=TRUE, open="wb", timeout=timeout)

  assign(".sparkRCon", con, envir=.sparkREnv)
  get(".sparkRCon", envir=.sparkREnv)
}

# Launch the SparkR backend using a call to 'system'.
launchBackend <- function(
    classPath, 
    mainClass, 
    args, 
    javaOpts="-Xms2g -Xmx2g",
    javaHome=Sys.getenv("JAVA_HOME")) {
  if (javaHome != "") {
    java_bin <- paste(javaHome, "bin", "java", sep="/")
  } else {
    java_bin <- "java"
  }
  command <- paste(java_bin, javaOpts, "-cp", classPath, mainClass, args, sep=" ")
  cat("Launching java with command ", command, "\n")
  invisible(system(command, intern=FALSE, ignore.stdout=F, ignore.stderr=F, wait=F))
}

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

# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
connectBackend <- function(hostname, port, timeout) {
  if (exists(".sparkRcon", envir = .sparkREnv)) {
    if (isOpen(.sparkREnv[[".sparkRCon"]])) {
      cat("SparkRBackend client connection already exists\n")
      return(get(".sparkRcon", envir = .sparkREnv))
    }
  }

  con <- socketConnection(host = hostname, port = port, server = FALSE,
                          blocking = TRUE, open = "wb", timeout = timeout)

  assign(".sparkRCon", con, envir = .sparkREnv)
  con
}

determineSparkSubmitBin <- function() {
  if (.Platform$OS.type == "unix") {
    sparkSubmitBinName <- "spark-submit"
  } else {
    sparkSubmitBinName <- "spark-submit2.cmd"
  }
  sparkSubmitBinName
}

generateSparkSubmitArgs <- function(args, sparkHome, jars, sparkSubmitOpts, packages) {
  jars <- paste0(jars, collapse = ",")
  if (jars != "") {
    # construct the jars argument with a space between --jars and comma-separated values
    jars <- paste0("--jars ", jars)
  }

  packages <- paste0(packages, collapse = ",")
  if (packages != "") {
    # construct the packages argument with a space between --packages and comma-separated values
    packages <- paste0("--packages ", packages)
  }

  combinedArgs <- paste(jars, packages, sparkSubmitOpts, args, sep = " ")
  combinedArgs
}

checkJavaVersion <- function() {
  javaBin <- "java"
  javaHome <- Sys.getenv("JAVA_HOME")
  if (javaHome != "") {
    javaBin <- file.path(javaHome, javaBin)
  }

  # If java is missing from PATH, we get an error in Unix and a warning in Windows
  javaVersionOut <- tryCatch(
      launchScript(javaBin, "-version", wait = TRUE, stdout = TRUE, stderr = TRUE),
                   error = function(e) {
                     stop("Java version check failed. Please make sure Java is installed",
                          " and set JAVA_HOME to point to the installation directory.")
                   },
                   warning = function(w) {
                     stop("Java version check failed. Please make sure Java is installed",
                          " and set JAVA_HOME to point to the installation directory.")
                   })
  javaVersionStr <- strsplit(javaVersionOut[[1]], "[\"]")[[1L]][2]
  javaVersionNum <- as.numeric(paste0(strsplit(javaVersionStr, "[.]")[[1L]][1:2], collapse = "."))
  if(javaVersionNum < 1.8) {
    stop(paste("Java 8, or greater, is required for this package; found version:", javaVersionNum))
  }
}

launchBackend <- function(args, sparkHome, jars, sparkSubmitOpts, packages) {
  sparkSubmitBinName <- determineSparkSubmitBin()
  if (sparkHome != "") {
    sparkSubmitBin <- file.path(sparkHome, "bin", sparkSubmitBinName)
  } else {
    sparkSubmitBin <- sparkSubmitBinName
  }

  combinedArgs <- generateSparkSubmitArgs(args, sparkHome, jars, sparkSubmitOpts, packages)
  cat("Launching java with spark-submit command", sparkSubmitBin, combinedArgs, "\n")
  invisible(launchScript(sparkSubmitBin, combinedArgs))
}

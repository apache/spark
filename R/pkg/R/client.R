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
connectBackend <- function(hostname, port, timeout, authSecret) {
  if (exists(".sparkRcon", envir = .sparkREnv)) {
    if (isOpen(.sparkREnv[[".sparkRCon"]])) {
      cat("SparkRBackend client connection already exists\n")
      return(get(".sparkRcon", envir = .sparkREnv))
    }
  }

  con <- socketConnection(host = hostname, port = port, server = FALSE,
                          blocking = TRUE, open = "wb", timeout = timeout)
  doServerAuth(con, authSecret)
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
  javaReqs <- utils::packageDescription(utils::packageName(), fields = c("SystemRequirements"))
  sparkJavaVersion <- as.numeric(tail(strsplit(javaReqs, "[(=)]")[[1]], n = 1L))
  if (javaHome != "") {
    javaBin <- file.path(javaHome, "bin", javaBin)
  }

  # If java is missing from PATH, we get an error in Unix and a warning in Windows
  javaVersionOut <- tryCatch(
    if (is_windows()) {
      # See SPARK-24535
      system2(javaBin, "-version", wait = TRUE, stdout = TRUE, stderr = TRUE)
    } else {
      launchScript(javaBin, "-version", wait = TRUE, stdout = TRUE, stderr = TRUE)
    },
    error = function(e) {
      stop("Java version check failed. Please make sure Java is installed",
           " and set JAVA_HOME to point to the installation directory.", e)
    },
    warning = function(w) {
      stop("Java version check failed. Please make sure Java is installed",
          " and set JAVA_HOME to point to the installation directory.", w)
    })
  javaVersionFilter <- Filter(
      function(x) {
        grepl(" version", x)
      }, javaVersionOut)

  javaVersionStr <- strsplit(javaVersionFilter[[1]], "[\"]")[[1L]][2]
  # javaVersionStr is of the form 1.8.0_92.
  # Extract 8 from it to compare to sparkJavaVersion
  javaVersionNum <- as.integer(strsplit(javaVersionStr, "[.]")[[1L]][2])
  if (javaVersionNum != sparkJavaVersion) {
    stop(paste("Java version", sparkJavaVersion, "is required for this package; found version:",
               javaVersionStr))
  }
  return(javaVersionNum)
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

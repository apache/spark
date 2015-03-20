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
connectBackend <- function(hostname, port, timeout = 6000) {
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

launchBackend <- function(args, sparkHome, jars, sparkSubmitOpts) {
  if (.Platform$OS.type == "unix") {
    sparkSubmitBinName = "spark-submit"
  } else {
    sparkSubmitBinName = "spark-submit.cmd"
  }

  if (sparkHome != "") {
    sparkSubmitBin <- file.path(sparkHome, "bin", sparkSubmitBinName)
  } else {
    sparkSubmitBin <- sparkSubmitBinName
  }

  if (jars != "") {
    jars <- paste("--jars", jars)
  }

  combinedArgs <- paste(jars, sparkSubmitOpts, args, sep = " ")
  cat("Launching java with spark-submit command", sparkSubmitBin, combinedArgs, "\n")
  invisible(system2(sparkSubmitBin, combinedArgs, wait = F))
}

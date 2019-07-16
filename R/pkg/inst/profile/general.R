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

.First <- function() {
  if (utils::compareVersion(paste0(R.version$major, ".", R.version$minor), "3.4.0") == -1) {
    warning("Support for R prior to version 3.4 is deprecated since Spark 3.0.0")
  }

  packageDir <- Sys.getenv("SPARKR_PACKAGE_DIR")
  dirs <- strsplit(packageDir, ",")[[1]]
  .libPaths(c(dirs, .libPaths()))
  Sys.setenv(NOAWT = 1)
}

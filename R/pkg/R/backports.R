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

# backport from R 4.0.0
if (!exists('deparse1', asNamespace('base'))) {
  deparse1 = function (expr, collapse = " ", width.cutoff = 500L, ...)
    paste(deparse(expr, width.cutoff, ...), collapse = collapse)
}

# backport from R 3.2.0
if (!exists('trimws', asNamespace('base'))) {
  trimws = function (x, which = c("both", "left", "right"), whitespace = "[ \t\r\n]") {
    which <- match.arg(which)
    mysub <- function(re, x) sub(re, "", x, perl = TRUE)
    switch(
      which,
      left = mysub(paste0("^", whitespace, "+"), x),
      right = mysub(paste0(whitespace, "+$"), x),
      both = mysub(paste0(whitespace, "+$"), mysub(paste0("^", whitespace, "+"), x))
    )
  }
}

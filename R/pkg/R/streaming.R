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

# streaming.R - Structured Streaming / StreamingQuery class and methods implemented in S4 OO classes

#' @include generics.R jobj.R
NULL

#' S4 class that represents a StreamingQuery
#'
#' StreamingQuery can be created by using readStream()
#'
#' @rdname StreamingQuery
#' @seealso \link{readStream}
#'
#' @param ssq A Java object reference to the backing Scala StreamingQuery
#' @export
#' @note StreamingQuery since 2.2.0
#' @note experimental
setClass("StreamingQuery",
         slots = list(ssq = "jobj"))

setMethod("initialize", "StreamingQuery", function(.Object, ssq) {
  .Object@ssq <- ssq
  .Object
})

streamingQuery <- function(ssq) {
  stopifnot(class(ssq) == "jobj")
  new("StreamingQuery", ssq)
}

setMethod("show", "StreamingQuery",
          function(object) {
            name <- callJMethod(object@ssq, "name")
            if (!is.null(name)) {
              cat("StreamingQuery", name, "\n")
            } else {
              cat("StreamingQuery", "[No queryName]", "\n")
            }
          })

# https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/streaming/StreamingQuery.scala
# explain(bool), awaitTermination, processAllAvailable, lastProgress, status, isActive
# ?? exception? check status instead; runId, id

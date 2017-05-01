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
#' StreamingQuery can be created by using read.stream() and write.stream()
#'
#' @rdname StreamingQuery
#' @seealso \link{read.stream}
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

#' @rdname show
#' @export
#' @note show(StreamingQuery) since 2.2.0
setMethod("show", "StreamingQuery",
          function(object) {
            name <- callJMethod(object@ssq, "name")
            if (!is.null(name)) {
              cat(paste0("StreamingQuery '", name, "'\n"))
            } else {
              cat("StreamingQuery", "\n")
            }
          })

#' queryName
#'
#' Returns the user-specified name of the query. This is specified in
#' \code{write.stream(df, queryName = "query")}. This name, if set, must be unique across all active
#' queries.
#'
#' @param x a StreamingQuery.
#' @return The name of the query, or NULL if not specified.
#' @rdname queryName
#' @name queryName
#' @aliases queryName,StreamingQuery-method
#' @family StreamingQuery methods
#' @seealso \link{write.stream}
#' @export
#' @examples
#' \dontrun{ queryName(sq) }
#' @note queryName(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("queryName",
          signature(x = "StreamingQuery"),
          function(x) {
            callJMethod(x@ssq, "name")
          })

#' @rdname explain
#' @name explain
#' @aliases explain,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ explain(sq) }
#' @note explain(StreamingQuery) since 2.2.0
setMethod("explain",
          signature(x = "StreamingQuery"),
          function(x, extended = FALSE) {
            cat(callJMethod(x@ssq, "explainInternal", extended), "\n")
          })

#' lastProgress
#'
#' Prints the most recent progess update of this streaming query in JSON format.
#'
#' @param x a StreamingQuery.
#' @rdname lastProgress
#' @name lastProgress
#' @aliases lastProgress,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ lastProgress(sq) }
#' @note lastProgress(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("lastProgress",
          signature(x = "StreamingQuery"),
          function(x) {
            p <- callJMethod(x@ssq, "lastProgress")
            if (is.null(p)) {
              cat("Streaming query has no progress")
            } else {
              cat(callJMethod(p, "toString"), "\n")
            }
          })

#' status
#'
#' Prints the current status of the query in JSON format.
#'
#' @param x a StreamingQuery.
#' @rdname status
#' @name status
#' @aliases status,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ status(sq) }
#' @note status(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("status",
          signature(x = "StreamingQuery"),
          function(x) {
            cat(callJMethod(callJMethod(x@ssq, "status"), "toString"), "\n")
          })

#' isActive
#'
#' Returns TRUE if this query is actively running.
#'
#' @param x a StreamingQuery.
#' @return TRUE if query is actively running, FALSE if stopped.
#' @rdname isActive
#' @name isActive
#' @aliases isActive,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ isActive(sq) }
#' @note isActive(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("isActive",
          signature(x = "StreamingQuery"),
          function(x) {
            callJMethod(x@ssq, "isActive")
          })

#' awaitTermination
#'
#' Waits for the termination of the query, either by \code{stopQuery} or by an error.
#'
#' If the query has terminated, then all subsequent calls to this method will return TRUE
#' immediately.
#'
#' @param x a StreamingQuery.
#' @param timeout time to wait in milliseconds, if omitted, wait indefinitely until \code{stopQuery}
#'                is called or an error has occured.
#' @return TRUE if query has terminated within the timeout period; nothing if timeout is not
#'         specified.
#' @rdname awaitTermination
#' @name awaitTermination
#' @aliases awaitTermination,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ awaitTermination(sq, 10000) }
#' @note awaitTermination(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("awaitTermination",
          signature(x = "StreamingQuery"),
          function(x, timeout = NULL) {
            if (is.null(timeout)) {
              invisible(handledCallJMethod(x@ssq, "awaitTermination"))
            } else {
              handledCallJMethod(x@ssq, "awaitTermination", as.integer(timeout))
            }
          })

#' stopQuery
#'
#' Stops the execution of this query if it is running. This method blocks until the execution is
#' stopped.
#'
#' @param x a StreamingQuery.
#' @rdname stopQuery
#' @name stopQuery
#' @aliases stopQuery,StreamingQuery-method
#' @family StreamingQuery methods
#' @export
#' @examples
#' \dontrun{ stopQuery(sq) }
#' @note stopQuery(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("stopQuery",
          signature(x = "StreamingQuery"),
          function(x) {
            invisible(callJMethod(x@ssq, "stop"))
          })

# S4 class representing Broadcast variables

# Hidden environment that holds values for broadcast variables
# This will not be serialized / shipped by default
.broadcastNames <- new.env()
.broadcastValues <- new.env()
.broadcastIdToName <- new.env()

#' @title S4 class that represents a Broadcast variable
#' @description Broadcast variables can be created using the broadcast
#'              function from a \code{SparkContext}.
#' @rdname broadcast-class
#' @seealso broadcast 
#'
#' @param id Id of the backing Spark broadcast variable 
#' @export
setClass("Broadcast", slots = list(id = "character"))

#' @rdname broadcast-class
#' @param value Value of the broadcast variable
#' @param jBroadcastRef reference to the backing Java broadcast object
#' @param objName name of broadcasted object
#' @export
Broadcast <- function(id, value, jBroadcastRef, objName) {
  .broadcastValues[[id]] <- value
  .broadcastNames[[as.character(objName)]] <- jBroadcastRef
  .broadcastIdToName[[id]] <- as.character(objName)
  new("Broadcast", id = id)
}

#' @rdname broadcast
#' @aliases value,Broadcast-method
setMethod("value",
          signature(bcast = "Broadcast"),
          function(bcast) {
            if (exists(bcast@id, envir = .broadcastValues)) {
              get(bcast@id, envir = .broadcastValues)
            } else {
              NULL
            }
          })

#' Internal function to set values of a broadcast variable.
#'
#' This function is used internally by Spark to set the value of a broadcast
#' variable on workers. Not intended for use outside the package.
#'
#' @rdname broadcast-internal
#' @seealso broadcast, value 

#' @param bcastId The id of broadcast variable to set
#' @param value The value to be set
#' @export
setBroadcastValue <- function(bcastId, value) {
  bcastIdStr <- as.character(bcastId)
  .broadcastValues[[bcastIdStr]] <- value
}

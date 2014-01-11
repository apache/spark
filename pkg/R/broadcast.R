# S4 class representing Broadcast variables

# Hidden environment that holds values for broadcast variables
# This will not be serialized / shipped by default
.broadcastNames <- new.env()
.broadcastValues <- new.env()
.broadcastIdToName <- new.env()

setClass("Broadcast", slots = list(id = "character"))

Broadcast <- function(id, value, jBroadcastRef, objName) {
  .broadcastValues[[id]] <- value
  .broadcastNames[[as.character(objName)]] <- jBroadcastRef
  .broadcastIdToName[[id]] <- as.character(objName)
  new("Broadcast", id = id)
}

setGeneric("value", function(bcast) { standardGeneric("value") })

setMethod("value",
          signature(bcast = "Broadcast"),
          function(bcast) {
            if (exists(bcast@id, envir=.broadcastValues)) {
              get(bcast@id, envir=.broadcastValues)
            } else {
              NULL
            }
          })

# Package local function to set values on the worker side
setBroadcastValue <- function(bcastId, value) {
  bcastIdStr <- as.character(bcastId)
  .broadcastValues[[bcastIdStr]] <- value
}

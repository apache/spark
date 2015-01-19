# jobj.R
# Handler for a java object that exists on the backend.

jobj <- function(objId) {
  if (!is.character(objId)) stop("object id must be a character")
  structure(list(id=objId), class = "jobj")
}

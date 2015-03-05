############################## GroupedData ########################################

setClass("GroupedData",
         slots = list(env = "environment",
                      sgd = "jobj"))

setMethod("initialize", "GroupedData", function(.Object, sgd) {
  .Object@env <- new.env()
  .Object@sgd <- sgd
  .Object
})

groupedData <- function(sgd) {
  new("GroupedData", sgd)
}

setMethod("count",
          signature(x = "GroupedData"),
          function(x) {
            dataFrame(callJMethod(x@sgd, "count"))
          })

#' Agg
#'
#' Aggregates on the entire DataFrame without groups.
#'
#' df2 <- agg(df, <column> = <aggFunction>)
#' df2 <- agg(df, newColName = aggFunction(column))
#' @examples
#' \dontrun{
#'  df2 <- agg(df, age = "sum")  # new column name will be created as 'SUM(age#0)'
#'  df2 <- agg(df, ageSum = sum(df$age)) # Creates a new column named ageSum
#' }
setGeneric("agg", function (x, ...) { standardGeneric("agg") })

setMethod("agg",
          signature(x = "GroupedData"),
          function(x, ...) {
            cols = list(...)
            stopifnot(length(cols) > 0)
            if (is.character(cols[[1]])) {
              cols <- varargsToEnv(...)
              sdf <- callJMethod(x@sgd, "agg", cols)
            } else if (class(cols[[1]]) == "Column") {
              ns <- names(cols)
              if (!is.null(ns)) {
                for (n in ns) {
                  if (n != "") {
                    cols[[n]] = alias(cols[[n]], n)
                  }
                }
              }
              jcols <- lapply(cols, function(c) { c@jc })
              sdf <- callJMethod(x@sgd, "agg", jcols[[1]], listToSeq(jcols[-1]))
            } else {
              stop("agg can only support Column or character")
            }
            dataFrame(sdf)
          })

#' sum/mean/avg/min/max

methods <- c("sum", "mean", "avg", "min", "max")

createMethod <- function(name) {
  setMethod(name,
            signature(x = "GroupedData"),
            function(x, ...) {
              sdf <- callJMethod(x@sgd, name, toSeq(...))
              dataFrame(sdf)
            })
}

createMethods <- function() {
  for (name in methods) {
    createMethod(name)
  }
}

createMethods()


#' Column Class

#' @include jobj.R
NULL

setOldClass("jobj")

#' @title S4 class that represents a DataFrame column

#' @rdname column-class
#'
#' @param jc reference to JVM DataFrame column
#' @export
setClass("Column",
         slots = list(jc = "jobj"))

setMethod("initialize", "Column", function(.Object, jc) {
  .Object@jc <- jc
  .Object
})

column <- function(jc) {
  new("Column", jc)
}

# TODO: change Dsl to functions once update spark-sql
col <- function(x) {
  column(callJStatic("org.apache.spark.sql.Dsl", "col", x))
}

alias <- function(col, name) {
  column(callJMethod(col@jc, "as", name))
}

cast <- function(col, dataType) {
  # TODO(davies): support DataType
  if (class(dataType) == "character") {
    column(callJMethod(col@jc, "cast", dataType))
  }
}

#TODO(davies): like, rlike, startwith, substr, isNull, isNotNull, getField, getItem

operators <- list(
  "+" = "plus", "-" = "minus", "*" = "multiply", "/" = "divide", "%%" = "mod",
  "==" = "equalTo", ">" = "gt", "<" = "lt", "!=" = "notEqual", "<=" = "leq", ">=" = "geq",
  # we can not override `&&` and `||`, so use `&` and `|` instead
  "&" = "and", "|" = "or"
)

createOperator <- function(op) {
  setMethod(op,
            signature(e1 = "Column"),
            function(e1, e2) {
              if (class(e2) == "Column") {
                e2 <- e2@jc
              }
              jc <- callJMethod(e1@jc, operators[[op]], e2)
              column(jc)
            })
}

for (op in names(operators)) {
  createOperator(op)
}


createFunction <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              jc <- callJStatic("org.apache.spark.sql.Dsl", name, x@jc)
              column(jc)
            })
}

setGeneric("avg", function(x) { standardGeneric("avg") })
setGeneric("last", function(x) { standardGeneric("last") })
setGeneric("asc", function(x) { standardGeneric("asc") })
setGeneric("desc", function(x) { standardGeneric("desc") })
setGeneric("lower", function(x) { standardGeneric("lower") })
setGeneric("upper", function(x) { standardGeneric("upper") })

Functions <- c("min", "max", "sum", "avg", "mean", "count", "abs", "sqrt",
 "first", "last", "asc", "desc", "lower", "upper")

for (name in Functions) {
  createFunction(name)
}

approxCountDistinct <- function(col, rsd) {
  jc <- callJStatic("org.apache.spark.sql.Dsl", "approxCountDistinct", col@jc, rsd)
  column(jc)
}

countDistinct <- function(col, ...) {
  jcol <- lapply(list(...), function (col) {
    col@jc
  })
  jc <- callJStatic("org.apache.spark.sql.Dsl", "countDistinct", col@jc, arrayToSeq(jcol))
  column(jc)
}

sumDistinct <- function(col) {
  jc <- callJStatic("org.apache.spark.sql.Dsl", "sumDistinct", x@jc)
  column(jc)
}


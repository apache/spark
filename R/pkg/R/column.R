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

col <- function(x) {
  column(callJStatic("org.apache.spark.sql.functions", "col", x))
}

setMethod("show", "Column",
          function(object) {
            cat("Column", callJMethod(object@jc, "toString"), "\n")
          })

# TODO(davies): like, rlike, startwith, substr, getField, getItem
operators <- list(
  "+" = "plus", "-" = "minus", "*" = "multiply", "/" = "divide", "%%" = "mod",
  "==" = "equalTo", ">" = "gt", "<" = "lt", "!=" = "notEqual", "<=" = "leq", ">=" = "geq",
  # we can not override `&&` and `||`, so use `&` and `|` instead
  "&" = "and", "|" = "or" #, "!" = "unary_$bang"
)

functions <- c("min", "max", "sum", "avg", "mean", "count", "abs", "sqrt",
               "first", "last", "lower", "upper", "sumDistinct",
               "isNull", "isNotNull")

createOperator <- function(op) {
  setMethod(op,
            signature(e1 = "Column"),
            function(e1, e2) {
              jc <- if (missing(e2)) {
                if (op == "-") {
                  callJMethod(e1@jc, "unary_$minus")
                } else {
                  callJMethod(e1@jc, operators[[op]])
                }
              } else {
                if (class(e2) == "Column") {
                  e2 <- e2@jc
                }
                callJMethod(e1@jc, operators[[op]], e2)
              }
              column(jc)
            })
}

createFunction <- function(name) {
  setMethod(name,
            signature(x = "Column"),
            function(x) {
              jc <- callJStatic("org.apache.spark.sql.functions", name, x@jc)
              column(jc)
            })
}

createMethods <- function() {
  for (op in names(operators)) {
    createOperator(op)
  }

  setGeneric("avg", function(x, ...) { standardGeneric("avg") })
  setGeneric("last", function(x) { standardGeneric("last") })
  setGeneric("lower", function(x) { standardGeneric("lower") })
  setGeneric("upper", function(x) { standardGeneric("upper") })
  setGeneric("isNull", function(x) { standardGeneric("isNull") })
  setGeneric("isNotNull", function(x) { standardGeneric("isNotNull") })
  setGeneric("sumDistinct", function(x) { standardGeneric("sumDistinct") })

  for (x in functions) {
    createFunction(x)
  }
}

createMethods()

setGeneric("asc", function(x) { standardGeneric("asc") })

setMethod("asc",
          signature(x = "Column"),
          function(x) {
            jc <- callJMethod(x@jc, "asc")
            column(jc)
          })

setGeneric("desc", function(x) { standardGeneric("desc") })

setMethod("desc",
          signature(x = "Column"),
          function(x) {
            jc <- callJMethod(x@jc, "desc")
            column(jc)
          })

setMethod("alias",
          signature(object = "Column"),
          function(object, data) {
            if (class(data) == "character") {
              column(callJMethod(object@jc, "as", data))
            } else {
              # TODO(davies): support DataType object
              stop("not implemented")
            }
          })

setGeneric("cast", function(x, dataType) { standardGeneric("cast") })

setMethod("cast",
          signature(x = "Column", dataType = "character"),
          function(x, dataType) {
            column(callJMethod(x@jc, "cast", dataType))
          })


setGeneric("approxCountDistinct", function(x, ...) { standardGeneric("approxCountDistinct") })

setMethod("approxCountDistinct",
          signature(x = "Column"),
          function(x, rsd = 0.95) {
            jc <- callJStatic("org.apache.spark.sql.functions", "approxCountDistinct", x@jc, rsd)
            column(jc)
          })

setGeneric("countDistinct", function(x, ...) { standardGeneric("countDistinct") })

setMethod("countDistinct",
          signature(x = "Column"),
          function(x, ...) {
            jcol <- lapply(list(...), function (x) {
              x@jc
            })
            jc <- callJStatic("org.apache.spark.sql.functions", "countDistinct", x@jc, listToSeq(jcol))
            column(jc)
          })


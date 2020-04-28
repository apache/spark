# backport from R 4.0.0
if (!exists('deparse1', asNamespace('base'))) {
  deparse1 = function (expr, collapse = " ", width.cutoff = 500L, ...)
    paste(deparse(expr, width.cutoff, ...), collapse = collapse)
}

# backport from R 3.2.0
if (!exists('trimws'), asNamespace('base')) {
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

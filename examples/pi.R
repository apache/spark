library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) < 1) {
  print("Usage: pi <master> [<slices>]")
  q("no")
}

sc <- sparkR.init(args[[1]], "PiR")

slices <- ifelse(length(args) > 1, as.integer(args[[2]]), 2)

n <- 100000 * slices

piFunc <- function(elem) {
  rands <- runif(n = 2, min = -1, max = 1)
  val <- ifelse((rands[1]^2 + rands[2]^2) < 1, 1.0, 0.0)
  val
}


piFuncVec <- function(elems) {
  message(length(elems))
  rands1 <- runif(n = length(elems), min = -1, max = 1)
  rands2 <- runif(n = length(elems), min = -1, max = 1)
  val <- ifelse((rands1^2 + rands2^2) < 1, 1.0, 0.0)
  sum(val)
}

rdd <- parallelize(sc, 1:n, slices)
count <- reduce(lapplyPartition(rdd, piFuncVec), sum)
cat("Pi is roughly", 4.0 * count / n, "\n")
cat("Num elements in RDD ", count(rdd), "\n")

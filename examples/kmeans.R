require(SparkR)

# Logistic regression in Spark.
# Note: unlike the example in Scala, a point here is represented as a vector of
# doubles.

parseVector <- function(line) {
  nums = strsplit(line, " ")[[1]]
  sapply(nums, as.double)
}

parseVectors <- function(lines) {
	lines = strsplit(as.character(lines) , " ", fixed = TRUE)
	split(matrix(as.numeric(unlist(lines)), ncol = length(lines[[1]])), 1:length(lines))
}


closestPoint <- function(p, centers) {
  bestIndex <- 0
  closest <- .Machine$double.xmax
  for (i in 1:length(centers)) {
    tempDist <- sum((p - centers[[i]]) ** 2)
    if (tempDist < closest) {
      closest <- tempDist
      bestIndex <- i
    }
  }
  bestIndex
}

# Main program

args <- commandArgs(trailing = TRUE)

if (length(args) != 4) {
  print("Usage: kmeans <master> <file> <K> <convergeDist>")
  q("no")
}

sc <- sparkR.init(args[[1]], "RKMeans")
K <- as.integer(args[[3]])
convergeDist <- as.double(args[[4]])

lines <- textFile(sc, args[[2]])
points <- cache(lapplyPartition(lines, parseVectors))
# kPoints <- take(points, K)
kPoints <- takeSample(points, FALSE, K, 16189L)
tempDist <- 1.0

while (tempDist > convergeDist) {
  closest <- lapply(points,
                    function(p) { list(closestPoint(p, kPoints), list(p, 1)) })

  pointStats <- reduceByKey(closest,
                            function(p1, p2) {
                              list(p1[[1]] + p2[[1]], p1[[2]] + p2[[2]])
                            },
                            2L)

  newPoints <- collect(lapply(pointStats,
                              function(tup) {
                                list(tup[[1]], tup[[2]][[1]] / tup[[2]][[2]])
                              }))

  tempDist <- sum(sapply(newPoints,
                         function(tup) {
                           sum((kPoints[[tup[[1]]]] - tup[[2]]) ** 2)
                         }))

  for (tup in newPoints)
    kPoints[[tup[[1]]]] <- tup[[2]]

  cat("Finished iteration (delta = ", tempDist, ")\n")
}

cat("Final centers:\n")
writeLines(unlist(lapply(kPoints, paste, collapse = " ")))

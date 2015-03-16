library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) != 4) {
  print("Usage: logistic_regression <master> <file> <iters> <dimension>")
  q("no")
}

# Initialize Spark context
sc <- sparkR.init(args[[1]], "LogisticRegressionR")
iterations <- as.integer(args[[3]])
D <- as.integer(args[[4]])

readPartition <- function(part){
  part = strsplit(part, " ", fixed = T)
  list(matrix(as.numeric(unlist(part)), ncol = length(part[[1]])))
}

# Read data points and convert each partition to a matrix
points <- cache(lapplyPartition(textFile(sc, args[[2]]), readPartition))

# Initialize w to a random value
w <- runif(n=D, min = -1, max = 1)
cat("Initial w: ", w, "\n")

# Compute logistic regression gradient for a matrix of data points
gradient <- function(partition) {
  partition = partition[[1]]
  Y <- partition[, 1]  # point labels (first column of input file)
  X <- partition[, -1] # point coordinates

  # For each point (x, y), compute gradient function
  dot <- X %*% w
  logit <- 1 / (1 + exp(-Y * dot))
  grad <- t(X) %*% ((logit - 1) * Y)
  list(grad)
}

for (i in 1:iterations) {
  cat("On iteration ", i, "\n")
  w <- w - reduce(lapplyPartition(points, gradient), "+")
}

cat("Final w: ", w, "\n")

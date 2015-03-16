library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) != 2) {
  print("Usage: wordcount <master> <file>")
  q("no")
}

# Initialize Spark context
sc <- sparkR.init(args[[1]], "RwordCount")
lines <- textFile(sc, args[[2]])

words <- flatMap(lines,
                 function(line) {
                   strsplit(line, " ")[[1]]
                 })
wordCount <- lapply(words, function(word) { list(word, 1L) })

counts <- reduceByKey(wordCount, "+", 2L)
output <- collect(counts)

for (wordcount in output) {
  cat(wordcount[[1]], ": ", wordcount[[2]], "\n")
}

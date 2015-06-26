
library(SparkR)

sc <- sparkR.init()

helloTest <- SparkR:::callJStatic("sparkR.test.hello",
                                  "helloWorld",
                                  "Dave")

basicFunction <- SparkR:::callJStatic("sparkR.test.basicFunction",
                                      "addStuff",
                                      2L,
                                      2L)

sparkR.stop()
output <- c(helloTest, basicFunction)
writeLines(output)

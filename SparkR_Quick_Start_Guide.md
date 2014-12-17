Quick Start
===

This tutorial provides a quick introduction to using SparkR. We will first introduce the API through SparkR’s interactive shell, then show how to write a standalone R application. 

To follow along with this guide, you'll need to install and configure both Spark and SparkR. If you're just starting out, we recommend [setting up a development environment] based on Cloudera's QuickStart VM. The VM approach will provide you with working versions of Hadoop, Spark, and SparkR while requiring minimal configuration.

Interactive Analysis with the SparkR Shell
===

Basics
---

SparkR's shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively. Start the shell by running the following in the SparkR-pkg directory of your VM:

```sh
> ./sparkR
```

Spark’s primary abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDDs can be created from Hadoop InputFormats (such as HDFS files) or by transforming other RDDs. Let’s make a new RDD from the text of the README file in the SparkR-pkg source directory:

```R
> textFile <- textFile(sc, "/home/cloudera/SparkR-pkg/README.md")
```

RDDs have [actions], which return values, and [transformations], which return pointers to new RDDs. Let’s start with a few actions:

```R
> count(textFile)
[1] 122

> take(textFile, 1)
[1] "# R on Spark"
```

Now let’s use a transformation. We will use the filterRDD transformation to return a new RDD with a subset of the items in the file.

```R
> linesWithSpark <- filterRDD(textFile, function(line){ grepl("Spark", line)})
```

We can chain together transformations and actions:

```R
> count(filterRDD(textFile, function(line){ grepl("Spark", line)})) # How many lines contain "Spark"?
[1] 35
```

More on RDD Operations
---

RDD actions and transformations can be used for more complex computations. Let’s say we want to find the line with the most words:

```R
> reduce( lapply( textFile, function(line) { length(strsplit(unlist(line), " ")[[1]])}), function(a, b) { if (a > b) { a } else { b }})
[1] 36
```

There are two functions here: `lapply` and `reduce`.  The inner function (`lapply`) maps a line to an integer value, creating a new RDD.  The outer function (`reduce`) is called on the new RDD to find the largest line count.  In this case, the arguments to both functions are passed as anonymous functions, but we can also define R functions beforehand as pass them as arguments to the RDD functions. For example, we’ll define a max function to make this code easier to understand:   

```R
> max <- function(a, b) {if (a > b) { a } else { b }}

> reduce(map(textFile, function(line) { length(strsplit(unlist(line), " ")[[1]])}), max)
[1] 36
```

One common data flow pattern is MapReduce, as popularized by Hadoop. MapReduce flows are easily implemented in SparkR:

```R
> words <- flatMap(textFile,
                 function(line) {
                   strsplit(line, " ")[[1]]
                   })
                   
> wordCount <- lapply(words, function(word){ list(word, 1L) })
  
> counts <- reduceByKey(wordCount, "+", 2L)
```
Here, we combined the flatMap, lapply and reduceByKey transformations to compute the per-word counts in the file as an RDD of (string, int) pairs. To collect the word counts in our shell, we can use the collect action:

```R
> output <- collect(counts)

> for (wordcount in output) {
    cat(wordcount[[1]], ": ", wordcount[[2]], "\n")
  }

SparkContext. :  1 
SparkContext, :  1 
master :  3 
executors :  1 
issues :  1 
frontend :  1 
variable :  3 
[...]
```

Caching
---

Spark also supports pulling data sets into a cluster-wide in-memory cache. This is very useful when data is accessed repeatedly, such as when querying a small “hot” dataset or when running an iterative algorithm like PageRank. As a simple example, let’s mark our linesWithSpark dataset to be cached:

```R
> cache(linesWithSpark)

> system.time(count(linesWithSpark))

   user  system elapsed 
  0.955   0.225   2.127 
            
> system.time(count(linesWithSpark))

   user  system elapsed 
  0.945   0.188   1.078 
```

It may seem silly to use Spark to explore and cache a 100-line text file. The interesting part is that these same functions can be used on very large data sets, even when they are striped across tens or hundreds of nodes. You can also do this interactively by connecting the SparkR shell to a cluster, an example of which is described in the [SparkR on EC2 wiki page].

Standalone Applications
===

Now we'll walk through the process of writing and executing a standalone in application in SparkR.  As an example, we'll create a simple R script, `SimpleApp.R`:

```R
library(SparkR)

sc <- sparkR.init(master="local")

logFile <- "/home/cloudera/SparkR-pkg/README.md"

logData <- cache(textFile(sc, logFile))

numAs <- count(filterRDD(logData, function(s) { grepl("a", s) }))
numBs <- count(filterRDD(logData, function(s) { grepl("b", s) }))

paste("Lines with a: ", numAs, ", Lines with b: ", numBs, sep="")
```
This program just counts the number of lines containing ‘a’ and the number containing ‘b’ in a text file and returns the counts as a string on the command line.  In this application, we use the `sparkR.init()` function to initialize a SparkContext which is then used to create RDDs.  We can pass R functions to Spark where they are automatically serialized along with any variables they reference.

To run this application, execute the following from the SparkR-pkg directory:

```sh
> ./sparkR examples/SimpleApp.R

[1] "Lines with a: 65, Lines with b: 32"
```

Where to Go from Here
===

Congratulations on running your first SparkR application!

For more information on SparkR, head to the [SparkR Wiki].

In addition, SparkR includes several samples in the `examples` directory.  To run one of them, use `./sparkR <filename> <args>`. For example:

```sh
./sparkR examples/pi.R local[2]
```

[setting up a development environment]: http://adventures.putler.org/blog/2014/12/08/Setting-Up-a-Virtual-Machine-with-SparkR/

[actions]: http://spark.apache.org/docs/latest/programming-guide.html#actions

[transformations]: http://spark.apache.org/docs/latest/programming-guide.html#transformations

[SparkR on EC2 wiki page]: https://github.com/amplab-extras/SparkR-pkg/wiki/SparkR-on-EC2

[SparkR Wiki]: https://github.com/amplab-extras/SparkR-pkg/wiki
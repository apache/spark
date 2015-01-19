### Notes and TODOs for SparkR Backend 

- Handle stderr, stdout of Java program better. We shouldn't probably put them on the shell, 
  but we need some way for users to look at the messages.
- Send back the reply length as the first field in the reply. 
  Enforce somehow that the second field should be the error code. Also we should add support
  to capture exceptions and send them in the reply.

Future work
- Can we refactor the serializers to have a more visitor-like pattern ?
  That will enable serializing UDFs etc. from R to Java and back.
  

### To try out the new Spark Context, run something like

```
./install-dev.sh
library(SparkR, lib.loc="./lib")
SparkR:::launchBackend("./pkg/src/target/scala-2.10/sparkr-assembly-0.1.jar",
                       "edu.berkeley.cs.amplab.sparkr.SparkRBackend", "12345")
Sys.sleep(5)
con <- SparkR:::init("localhost", 12345)
s <- SparkR:::createSparkContext("local", "SparkRJavaExpt", "", 
  list("./pkg/src/target/scala-2.10/sparkr-assembly-0.1.jar"))

# TextFile, cache, count
jrdd <- SparkR:::callJMethod(s, "textFile", "README.md", 2L)
jl <- SparkR:::callJMethod(jrdd, "count") # Should be 128
jrddCached <- SparkR:::callJMethod(jrdd, "cache")
jl <- SparkR:::callJMethod(jrdd, "count")  # Should show up on WebUI now

# Try out .jnew like call
hp <- SparkR:::newJava("org.apache.spark.HashPartitioner", 2L)

# Try out collect in steps
crs <- SparkR:::callJMethod(jrdd, "collect")
SparkR:::callJMethod(crs, "get", 2L)

# Stop backend
SparkR:::stopBackend()

# You should be able to go to localhost:4041 and see a SparkRJavaExpt as the app name
```
~

### Notes and TODOs for SparkR Backend 

- Send back the reply length as the first field in the reply. 
  Enforce somehow that the second field should be the error code ?
- Consider auto parsing all the arguments to RPCs on the R side
  i.e. take something like list(...) and serialize each argument.
- Consider using reflection on the Java side instead of explicit handlers

Future work
- Can we refactor the serializers to have a more visitor-like pattern ?
  That will enable serializing UDFs etc. from R to Java and back.
  

### To try out the new Spark Context, run something like

```
./install-dev.sh
library(SparkR, lib.loc="./lib")
SparkR:::launchBackend("./pkg/src/target/scala-2.10/sparkr-assembly-0.1.jar",
                       "edu.berkeley.cs.amplab.sparkr.SparkRBackend", "12345")
con <- SparkR:::init("localhost", 12345)
s <- SparkR:::createSparkContext("local", "SparkRJavaExpt", "", 
  c("./pkg/src/target/scala-2.10/sparkr-assembly-0.1.jar"))
# You should be able to go to localhost:4041 and see a SparkRJavaExpt as the app name
```
~

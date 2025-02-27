# Spark JVM Profiler Plugin

## Build

To build

```
./build/mvn clean package -DskipTests -Pjvm-profiler -pl :spark-profiler_2.13 -am
```

or

```
./build/sbt -Pjvm-profiler clean "profiler/package"
```

## Executor Code Profiling

The spark profiler module enables code profiling of executors in cluster mode based on the [async profiler](https://github.com/async-profiler/async-profiler/blob/v3.0/README.md), a low overhead sampling profiler. This allows a Spark application to capture CPU and memory profiles for application running on a cluster which can later be analyzed for performance issues. The profiler captures [Java Flight Recorder (jfr)](https://access.redhat.com/documentation/es-es/red_hat_build_of_openjdk/17/html/using_jdk_flight_recorder_with_red_hat_build_of_openjdk/openjdk-flight-recorded-overview) files for each executor; these can be read by many tools including Java Mission Control and Intellij.

The profiler writes the jfr files to the executor's working directory in the executor's local file system and the files can grow to be large, so it is advisable that the executor machines have adequate storage. The profiler can be configured to copy the jfr files to a hdfs location before the executor shuts down.

Code profiling is currently only supported for

*   Linux (x64)
*   Linux (arm64)
*   Linux (musl, x64)
*   MacOS

To get maximum profiling information set the following jvm options for the executor :

```
spark.executor.extraJavaOptions=-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer
```

For more information on async_profiler see the [Async Profiler Manual](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html)


To enable code profiling, first enable the code profiling plugin via

```
spark.plugins=org.apache.spark.profiler.ProfilerPlugin
```

Then enable the profiling in the configuration.


### Code profiling configuration

<table class="spark-config">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.profiler.driver.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, turn on profiling in driver.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, turn on profiling in executors.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.executor.fraction</code></td>
  <td>0.10</td>
  <td>
    The fraction of executors on which to enable profiling. The executors to be profiled are picked at random.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.dfsDir</code></td>
  <td>(none)</td>
  <td>
      An HDFS compatible path to which the profiler's output files are copied. The output files will be written as <i>dfsDir/{{APP_ID}}/profile-exec-{{EXECUTOR_ID}}.jfr</i> <br/>
      If no <i>dfsDir</i> is specified then the files are not copied over. Users should ensure there is sufficient disk space available otherwise it may lead to corrupt jfr files.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.localDir</code></td>
  <td><code>.</code> i.e. the executor's working dir</td>
  <td>
   The local directory in the executor container to write the jfr files to. If not specified the file will be written to the executor's working directory. Users should ensure there is sufficient disk space available on the system as running out of space may result in corrupt jfr file and even cause jobs to fail on systems like K8s.  
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.asyncProfiler.args</code></td>
  <td>event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s</td>
  <td>
      Arguments to pass to the Async Profiler. Detailed options are documented in the comments here:
      <a href="https://github.com/async-profiler/async-profiler/blob/v3.0/src/arguments.cpp#L44">Profiler arguments</a>.  
       Note that the arguments to start, stop, specify output format, and output file do not have to be specified.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.profiler.dfsWriteInterval</code></td>
  <td>30</td>
  <td>
    Time interval, in seconds, after which the profiler output will be synced to DFS.
  </td>
  <td>4.0.0</td>
</tr>
</table>

### Kubernetes
On Kubernetes, spark will try to shut down the executor pods while the profiler files are still being saved. To prevent this set
```
  spark.kubernetes.executor.deleteOnTermination=false
```

### Example
```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  -c spark.executor.extraJavaOptions="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer" \
  -c spark.plugins=org.apache.spark.profiler.ProfilerPlugin \
  -c spark.profiler.executor.enabled=true \
  -c spark.profiler.executor.fraction=0.10 \
  -c spark.profiler.dfsDir=s3a://my-bucket/spark/profiles/ \
  -c spark.profiler.asyncProfiler.args=event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s \
  -c spark.kubernetes.executor.deleteOnTermination=false \
  <application-jar> \
  [application-arguments]
```

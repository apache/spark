# Spark JVM Profiler Plugin

## Build

To build
```
  ./build/mvn clean package -DskipTests -Pjvm-profiler
```

## Executor Code Profiling

The spark profiler module enables code profiling of executors in cluster mode based on the the [async profiler](https://github.com/async-profiler/async-profiler/blob/v2.10/README.md), a low overhead sampling profiler. This allows a Spark application to capture CPU and memory profiles for application running on a cluster which can later be analyzed for performance issues. The profiler captures [Java Flight Recorder (jfr)](https://access.redhat.com/documentation/es-es/red_hat_build_of_openjdk/17/html/using_jdk_flight_recorder_with_red_hat_build_of_openjdk/openjdk-flight-recorded-overview) files for each executor; these can be read by many tools including Java Mission Control and Intellij.

The profiler writes the jfr files to the executor's working directory in the executor's local file system and the files can grow to be large so it is advisable that the executor machines have adequate storage. The profiler can be configured to copy the jfr files to a hdfs location before the executor shuts down.

Code profiling is currently only supported for

*   Linux (x64)
*   Linux (arm 64)
*   Linux (musl, x64)
*   MacOS

To get maximum profiling information set the following jvm options for the executor :

```
spark.executor.extraJavaOptions=-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer
```

For more information on async_profiler see the [Async Profiler Manual](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html)


To enable code profiling, first enable the code profiling plugin via

```
spark.plugins=org.apache.spark.executor.profiler.ExecutorProfilerPlugin
```

Then enable the profiling in the configuration.


### Code profiling configuration

<table class="spark-config">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.executor.profiling.enabled</code></td>
  <td><code>false</code></td>
  <td>
    If true, will enable code profiling
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.dfsDir</code></td>
  <td>(none)</td>
  <td>
      An HDFS compatible path to which the profiler's output files are copied. The output files will be written as <i>dfsDir/application_id/profile-appname-exec-executor_id.jfr</i> <br/>
      If no <i>dfsDir</i> is specified then the files are not copied over. Users should ensure there is sufficient disk space available otherwise it may lead to corrupt jfr files.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.localDir</code></td>
  <td><code>.</code> i.e. the executor's working dir</td>
  <td>
   The local directory in the executor container to write the jfr files to. If not specified the file will be written to the executor's working directory. Users should ensure there is sufficient disk space available on the system as running out of space may result in corrupt jfr file and even cause jobs to fail on systems like K8s.  
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.options</code></td>
  <td>event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s</td>
  <td>
      Options to pass to the profiler. Detailed options are documented in the comments here:
      <a href="https://github.com/async-profiler/async-profiler/blob/32601bccd9e49adda9510a2ed79d142ac6ef0ff9/src/arguments.cpp#L52">Profiler arguments</a>.  
       Note that the options to start, stop, specify output format, and output file do not have to be specified.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.fraction</code></td>
  <td>0.10</td>
  <td>
    The fraction of executors on which to enable code profiling. The executors to be profiled are picked at random.
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.writeInterval</code></td>
  <td>30</td>
  <td>
    Time interval, in seconds, after which the profiler output will be synced to dfs.
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
  -c spark.plugins=org.apache.spark.executor.profiler.ExecutorProfilerPlugin \
  -c spark.executor.profiling.enabled=true \
  -c spark.executor.profiling.dfsDir=s3a://my-bucket/spark/profiles/  \
  -c spark.executor.profiling.options=event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s \
  -c spark.executor.profiling.fraction=0.10  \
  -c spark.kubernetes.executor.deleteOnTermination=false \
  <application-jar> \
  [application-arguments]
```

# Spark Code Profiler Plugin

## Build

To build 
``` 
  ./build/mvn clean package -P code-profiler
```

## Executor Code Profiling

The spark profiler module enables code profiling of executors in cluster mode based on the the [async profiler](https://github.com/async-profiler/async-profiler/blob/master/README.md), a low overhead sampling profiler. This allows a Spark application to capture CPU and memory profiles for application running on a cluster which can later be analyzed for performance issues. The profiler captures [Java Flight Recorder (jfr)](https://developers.redhat.com/blog/2020/08/25/get-started-with-jdk-flight-recorder-in-openjdk-8u#) files for each executor; these can be read by many tools including Java Mission Control and Intellij.

The profiler writes the jfr files to the executor's working directory in the executor's local file system and the files can grow to be large so it is advisable that the executor machines have adequate storage. The profiler can be configured to copy the jfr files to a hdfs location before the executor shuts down.

Code profiling is currently only supported for

*   Linux (x64)
*   Linux (arm 64)
*   Linux (musl, x64)
*   MacOS

To get maximum profiling information set the following jvm options for the executor -

```
    -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer
```

For more information on async_profiler see the [Async Profiler Manual](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html)


To enable code profiling, first enable the code profiling plugin via

```
spark.plugins=org.apache.spark.executor.ExecutorProfilerPlugin
```

Then enable the profiling in the configuration.


### Code profiling configuration

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.executor.profiling.enabled</code></td>
  <td>
    <code>false</code>
  </td>
  <td>
    If true, will enable code profiling 
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.outputDir</code></td>
  <td></td>
  <td>
      An hdfs compatible path to which the profiler's output files are copied. The output files will be written as <i>outputDir/application_id/profile-appname-exec-executor_id.jfr</i> <br/>
      If no outputDir is specified then the files are not copied over. 
  </td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.executor.profiling.localDir</code></td>
  <td><code>.</code> i.e. the executor's working dir</td>
  <td>
   The local directory in the executor container to write the jfr files to. If not specified the file will be written to the executor's working directory. 
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
</table>

### Kubernetes
On Kubernetes, spark will try to shut down the executor pods while the profiler files are still being saved. To prevent this set 
```
  spark.kubernetes.executor.deleteOnTermination=false
```
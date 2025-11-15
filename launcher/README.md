# Spark Launcher

The Spark Launcher library provides a programmatic interface for launching Spark applications.

## Overview

The Launcher module allows you to:
- Launch Spark applications programmatically from Java/Scala code
- Monitor application state and output
- Manage Spark processes
- Build command-line arguments programmatically

This is an alternative to invoking `spark-submit` via shell commands.

## Key Components

### SparkLauncher

The main class for launching Spark applications.

**Location**: `src/main/java/org/apache/spark/launcher/SparkLauncher.java`

**Basic Usage:**
```java
import org.apache.spark.launcher.SparkLauncher;

SparkLauncher launcher = new SparkLauncher()
  .setAppResource("/path/to/app.jar")
  .setMainClass("com.example.MyApp")
  .setMaster("spark://master:7077")
  .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
  .setConf(SparkLauncher.EXECUTOR_MEMORY, "4g")
  .addAppArgs("arg1", "arg2");

Process spark = launcher.launch();
spark.waitFor();
```

### SparkAppHandle

Interface for monitoring launched applications.

**Location**: `src/main/java/org/apache/spark/launcher/SparkAppHandle.java`

**Usage:**
```java
import org.apache.spark.launcher.SparkAppHandle;

SparkAppHandle handle = launcher.startApplication();

// Add listener for state changes
handle.addListener(new SparkAppHandle.Listener() {
  @Override
  public void stateChanged(SparkAppHandle handle) {
    System.out.println("State: " + handle.getState());
  }
  
  @Override
  public void infoChanged(SparkAppHandle handle) {
    System.out.println("App ID: " + handle.getAppId());
  }
});

// Wait for completion
while (!handle.getState().isFinal()) {
  Thread.sleep(1000);
}
```

## API Reference

### Configuration Methods

```java
SparkLauncher launcher = new SparkLauncher();

// Application settings
launcher.setAppResource("/path/to/app.jar");
launcher.setMainClass("com.example.MainClass");
launcher.setAppName("MyApplication");

// Cluster settings
launcher.setMaster("spark://master:7077");
launcher.setDeployMode("cluster");

// Resource settings
launcher.setConf(SparkLauncher.DRIVER_MEMORY, "2g");
launcher.setConf(SparkLauncher.EXECUTOR_MEMORY, "4g");
launcher.setConf(SparkLauncher.EXECUTOR_CORES, "2");

// Additional configurations
launcher.setConf("spark.executor.instances", "5");
launcher.setConf("spark.sql.shuffle.partitions", "200");

// Dependencies
launcher.addJar("/path/to/dependency.jar");
launcher.addFile("/path/to/file.txt");
launcher.addPyFile("/path/to/module.py");

// Application arguments
launcher.addAppArgs("arg1", "arg2", "arg3");

// Environment
launcher.setSparkHome("/path/to/spark");
launcher.setPropertiesFile("/path/to/spark-defaults.conf");
launcher.setVerbose(true);
```

### Launch Methods

```java
// Launch and return Process handle
Process process = launcher.launch();

// Launch and return SparkAppHandle for monitoring
SparkAppHandle handle = launcher.startApplication();

// For child process mode (rare)
SparkAppHandle handle = launcher.startApplication(
  new SparkAppHandle.Listener() {
    // Listener implementation
  }
);
```

### Constants

Common configuration keys are available as constants:

```java
SparkLauncher.SPARK_MASTER          // "spark.master"
SparkLauncher.APP_RESOURCE          // "spark.app.resource"
SparkLauncher.APP_NAME              // "spark.app.name"
SparkLauncher.DRIVER_MEMORY         // "spark.driver.memory"
SparkLauncher.DRIVER_EXTRA_CLASSPATH // "spark.driver.extraClassPath"
SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS // "spark.driver.extraJavaOptions"
SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH // "spark.driver.extraLibraryPath"
SparkLauncher.EXECUTOR_MEMORY       // "spark.executor.memory"
SparkLauncher.EXECUTOR_CORES        // "spark.executor.cores"
SparkLauncher.EXECUTOR_EXTRA_CLASSPATH // "spark.executor.extraClassPath"
SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS // "spark.executor.extraJavaOptions"
SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH // "spark.executor.extraLibraryPath"
```

## Application States

The `SparkAppHandle.State` enum represents application lifecycle states:

- `UNKNOWN`: Initial state
- `CONNECTED`: Connected to Spark
- `SUBMITTED`: Application submitted
- `RUNNING`: Application running
- `FINISHED`: Completed successfully
- `FAILED`: Failed with error
- `KILLED`: Killed by user
- `LOST`: Connection lost

**Check if final:**
```java
if (handle.getState().isFinal()) {
  // Application has completed
}
```

## Examples

### Launch Scala Application

```java
import org.apache.spark.launcher.SparkLauncher;

public class LaunchSparkApp {
  public static void main(String[] args) throws Exception {
    Process spark = new SparkLauncher()
      .setAppResource("/path/to/app.jar")
      .setMainClass("com.example.SparkApp")
      .setMaster("local[2]")
      .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
      .launch();
    
    spark.waitFor();
    System.exit(spark.exitValue());
  }
}
```

### Launch Python Application

```java
SparkLauncher launcher = new SparkLauncher()
  .setAppResource("/path/to/app.py")
  .setMaster("yarn")
  .setDeployMode("cluster")
  .setConf(SparkLauncher.EXECUTOR_MEMORY, "4g")
  .addPyFile("/path/to/dependency.py")
  .addAppArgs("--input", "/data/input", "--output", "/data/output");

SparkAppHandle handle = launcher.startApplication();
```

### Monitor Application with Listener

```java
import org.apache.spark.launcher.SparkAppHandle;

class MyListener implements SparkAppHandle.Listener {
  @Override
  public void stateChanged(SparkAppHandle handle) {
    SparkAppHandle.State state = handle.getState();
    System.out.println("Application state changed to: " + state);
    
    if (state.isFinal()) {
      if (state == SparkAppHandle.State.FINISHED) {
        System.out.println("Application completed successfully");
      } else {
        System.out.println("Application failed: " + state);
      }
    }
  }
  
  @Override
  public void infoChanged(SparkAppHandle handle) {
    System.out.println("Application ID: " + handle.getAppId());
  }
}

// Use the listener
SparkAppHandle handle = new SparkLauncher()
  .setAppResource("/path/to/app.jar")
  .setMainClass("com.example.App")
  .setMaster("spark://master:7077")
  .startApplication(new MyListener());
```

### Capture Output

```java
import java.io.*;

Process spark = new SparkLauncher()
  .setAppResource("/path/to/app.jar")
  .setMainClass("com.example.App")
  .setMaster("local")
  .redirectOutput(ProcessBuilder.Redirect.PIPE)
  .redirectError(ProcessBuilder.Redirect.PIPE)
  .launch();

// Read output
BufferedReader reader = new BufferedReader(
  new InputStreamReader(spark.getInputStream())
);
String line;
while ((line = reader.readLine()) != null) {
  System.out.println(line);
}

spark.waitFor();
```

### Kill Running Application

```java
SparkAppHandle handle = launcher.startApplication();

// Later, kill the application
handle.kill();

// Or stop gracefully
handle.stop();
```

## In-Process Launcher

For testing or special cases, launch Spark in the same JVM:

```java
import org.apache.spark.launcher.InProcessLauncher;

InProcessLauncher launcher = new InProcessLauncher();
// Configure launcher...
SparkAppHandle handle = launcher.startApplication();
```

**Note**: This is primarily for testing. Production code should use `SparkLauncher`.

## Building and Testing

### Build Launcher Module

```bash
# Build launcher module
./build/mvn -pl launcher -am package

# Skip tests
./build/mvn -pl launcher -am -DskipTests package
```

### Run Tests

```bash
# Run all launcher tests
./build/mvn test -pl launcher

# Run specific test
./build/mvn test -pl launcher -Dtest=SparkLauncherSuite
```

## Source Code Organization

```
launcher/src/main/java/org/apache/spark/launcher/
├── SparkLauncher.java              # Main launcher class
├── SparkAppHandle.java             # Application handle interface
├── AbstractLauncher.java           # Base launcher implementation
├── InProcessLauncher.java          # In-process launcher (testing)
├── Main.java                       # Entry point for spark-submit
├── SparkSubmitCommandBuilder.java  # Builds spark-submit commands
├── CommandBuilderUtils.java        # Command building utilities
└── LauncherBackend.java           # Backend communication
```

## Integration with spark-submit

The Launcher library is used internally by `spark-submit`:

```
spark-submit script
    ↓
Main.main()
    ↓
SparkSubmitCommandBuilder
    ↓
Launch JVM with SparkSubmit
```

## Configuration Priority

Configuration values are resolved in this order (highest priority first):

1. Values set via `setConf()` or specific setters
2. Properties file specified with `setPropertiesFile()`
3. `conf/spark-defaults.conf` in `SPARK_HOME`
4. Environment variables

## Environment Variables

The launcher respects these environment variables:

- `SPARK_HOME`: Spark installation directory
- `JAVA_HOME`: Java installation directory
- `SPARK_CONF_DIR`: Configuration directory
- `HADOOP_CONF_DIR`: Hadoop configuration directory
- `YARN_CONF_DIR`: YARN configuration directory

## Security Considerations

When launching applications programmatically:

1. **Validate inputs**: Sanitize application arguments
2. **Secure credentials**: Don't hardcode secrets
3. **Limit permissions**: Run with minimal required privileges
4. **Monitor processes**: Track launched applications
5. **Clean up resources**: Always close handles and processes

## Common Use Cases

### Workflow Orchestration

Launch Spark jobs as part of data pipelines:

```java
public class DataPipeline {
  public void runStage(String stageName, String mainClass) throws Exception {
    SparkAppHandle handle = new SparkLauncher()
      .setAppResource("/path/to/pipeline.jar")
      .setMainClass(mainClass)
      .setMaster("yarn")
      .setAppName("Pipeline-" + stageName)
      .startApplication();
    
    // Wait for completion
    while (!handle.getState().isFinal()) {
      Thread.sleep(1000);
    }
    
    if (handle.getState() != SparkAppHandle.State.FINISHED) {
      throw new RuntimeException("Stage " + stageName + " failed");
    }
  }
}
```

### Testing

Launch Spark applications in integration tests:

```java
@Test
public void testSparkApp() throws Exception {
  SparkAppHandle handle = new SparkLauncher()
    .setAppResource("target/test-app.jar")
    .setMainClass("com.example.TestApp")
    .setMaster("local[2]")
    .startApplication();
  
  // Wait for completion
  handle.waitFor(60000); // 60 second timeout
  
  assertEquals(SparkAppHandle.State.FINISHED, handle.getState());
}
```

### Resource Management

Launch applications with dynamic resource allocation:

```java
int executors = calculateRequiredExecutors(dataSize);
String memory = calculateMemory(dataSize);

SparkLauncher launcher = new SparkLauncher()
  .setAppResource("/path/to/app.jar")
  .setMainClass("com.example.App")
  .setMaster("yarn")
  .setConf("spark.executor.instances", String.valueOf(executors))
  .setConf(SparkLauncher.EXECUTOR_MEMORY, memory)
  .setConf("spark.dynamicAllocation.enabled", "true");
```

## Best Practices

1. **Use SparkAppHandle**: Monitor application state
2. **Add listeners**: Track state changes and failures
3. **Set timeouts**: Don't wait indefinitely
4. **Handle errors**: Check exit codes and states
5. **Clean up**: Stop handles and processes
6. **Log everything**: Record launches and outcomes
7. **Use constants**: Use SparkLauncher constants for config keys

## Troubleshooting

### Application Not Starting

**Check:**
- SPARK_HOME is set correctly
- Application JAR path is correct
- Master URL is valid
- Required resources are available

### Process Hangs

**Solutions:**
- Add timeout: `handle.waitFor(timeout)`
- Check for deadlocks in application
- Verify cluster has capacity
- Check logs for issues

### Cannot Monitor Application

**Solutions:**
- Use `startApplication()` instead of `launch()`
- Add listener before starting
- Check for connection issues
- Verify cluster is accessible

## Further Reading

- [Submitting Applications](../docs/submitting-applications.md)
- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Configuration Guide](../docs/configuration.md)

## API Documentation

Full JavaDoc available in the built JAR or online at:
https://spark.apache.org/docs/latest/api/java/org/apache/spark/launcher/package-summary.html

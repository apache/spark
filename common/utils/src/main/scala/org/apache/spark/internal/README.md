# Guidelines for the Structured Logging Framework

## Scala Logging
Use the `org.apache.spark.internal.Logging` trait for logging in Scala code:
* **Logging Messages with Variables**: When logging a message with variables, wrap all the variables with `MDC`s and they will be automatically added to the Mapped Diagnostic Context (MDC). This allows for structured logging and better log analysis.
```scala
logInfo(log"Trying to recover app: ${MDC(LogKeys.APP_ID, app.id)}")
```
* **Constant String Messages**: If you are logging a constant string message, use the log methods that accept a constant string.
```scala
logInfo("StateStore stopped")
```

## Java Logging
Use the `org.apache.spark.internal.SparkLoggerFactory` to get the logger instance in Java code:
* **Getting Logger Instance**: Instead of using `org.slf4j.LoggerFactory`, use `org.apache.spark.internal.SparkLoggerFactory` to ensure structured logging.
```java
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

private static final SparkLogger logger = SparkLoggerFactory.getLogger(JavaUtils.class);
```
* **Logging Messages with Variables**: When logging messages with variables, wrap all the variables with `MDC`s and they will be automatically added to the Mapped Diagnostic Context (MDC).
```java
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

logger.error("Unable to delete file for partition {}", MDC.of(LogKeys.PARTITION_ID$.MODULE$, i));
```

* **Constant String Messages**: For logging constant string messages, use the standard logging methods.
```java
logger.error("Failed to abort the writer after failing to write map output.", e);
```

## LogKey

`LogKey`s serve as identifiers for mapped diagnostic contexts (MDC) within logs. Follow these guidelines when adding a new LogKey:
* Define all structured logging keys in `LogKey.scala`, and sort them alphabetically for ease of search.
* Use `UPPER_SNAKE_CASE` for key names.
* Key names should be both simple and broad, yet include specific identifiers like `STAGE_ID`, `TASK_ID`, and `JOB_ID` when needed for clarity. For instance, use `MAX_ATTEMPTS` as a general key instead of creating separate keys for each scenario such as `EXECUTOR_STATE_SYNC_MAX_ATTEMPTS` and `MAX_TASK_FAILURES`. This balances simplicity with the detail needed for effective logging.
* Use abbreviations in names if they are widely understood, such as `APP_ID` for APPLICATION_ID, and `K8S` for KUBERNETES.
* For time-related keys, use milliseconds as the unit of time.

## Exceptions

To ensure logs are compatible with Spark SQL and log analysis tools, avoid `Exception.printStackTrace()`. Use `logError`, `logWarning`, and `logInfo` methods from the `Logging` trait to log exceptions, maintaining structured and parsable logs.

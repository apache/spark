# Guidelines for the Structured Logging Framework

## LogKey

LogKeys serve as identifiers for mapped diagnostic contexts (MDC) within logs. Follow these guidelines when adding new LogKeys:
* Define all structured logging keys in `LogKey.scala`, and sort them alphabetically for ease of search.
* Use `UPPER_SNAKE_CASE` for key names.
* Key names should be both simple and broad, yet include specific identifiers like `STAGE_ID`, `TASK_ID`, and `JOB_ID` when needed for clarity. For instance, use `MAX_ATTEMPTS` as a general key instead of creating separate keys for each scenario such as `EXECUTOR_STATE_SYNC_MAX_ATTEMPTS` and `MAX_TASK_FAILURES`. This balances simplicity with the detail needed for effective logging.
* Use abbreviations in names if they are widely understood, such as `APP_ID` for APPLICATION_ID, and `K8S` for KUBERNETES.
* For time-related keys, use milliseconds as the unit of time.

## Exceptions

To ensure logs are compatible with Spark SQL and log analysis tools, avoid `Exception.printStackTrace()`. Use `logError`, `logWarning`, and `logInfo` methods from the `Logging` trait to log exceptions, maintaining structured and parsable logs.

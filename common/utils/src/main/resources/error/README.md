# Guidelines for Throwing User-Facing Errors

To throw a user-facing error or exception, developers should specify a standardized SQLSTATE, an error condition, and message parameters rather than an arbitrary error message.

This guide will describe how to do this.

## Error Hierarchy and Terminology

The error hierarchy is as follows:
1. Error state / SQLSTATE
2. Error condition
3. Error sub-condition

The error state / SQLSTATE itself is comprised of two parts:
1. Error class
2. Error sub-class

Acceptable values for these various error parts are defined in the following files:
* [`error-classes.json`](error-classes.json)
* [`error-states.json`](error-states.json)
* [`error-conditions.json`](error-conditions.json)

The terms error class, state, and condition come from the SQL standard.

### Illustrative Example
* Error state / SQLSTATE: `42K01` (Class: `42`; Sub-class: `K01`)
  * Error condition: `DATATYPE_MISSING_SIZE`
  * Error condition: `INCOMPLETE_TYPE_DEFINITION`
    * Error sub-condition: `ARRAY`
    * Error sub-condition: `MAP`
    * Error sub-condition: `STRUCT`
* Error state / SQLSTATE: `42604` (Class: `42`; Sub-class: `604`)
  * Error condition: `INVALID_ESCAPE_CHAR`
  * Error condition: `AS_OF_JOIN`
    * Error sub-condition: `TOLERANCE_IS_NON_NEGATIVE`
    * Error sub-condition: `TOLERANCE_IS_UNFOLDABLE`
    * Error sub-condition: `UNSUPPORTED_DIRECTION`

### Inconsistent Use of the Term "Error Class"

Unfortunately, we have historically used the term "error class" inconsistently to refer both to a proper error class like `42` and also to an error condition like `DATATYPE_MISSING_SIZE`.

Fixing this will require renaming `SparkException.errorClass` to `SparkException.errorCondition` and making similar changes to `ErrorClassesJsonReader` and other parts of the codebase. We will address this in [SPARK-47429]. Until that is complete, we will have to live with the fact that a string like `DATATYPE_MISSING_SIZE` is called an "error condition" in our user-facing documentation but an "error class" in the code.

For more details, please see [SPARK-46810].

[SPARK-46810]: https://issues.apache.org/jira/browse/SPARK-46810
[SPARK-47429]: https://issues.apache.org/jira/browse/SPARK-47429

## Usage

1. Check if the error is an internal error.
   Internal errors are bugs in the code that we do not expect users to encounter; this does not include unsupported operations.
   If true, use the error condition `INTERNAL_ERROR` and skip to step 4.
2. Check if an appropriate error condition already exists in [`error-conditions.json`](error-conditions.json).
   If true, use the error condition and skip to step 4.
3. Add a new condition to [`error-conditions.json`](error-conditions.json). If the new condition requires a new error state, add the new error state to [`error-states.json`](error-states.json).
4. Check if the exception type already extends `SparkThrowable`.
   If true, skip to step 6.
5. Mix `SparkThrowable` into the exception.
6. Throw the exception with the error condition and message parameters. If the same exception is thrown in several places, create an util function in a central place such as `QueryCompilationErrors.scala` to instantiate the exception.

### Before

Throw with arbitrary error message:

```scala
throw new TestException("Problem A because B")
```

### After

`error-conditions.json`

```json
"PROBLEM_BECAUSE" : {
  "message" : ["Problem <problem> because <cause>"],
  "sqlState" : "XXXXX"
}
```

`SparkException.scala`

```scala
class SparkTestException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends TestException(SparkThrowableHelper.getMessage(errorClass, messageParameters))
    with SparkThrowable {
    
  override def getMessageParameters: java.util.Map[String, String] =
    messageParameters.asJava

  override def getErrorClass: String = errorClass
}
```

Throw with error condition and message parameters:

```scala
throw new SparkTestException("PROBLEM_BECAUSE", Map("problem" -> "A", "cause" -> "B"))
```

### Access fields

To access error fields, catch exceptions that extend `org.apache.spark.SparkThrowable` and access
  - Error condition with `getErrorClass`
  - SQLSTATE with `getSqlState`

```scala
try {
    ...
} catch {
    case e: SparkThrowable if Option(e.getSqlState).forall(_.startsWith("42")) =>
        warn("Syntax error")
}
```

## Fields

### Error condition

Error conditions are a succinct, human-readable representation of the error category.

An uncategorized errors can be assigned to a legacy error condition with the prefix `_LEGACY_ERROR_TEMP_` and an unused sequential number, for instance `_LEGACY_ERROR_TEMP_0053`.

You should not introduce new uncategorized errors. Instead, convert them to proper errors whenever encountering them in new code.

**Note:** Though the proper term for this field is an "error condition", it is called `errorClass` in the codebase due to an unfortunate accident of history. For more details, please refer to [SPARK-46810].

#### Invariants

- Unique
- Consistent across releases
- Sorted alphabetically

### Message

Error messages provide a descriptive, human-readable representation of the error.
The message format accepts string parameters via the HTML tag syntax: e.g. `<relationName>`.

The values passed to the message should not themselves be messages.
They should be: runtime-values, keywords, identifiers, or other values that are not translated.

The quality of the error message should match the
[guidelines](https://spark.apache.org/error-message-guidelines.html).

#### Invariants

- Unique

### SQLSTATE

SQLSTATE is a mandatory portable error identifier across SQL engines.
SQLSTATE comprises a 2-character class followed by a 3-character sub-class.
Spark prefers to re-use existing SQLSTATEs, preferably used by multiple vendors.
For extension Spark claims the `K**` sub-class range.
If a new class is needed it will also claim the `K0` class.

Internal errors should use the `XX` class. You can subdivide internal errors by component.
For example: The existing `XXKD0` is used for an internal analyzer error.

#### Invariants

- Consistent across releases unless the error is internal.

#### ANSI/ISO standard

The SQLSTATEs in [`error-states.json`](error-states.json) are collated from:
- SQL2016
- DB2 zOS/LUW
- PostgreSQL 15
- Oracle 12 (last published)
- SQL Server
- Redshift

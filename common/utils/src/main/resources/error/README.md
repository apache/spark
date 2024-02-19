# Guidelines

To throw a standardized user-facing error or exception, developers should specify the error class, a SQLSTATE,
and message parameters rather than an arbitrary error message.

## Usage

1. Check if the error is an internal error.
   Internal errors are bugs in the code that we do not expect users to encounter; this does not include unsupported operations.
   If true, use the error class `INTERNAL_ERROR` and skip to step 4.
2. Check if an appropriate error class already exists in `error-classes.json`.
   If true, use the error class and skip to step 4.
3. Add a new class with a new or existing SQLSTATE to `error-classes.json`; keep in mind the invariants below.
4. Check if the exception type already extends `SparkThrowable`.
   If true, skip to step 6.
5. Mix `SparkThrowable` into the exception.
6. Throw the exception with the error class and message parameters. If the same exception is thrown in several places, create an util function in a central place such as `QueryCompilationErrors.scala` to instantiate the exception.

### Before

Throw with arbitrary error message:

    throw new TestException("Problem A because B")

### After

`error-classes.json`

    "PROBLEM_BECAUSE" : {
      "message" : ["Problem <problem> because <cause>"],
      "sqlState" : "XXXXX"
    }

`SparkException.scala`

    class SparkTestException(
        errorClass: String,
        messageParameters: Map[String, String])
      extends TestException(SparkThrowableHelper.getMessage(errorClass, messageParameters))
        with SparkThrowable {
        
      override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

      override def getErrorClass: String = errorClass
    }

Throw with error class and message parameters:

    throw new SparkTestException("PROBLEM_BECAUSE", Map("problem" -> "A", "cause" -> "B"))

## Access fields

To access error fields, catch exceptions that extend `org.apache.spark.SparkThrowable` and access
  - Error class with `getErrorClass`
  - SQLSTATE with `getSqlState`


    try {
        ...
    } catch {
        case e: SparkThrowable if Option(e.getSqlState).forall(_.startsWith("42")) =>
            warn("Syntax error")
    }

## Fields

### Error class

Error classes are a succinct, human-readable representation of the error category.

An uncategorized errors can be assigned to a legacy error class with the prefix `_LEGACY_ERROR_TEMP_` and an unused sequential number, for instance `_LEGACY_ERROR_TEMP_0053`.

You should not introduce new uncategorized errors. Instead, convert them to proper errors whenever encountering them in new code.

#### Invariants

- Unique
- Consistent across releases
- Sorted alphabetically

### Message

Error messages provide a descriptive, human-readable representation of the error.
The message format accepts string parameters via the HTML tag syntax: e.g. <relationName>.

The values passed to the message shoudl not themselves be messages.
They should be: runtime-values, keywords, identifiers, or other values that are not translated.

The quality of the error message should match the
[guidelines](https://spark.apache.org/error-message-guidelines.html).

#### Invariants

- Unique

### SQLSTATE

SQLSTATE is an mandatory portable error identifier across SQL engines.
SQLSTATE comprises a 2-character class value followed by a 3-character subclass value.
Spark prefers to re-use existing SQLSTATEs, preferably used by multiple vendors.
For extension Spark claims the 'K**' subclass range.
If a new class is needed it will also claim the 'K0' class.

Internal errors should use the 'XX' class. You can subdivide internal errors by component. 
For example: The existing 'XXKD0' is used for an internal analyzer error.

#### Invariants

- Consistent across releases unless the error is internal.

#### ANSI/ISO standard

The following SQLSTATEs are collated from:
- SQL2016
- DB2 zOS/LUW
- PostgreSQL 15
- Oracle 12 (last published)
- SQL Server
- Redshift

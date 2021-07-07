# Guidelines

To throw a standardized user-facing error or exception, developers should specify the error class
and message parameters rather than an arbitrary error message.

## Usage

1. Check if an appropriate error class already exists in `error-class.json`.
   If true, skip to step 3. Otherwise, continue to step 2.
2. Add a new class to `error-class.json`; keep in mind the invariants below.
3. Check if the exception type already extends `SparkThrowable`.
   If true, skip to step 5. Otherwise, continue to step 4.
4. Mix `SparkThrowable` into the exception.
5. Throw the exception with the error class and message parameters.

### Before

Throw with arbitrary error message:

    throw new TestException("Problem A because B")

### After

`error-class.json`

    "PROBLEM_BECAUSE": {
      "message": ["Problem %s because %s"],
      "sqlState": "XXXXX"
    }

`SparkException.scala`

    class SparkTestException(
        errorClass: String,
        messageParameters: Seq[String])
      extends TestException(SparkThrowableHelper.getMessage(errorClass, messageParameters))
        with SparkThrowable {
        
      def getErrorClass: String = errorClass
      def getMessageParameters: Array[String] = messageParameters
      def getSqlState: String = SparkThrowableHelper.getSqlState(errorClass)
    }

Throw with error class and message parameters:

    throw new SparkTestException("PROBLEM_BECAUSE", Seq("A", "B"))

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

Invariants:

- Unique
- Consistent across releases
- Sorted alphabetically

### Message

Error messages provide a descriptive, human-readable representation of the error.
The message format accepts string parameters via the C-style printf syntax.

The quality of the error message should match the
[guidelines](https://spark.apache.org/error-message-guidelines.html).

Invariants:

- Unique

### SQLSTATE

SQLSTATE is an optional portable error identifier across SQL engines.
For consistency, Spark only sets SQLSTATE as defined in the ANSI/ISO standard.
Spark does not define its own classes or subclasses.

Invariants:

- Consistent across releases

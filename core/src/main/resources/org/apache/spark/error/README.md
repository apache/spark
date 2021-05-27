# Guidelines

To throw a standardized exception, developers should use an error class and message parameters
rather than an arbitrary error message.

## Usage

To throw an exception, do the following.

1. Check if an appropriate error class already exists in `error-classes.json`.
   If true, skip to step 3. Otherwise, continue to step 2.
2. Add a new class to `error-classes.json`; keep in mind the invariants below.
3. Check if the exception type already extends `SparkError`.
   If true, skip to step 5. Otherwise, continue to step 4.
4. Mix `SparkError` into the exception.
5. Throw the exception with the error class and message parameters.

### Before

Throw exception:

    throw new TestException("Problem A because B")


### After

`error-classes.json`

    "PROBLEM_BECAUSE": {
      "sqlState": "XXXXX",
      "messageFormatLines": ["Problem {0} because {1}"]
    }

`SparkException.scala`

    class SparkTestException(
        val errorClass: String,
        val messageParameters: Seq[String])
      extends TestException(SparkError.getMessage(errorClass, messageParameters))
        with SparkError

Throw exception:

    throw new SparkTestException("PROBLEM_BECAUSE", Seq("A", "B"))

## Access fields

To add error fields to error messages, set the Spark configuration:

    spark.showSparkErrorFields = true

To access error fields, catch exceptions that extend `org.apache.spark.SparkError` and access
  - Error class with `errorClass`
  - SQLSTATE with `sqlState`


    try {
        ...
    } catch {
        case e: SparkError =>
            warn(s"Saw error with SQLSTATE ${e.sqlState}")
    }

## Fields

All fields, excluding error messages, should be consistent across releases.

### Error class

Error classes are a succinct representation of the error category.

### SQLSTATE

SQLSTATE is a portable error identifier across SQL engines.
For consistency, Spark only sets SQLSTATE as defined in the ANSI/ISO standard.
Spark does not define its own classes or subclasses.

### Message format

Error messages should be unique to avoid duplication.
The error message format should be written to accept parameters via the C-style printf syntax.
The parameters are Strings for simplicity.

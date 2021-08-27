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

#### Invariants

- Unique
- Consistent across releases
- Sorted alphabetically

### Message

Error messages provide a descriptive, human-readable representation of the error.
The message format accepts string parameters via the C-style printf syntax.

The quality of the error message should match the
[guidelines](https://spark.apache.org/error-message-guidelines.html).

#### Invariants

- Unique

### SQLSTATE

SQLSTATE is an optional portable error identifier across SQL engines.
For consistency, Spark only sets SQLSTATE as defined in the ANSI/ISO standard.
SQLSTATE comprises a 2-character class value followed by a 3-character subclass value.
Spark only uses the standard-defined classes and subclasses, and does not use implementation-defined classes or subclasses.

#### Invariants

- Consistent across releases

#### ANSI/ISO standard

The following SQLSTATEs are from ISO/IEC CD 9075-2.

|SQLSTATE|Class|Condition                                                   |Subclass|Subcondition                                                   |
|--------|-----|------------------------------------------------------------|--------|---------------------------------------------------------------|
|07000   |07   |dynamic SQL error                                           |000     |(no subclass)                                                  |
|07001   |07   |dynamic SQL error                                           |001     |using clause does not match dynamic parameter specifications   |
|07002   |07   |dynamic SQL error                                           |002     |using clause does not match target specifications              |
|07003   |07   |dynamic SQL error                                           |003     |cursor specification cannot be executed                        |
|07004   |07   |dynamic SQL error                                           |004     |using clause required for dynamic parameters                   |
|07005   |07   |dynamic SQL error                                           |005     |prepared statement not a cursor specification                  |
|07006   |07   |dynamic SQL error                                           |006     |restricted data type attribute violation                       |
|07007   |07   |dynamic SQL error                                           |007     |using clause required for result fields                        |
|07008   |07   |dynamic SQL error                                           |008     |invalid descriptor count                                       |
|07009   |07   |dynamic SQL error                                           |009     |invalid descriptor index                                       |
|0700B   |07   |dynamic SQL error                                           |00B     |data type transform function violation                         |
|0700C   |07   |dynamic SQL error                                           |00C     |undefined DATA value                                           |
|0700D   |07   |dynamic SQL error                                           |00D     |invalid DATA target                                            |
|0700E   |07   |dynamic SQL error                                           |00E     |invalid LEVEL value                                            |
|0700F   |07   |dynamic SQL error                                           |00F     |invalid DATETIME_INTERVAL_CODE                               |
|08000   |08   |connection exception                                        |000     |(no subclass)                                                  |
|08001   |08   |connection exception                                        |001     |SQL-client unable to establish SQL-connection                  |
|08002   |08   |connection exception                                        |002     |connection name in use                                         |
|08003   |08   |connection exception                                        |003     |connection does not exist                                      |
|08004   |08   |connection exception                                        |004     |SQL-server rejected establishment of SQL-connection            |
|08006   |08   |connection exception                                        |006     |connection failure                                             |
|08007   |08   |connection exception                                        |007     |transaction resolution unknown                                 |
|09000   |09   |triggered action exception                                  |000     |(no subclass)                                                  |
|0A000   |0A   |feature not supported                                       |000     |(no subclass)                                                  |
|0A001   |0A   |feature not supported                                       |001     |multiple server transactions                                   |
|0D000   |0D   |invalid target type specification                           |000     |(no subclass)                                                  |
|0E000   |0E   |invalid schema name list specification                      |000     |(no subclass)                                                  |
|0F000   |0F   |locator exception                                           |000     |(no subclass)                                                  |
|0F001   |0F   |locator exception                                           |001     |invalid specification                                          |
|0L000   |0L   |invalid grantor                                             |000     |(no subclass)                                                  |
|0M000   |0M   |invalid SQL-invoked procedure reference                     |000     |(no subclass)                                                  |
|0P000   |0P   |invalid role specification                                  |000     |(no subclass)                                                  |
|0S000   |0S   |invalid transform group name specification                  |000     |(no subclass)                                                  |
|0T000   |0T   |target table disagrees with cursor specification            |000     |(no subclass)                                                  |
|0U000   |0U   |attempt to assign to non-updatable column                   |000     |(no subclass)                                                  |
|0V000   |0V   |attempt to assign to ordering column                        |000     |(no subclass)                                                  |
|0W000   |0W   |prohibited statement encountered during trigger execution   |000     |(no subclass)                                                  |
|0W001   |0W   |prohibited statement encountered during trigger execution   |001     |modify table modified by data change delta table               |
|0Z000   |0Z   |diagnostics exception                                       |000     |(no subclass)                                                  |
|0Z001   |0Z   |diagnostics exception                                       |001     |maximum number of stacked diagnostics areas exceeded           |
|21000   |21   |cardinality violation                                       |000     |(no subclass)                                                  |
|22000   |22   |data exception                                              |000     |(no subclass)                                                  |
|22001   |22   |data exception                                              |001     |string data, right truncation                                  |
|22002   |22   |data exception                                              |002     |null value, no indicator parameter                             |
|22003   |22   |data exception                                              |003     |numeric value out of range                                     |
|22004   |22   |data exception                                              |004     |null value not allowed                                         |
|22005   |22   |data exception                                              |005     |error in assignment                                            |
|22006   |22   |data exception                                              |006     |invalid interval format                                        |
|22007   |22   |data exception                                              |007     |invalid datetime format                                        |
|22008   |22   |data exception                                              |008     |datetime field overflow                                        |
|22009   |22   |data exception                                              |009     |invalid time zone displacement value                           |
|2200B   |22   |data exception                                              |00B     |escape character conflict                                      |
|2200C   |22   |data exception                                              |00C     |invalid use of escape character                                |
|2200D   |22   |data exception                                              |00D     |invalid escape octet                                           |
|2200E   |22   |data exception                                              |00E     |null value in array target                                     |
|2200F   |22   |data exception                                              |00F     |zero-length character string                                   |
|2200G   |22   |data exception                                              |00G     |most specific type mismatch                                    |
|2200H   |22   |data exception                                              |00H     |sequence generator limit exceeded                              |
|2200P   |22   |data exception                                              |00P     |interval value out of range                                    |
|2200Q   |22   |data exception                                              |00Q     |multiset value overflow                                        |
|22010   |22   |data exception                                              |010     |invalid indicator parameter value                              |
|22011   |22   |data exception                                              |011     |substring error                                                |
|22012   |22   |data exception                                              |012     |division by zero                                               |
|22013   |22   |data exception                                              |013     |invalid preceding or following size in window function         |
|22014   |22   |data exception                                              |014     |invalid argument for NTILE function                            |
|22015   |22   |data exception                                              |015     |interval field overflow                                        |
|22016   |22   |data exception                                              |016     |invalid argument for NTH_VALUE function                        |
|22018   |22   |data exception                                              |018     |invalid character value for cast                               |
|22019   |22   |data exception                                              |019     |invalid escape character                                       |
|2201B   |22   |data exception                                              |01B     |invalid regular expression                                     |
|2201C   |22   |data exception                                              |01C     |null row not permitted in table                                |
|2201E   |22   |data exception                                              |01E     |invalid argument for natural logarithm                         |
|2201F   |22   |data exception                                              |01F     |invalid argument for power function                            |
|2201G   |22   |data exception                                              |01G     |invalid argument for width bucket function                     |
|2201H   |22   |data exception                                              |01H     |invalid row version                                            |
|2201S   |22   |data exception                                              |01S     |invalid XQuery regular expression                              |
|2201T   |22   |data exception                                              |01T     |invalid XQuery option flag                                     |
|2201U   |22   |data exception                                              |01U     |attempt to replace a zero-length string                        |
|2201V   |22   |data exception                                              |01V     |invalid XQuery replacement string                              |
|2201W   |22   |data exception                                              |01W     |invalid row count in fetch first clause                        |
|2201X   |22   |data exception                                              |01X     |invalid row count in result offset clause                      |
|22020   |22   |data exception                                              |020     |invalid period value                                           |
|22021   |22   |data exception                                              |021     |character not in repertoire                                    |
|22022   |22   |data exception                                              |022     |indicator overflow                                             |
|22023   |22   |data exception                                              |023     |invalid parameter value                                        |
|22024   |22   |data exception                                              |024     |unterminated C string                                          |
|22025   |22   |data exception                                              |025     |invalid escape sequence                                        |
|22026   |22   |data exception                                              |026     |string data, length mismatch                                   |
|22027   |22   |data exception                                              |027     |trim error                                                     |
|22029   |22   |data exception                                              |029     |noncharacter in UCS string                                     |
|2202D   |22   |data exception                                              |02D     |null value substituted for mutator subject parameter           |
|2202E   |22   |data exception                                              |02E     |array element error                                            |
|2202F   |22   |data exception                                              |02F     |array data, right truncation                                   |
|2202G   |22   |data exception                                              |02G     |invalid repeat argument in a sample clause                     |
|2202H   |22   |data exception                                              |02H     |invalid sample size                                            |
|2202J   |22   |data exception                                              |02J     |invalid argument for row pattern navigation operation          |
|2202K   |22   |data exception                                              |02K     |skip to non-existent row                                       |
|2202L   |22   |data exception                                              |02L     |skip to first row of match                                     |
|23000   |23   |integrity constraint violation                              |000     |(no subclass)                                                  |
|23001   |23   |integrity constraint violation                              |001     |restrict violation                                             |
|24000   |24   |invalid cursor state                                        |000     |(no subclass)                                                  |
|25000   |25   |invalid transaction state                                   |000     |(no subclass)                                                  |
|25001   |25   |invalid transaction state                                   |001     |active SQL-transaction                                         |
|25002   |25   |invalid transaction state                                   |002     |branch transaction already active                              |
|25003   |25   |invalid transaction state                                   |003     |inappropriate access mode for branch transaction               |
|25004   |25   |invalid transaction state                                   |004     |inappropriate isolation level for branch transaction           |
|25005   |25   |invalid transaction state                                   |005     |no active SQL-transaction for branch transaction               |
|25006   |25   |invalid transaction state                                   |006     |read-only SQL-transaction                                      |
|25007   |25   |invalid transaction state                                   |007     |schema and data statement mixing not supported                 |
|25008   |25   |invalid transaction state                                   |008     |held cursor requires same isolation level                      |
|26000   |26   |invalid SQL statement name                                  |000     |(no subclass)                                                  |
|27000   |27   |triggered data change violation                             |000     |(no subclass)                                                  |
|27001   |27   |triggered data change violation                             |001     |modify table modified by data change delta table               |
|28000   |28   |invalid authorization specification                         |000     |(no subclass)                                                  |
|2B000   |2B   |dependent privilege descriptors still exist                 |000     |(no subclass)                                                  |
|2C000   |2C   |invalid character set name                                  |000     |(no subclass)                                                  |
|2C001   |2C   |invalid character set name                                  |001     |cannot drop SQL-session default character set                  |
|2D000   |2D   |invalid transaction termination                             |000     |(no subclass)                                                  |
|2E000   |2E   |invalid connection name                                     |000     |(no subclass)                                                  |
|2F000   |2F   |SQL routine exception                                       |000     |(no subclass)                                                  |
|2F002   |2F   |SQL routine exception                                       |002     |modifying SQL-data not permitted                               |
|2F003   |2F   |SQL routine exception                                       |003     |prohibited SQL-statement attempted                             |
|2F004   |2F   |SQL routine exception                                       |004     |reading SQL-data not permitted                                 |
|2F005   |2F   |SQL routine exception                                       |005     |function executed no return statement                          |
|2H000   |2H   |invalid collation name                                      |000     |(no subclass)                                                  |
|30000   |30   |invalid SQL statement identifier                            |000     |(no subclass)                                                  |
|33000   |33   |invalid SQL descriptor name                                 |000     |(no subclass)                                                  |
|34000   |34   |invalid cursor name                                         |000     |(no subclass)                                                  |
|35000   |35   |invalid condition number                                    |000     |(no subclass)                                                  |
|36000   |36   |cursor sensitivity exception                                |000     |(no subclass)                                                  |
|36001   |36   |cursor sensitivity exception                                |001     |request rejected                                               |
|36002   |36   |cursor sensitivity exception                                |002     |request failed                                                 |
|38000   |38   |external routine exception                                  |000     |(no subclass)                                                  |
|38001   |38   |external routine exception                                  |001     |containing SQL not permitted                                   |
|38002   |38   |external routine exception                                  |002     |modifying SQL-data not permitted                               |
|38003   |38   |external routine exception                                  |003     |prohibited SQL-statement attempted                             |
|38004   |38   |external routine exception                                  |004     |reading SQL-data not permitted                                 |
|39000   |39   |external routine invocation exception                       |000     |(no subclass)                                                  |
|39004   |39   |external routine invocation exception                       |004     |null value not allowed                                         |
|3B000   |3B   |savepoint exception                                         |000     |(no subclass)                                                  |
|3B001   |3B   |savepoint exception                                         |001     |invalid specification                                          |
|3B002   |3B   |savepoint exception                                         |002     |too many                                                       |
|3C000   |3C   |ambiguous cursor name                                       |000     |(no subclass)                                                  |
|3D000   |3D   |invalid catalog name                                        |000     |(no subclass)                                                  |
|3F000   |3F   |invalid schema name                                         |000     |(no subclass)                                                  |
|40000   |40   |transaction rollback                                        |000     |(no subclass)                                                  |
|40001   |40   |transaction rollback                                        |001     |serialization failure                                          |
|40002   |40   |transaction rollback                                        |002     |integrity constraint violation                                 |
|40003   |40   |transaction rollback                                        |003     |statement completion unknown                                   |
|40004   |40   |transaction rollback                                        |004     |triggered action exception                                     |
|42000   |42   |syntax error or access rule violation                       |000     |(no subclass)                                                  |
|44000   |44   |with check option violation                                 |000     |(no subclass)                                                  |
|HZ000   |HZ   |remote database access                                      |000     |(no subclass)                                                  |

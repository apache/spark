# Identifier-Lite Feature Design

## Overview

The **identifier-lite** feature is a simplified version of the existing `IDENTIFIER` clause in Spark SQL. It allows `IDENTIFIER('string_literal')` to be used anywhere identifiers can appear in SQL statements, with the string literal being folded immediately during the parse phase.

## Motivation

The existing `IDENTIFIER` clause in Spark is limited to a narrow set of use cases:
- It only works in specific grammar positions (table references, column references, function names)
- It requires analysis-time resolution via `PlanWithUnresolvedIdentifier` and `ExpressionWithUnresolvedIdentifier`
- It supports full expressions (including parameter markers and concatenation)

The identifier-lite feature generalizes identifier templating to **all places where identifiers can be used**, while simplifying the implementation by:
- Only accepting string literals (not arbitrary expressions)
- Folding the string literal into an identifier at parse time (not analysis time)
- Working seamlessly with all existing grammar rules that use identifiers

## Design

### Grammar Changes

#### SqlBaseParser.g4

Added a new alternative to the `strictIdentifier` grammar rule:

```antlr
strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | IDENTIFIER_KW LEFT_PAREN stringLit RIGHT_PAREN  #identifierLiteral
    | {SQL_standard_keyword_behavior}? ansiNonReserved #unquotedIdentifier
    | {!SQL_standard_keyword_behavior}? nonReserved    #unquotedIdentifier
    ;
```

This allows `IDENTIFIER('string')` to appear anywhere a regular identifier can appear, including:
- Table names
- Column names
- Schema/database names
- Function names
- Constraint names
- And any other identifier context

### Qualified Identifier Support

The identifier-lite feature supports **qualified identifiers** within the string literal. When you write:
- `IDENTIFIER('`catalog`.`schema`')` - this is parsed into multiple parts: `['catalog', 'schema']`
- `IDENTIFIER('schema.table')` - parsed into: `['schema', 'table']`
- `IDENTIFIER('schema').table` - the schema part is parsed, then combined with the literal `table`

This allows flexible composition of identifiers:
```sql
-- These are all equivalent for table 'catalog.schema.table':
IDENTIFIER('catalog.schema.table')
IDENTIFIER('catalog.schema').table
IDENTIFIER('catalog').schema.table
catalog.IDENTIFIER('schema.table')
catalog.IDENTIFIER('schema').table
```

### Parser Implementation Changes

#### DataTypeAstBuilder.scala

Added helper methods to handle identifier-lite with qualified identifier support:

```scala
protected def getIdentifierParts(ctx: ParserRuleContext): Seq[String] = {
  ctx match {
    case idLitCtx: IdentifierLiteralContext =>
      // For IDENTIFIER('literal'), extract the string literal value and parse it
      val literalValue = string(visitStringLit(idLitCtx.stringLit()))
      // Parse the string as a multi-part identifier (e.g., "`cat`.`schema`" -> Seq("cat", "schema"))
      CatalystSqlParser.parseMultipartIdentifier(literalValue)
    case _ =>
      // For regular identifiers, just return the text as a single part
      Seq(ctx.getText)
  }
}

protected def getIdentifierText(ctx: ParserRuleContext): String = {
  getIdentifierParts(ctx).mkString(".")
}
```

Updated `visitMultipartIdentifier()` to flatten parts when an identifier-lite contains multiple parts:

```scala
override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
  ctx.parts.asScala.flatMap { part =>
    val identifierCtx = part.identifier()
    if (identifierCtx != null && identifierCtx.strictIdentifier() != null) {
      // Returns Seq with 1+ elements (multiple if qualified)
      getIdentifierParts(identifierCtx.strictIdentifier())
    } else {
      Seq(part.getText)
    }
  }.toSeq
```

#### AstBuilder.scala

Updated all methods that extract identifier text to use `getIdentifierParts()`:
- `visitIdentifierSeq()` - uses `getIdentifierText()` to keep list items as single strings
- `visitTableIdentifier()` - combines db and table parts from qualified identifiers
- `visitFunctionIdentifier()` - combines db and function parts from qualified identifiers
- `visitColDefinition()` - extracts column names
- Column name extraction in various contexts

Special handling for `TableIdentifier` and `FunctionIdentifier` to properly combine parts:
```scala
override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = {
  val tableParts = getIdentifierParts(ctx.table.strictIdentifier())
  val dbParts = Option(ctx.db).map(db => getIdentifierParts(db.strictIdentifier()))
  val allParts = dbParts.getOrElse(Seq.empty) ++ tableParts

  allParts match {
    case Seq(table) => TableIdentifier(table, None)
    case parts if parts.size >= 2 =>
      TableIdentifier(parts.last, Some(parts.dropRight(1).mkString(".")))
  }
}
```

## Key Differences from Full IDENTIFIER Clause

| Feature | Full IDENTIFIER Clause | Identifier-Lite |
|---------|----------------------|-----------------|
| **Syntax** | `IDENTIFIER(expression)` | `IDENTIFIER('literal')` |
| **Supported Arguments** | Any constant string expression (including parameter markers, variables, concatenation) | Only string literals |
| **Resolution Time** | Analysis phase (via `PlanWithUnresolvedIdentifier`) | Parse phase (immediately folded) |
| **Grammar Positions** | Limited to specific rules (`identifierReference`, `functionName`) | All positions where identifiers are used |
| **Use Case** | Dynamic identifier resolution with runtime values | Static identifier specification with unusual names |

## Usage Examples

### Table Names

```sql
-- Create table with identifier-lite
CREATE TABLE IDENTIFIER('my_table') (c1 INT);

-- Query table
SELECT * FROM IDENTIFIER('my_table');

-- Qualified table name (fully specified)
SELECT * FROM IDENTIFIER('schema.table');

-- Qualified table name (partial specification)
SELECT * FROM IDENTIFIER('schema').table;

-- Qualified with backticks
SELECT * FROM IDENTIFIER('`my schema`.`my table`');
```

### Column Names

```sql
-- Select specific columns
SELECT IDENTIFIER('col1'), IDENTIFIER('col2') FROM t;

-- Column with special characters
SELECT IDENTIFIER('`column with spaces`') FROM t;

-- Mixed usage
CREATE TABLE t(IDENTIFIER('col1') INT, IDENTIFIER('col2') STRING);

-- Qualified column references
SELECT IDENTIFIER('t.col1') FROM t;
```

### Function Names

```sql
-- Use identifier-lite for function names
SELECT IDENTIFIER('abs')(-5);
SELECT IDENTIFIER('upper')('hello');

-- Qualified function names
SELECT IDENTIFIER('schema.my_udf')(value) FROM t;
```

### DDL Operations

```sql
-- ALTER TABLE operations
ALTER TABLE IDENTIFIER('table_name') ADD COLUMN IDENTIFIER('new_col') INT;
ALTER TABLE IDENTIFIER('table_name') RENAME COLUMN IDENTIFIER('old') TO IDENTIFIER('new');

-- DROP operations with qualified names
DROP TABLE IDENTIFIER('schema.table_name');

-- Mixed qualification
DROP TABLE IDENTIFIER('schema').table_name;
```

### Complex Qualified Identifier Examples

```sql
-- Three-part identifier (catalog.schema.table)
SELECT * FROM IDENTIFIER('catalog.schema.table');

-- Equivalent forms:
SELECT * FROM IDENTIFIER('catalog.schema').table;
SELECT * FROM IDENTIFIER('catalog').schema.table;
SELECT * FROM catalog.IDENTIFIER('schema.table');
SELECT * FROM catalog.IDENTIFIER('schema').table;

-- With backticked parts
SELECT * FROM IDENTIFIER('`my catalog`.`my schema`.`my table`');

-- Mixed backticks and regular identifiers
SELECT * FROM IDENTIFIER('`my catalog`.`my schema`').regular_table;
```

## Implementation Files Modified

1. **Grammar Files**:
   - `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4`
   - `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseLexer.g4` (no changes - reusing existing `IDENTIFIER_KW`)

2. **Parser Implementation**:
   - `sql/api/src/main/scala/org/apache/spark/sql/catalyst/parser/DataTypeAstBuilder.scala`
   - `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala`

3. **Test Files**:
   - `sql/core/src/test/resources/sql-tests/inputs/identifier-clause.sql` (merged identifier-lite tests)
   - `sql/core/src/test/scala/org/apache/spark/sql/ParametersSuite.scala` (added `IdentifierLiteSuite`)

## Limitations

1. **No Expression Support**: Identifier-lite only accepts string literals. Expressions like `IDENTIFIER('tab' || '_name')` or parameter markers like `IDENTIFIER(:param)` are not supported. For these use cases, the full IDENTIFIER clause should be used instead.

2. **No Runtime Binding**: Since the identifier is folded at parse time, it cannot be changed dynamically. For dynamic identifier binding, use the full IDENTIFIER clause with parameter markers or variables.

3. **String Literal Only**: The argument must be a string literal (`'value'` or `"value"`). Variables, parameter markers, and expressions are not supported.

## Testing

Test coverage includes:
1. Basic usage with table names, column names, and function names
2. Qualified identifiers (e.g., `schema.table`, `catalog.schema.table`)
3. Identifiers with special characters (backticked identifiers)
4. DDL operations (CREATE, ALTER, DROP)
5. Mixed usage with regular identifiers
6. Column definitions using identifier-lite
7. ALTER TABLE operations with identifier-lite (RENAME COLUMN, ADD COLUMN, DROP COLUMN, RENAME TABLE)
8. **Qualified table references with identifier-lite:**
   - `IDENTIFIER('schema.table')` - fully qualified in one literal
   - `IDENTIFIER('schema').table` - partial qualification
   - `IDENTIFIER('`schema`.`table`')` - with backticks
   - Mixed forms with both identifier-lite and regular identifiers

All identifier-lite tests have been integrated into the existing `identifier-clause.sql` test suite under a dedicated section, making it easy to see the distinction between:
- Full IDENTIFIER clause tests (using expressions, concatenation, variables)
- Identifier-lite tests (using only string literals)

## Future Enhancements

Potential future improvements:
1. Better error messages when users try to use expressions instead of literals
2. Support for identifier-lite in additional contexts (e.g., constraint names, index names)
3. Documentation updates in the SQL reference guide

# Legacy Configuration for IDENTIFIER Clause

## Overview

The identifier-lite feature introduces `IDENTIFIER('literal')` syntax that resolves string literals to identifiers at parse time. To maintain backward compatibility with the legacy `IDENTIFIER(expression)` behavior, a configuration option is provided.

## Configuration

### `spark.sql.legacy.identifierClause`

- **Type**: Boolean
- **Default**: `false` (identifier-lite enabled)
- **Internal**: Yes
- **Since**: 4.1.0

### Behavior

#### Default Behavior (`false`)
When `spark.sql.legacy.identifierClause = false` (default):
- **NEW**: `IDENTIFIER('literal')` is resolved at parse time to the identifier `literal`
- **LEGACY**: `IDENTIFIER(expression)` still works for dynamic table/schema references

Examples:
```sql
-- Identifier-lite: Resolved at parse time
SELECT IDENTIFIER('col1') FROM t;  -- Same as: SELECT col1 FROM t

-- Parameter markers work with identifier-lite
SELECT IDENTIFIER(:param) FROM t;  -- If :param = 'col1', same as SELECT col1 FROM t

-- String coalescing works with identifier-lite
SELECT IDENTIFIER('col' '1') FROM t;  -- Same as: SELECT col1 FROM t

-- Legacy IDENTIFIER clause still works
DECLARE table_name = 'my_table';
SELECT * FROM IDENTIFIER(table_name);  -- Evaluated at analysis time
```

#### Legacy-Only Behavior (`true`)
When `spark.sql.legacy.identifierClause = true`:
- **DISABLED**: `IDENTIFIER('literal')` is NOT allowed
- **LEGACY ONLY**: Only `IDENTIFIER(expression)` is allowed

Examples:
```sql
SET spark.sql.legacy.identifierClause = true;

-- This will FAIL with parse error
SELECT IDENTIFIER('col1') FROM t;

-- Only the legacy dynamic form works
DECLARE table_name = 'my_table';
SELECT * FROM IDENTIFIER(table_name);  -- Works
```

## Implementation Details

### Grammar Rule Guards

The identifier-lite alternatives are guarded by `{!legacy_identifier_clause_only}?` predicates:

```antlr
strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | {!legacy_identifier_clause_only}? IDENTIFIER_KW LEFT_PAREN stringLit RIGHT_PAREN  #identifierLiteral
    | ...
    ;

errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra                         #errorCapturingIdentifierBase
    | {!legacy_identifier_clause_only}? IDENTIFIER_KW LEFT_PAREN stringLit RIGHT_PAREN errorCapturingIdentifierExtra #identifierLiteralWithExtra
    ;
```

### Parser Precedence

The `identifierReference` rule is ordered to prioritize the legacy syntax:

```antlr
identifierReference
    : IDENTIFIER_KW LEFT_PAREN expression RIGHT_PAREN  // Legacy: try first
    | multipartIdentifier                               // Identifier-lite: try second
    ;
```

This ensures that when identifier-lite is enabled, the parser:
1. First tries to match the legacy `IDENTIFIER(expression)` syntax
2. Only if that fails (e.g., because it's a string literal), falls back to matching identifier-lite through `multipartIdentifier`

### Configuration Flow

1. **SQLConf.scala**: Defines `LEGACY_IDENTIFIER_CLAUSE_ONLY` config
2. **SqlApiConf.scala**: Trait method `def legacyIdentifierClauseOnly: Boolean`
3. **SQLConf.scala**: Implementation `getConf(LEGACY_IDENTIFIER_CLAUSE_ONLY)`
4. **parsers.scala**: Sets parser boolean: `parser.legacy_identifier_clause_only = conf.legacyIdentifierClauseOnly`
5. **SqlBaseParser.g4**: Grammar predicates check `{!legacy_identifier_clause_only}?`

## Use Cases

### When to Use Legacy Mode (`true`)

1. **Backward Compatibility**: Existing applications that rely exclusively on the legacy `IDENTIFIER(expression)` behavior
2. **Migration Period**: Temporarily disable identifier-lite while migrating code
3. **Testing**: Verify that code doesn't accidentally use identifier-lite syntax

### Recommended Settings

- **New Applications**: Keep default (`false`) to use identifier-lite
- **Existing Applications**: Test with default (`false`); use legacy mode (`true`) only if needed
- **Production**: Use default (`false`) for maximum flexibility

## Examples

### Complete Example: Both Modes

```sql
-- Default mode (identifier-lite enabled)
SET spark.sql.legacy.identifierClause = false;

CREATE TABLE my_table(col1 INT, col2 STRING);

-- Identifier-lite works
SELECT IDENTIFIER('col1') FROM my_table;  -- Returns col1 values

-- Legacy still works
DECLARE tab_name = 'my_table';
SELECT * FROM IDENTIFIER(tab_name);  -- Returns all rows

-- With parameters
SELECT IDENTIFIER(:col) FROM IDENTIFIER(:tab) USING 'col1' AS col, 'my_table' AS tab;

---

-- Legacy mode (identifier-lite disabled)
SET spark.sql.legacy.identifierClause = true;

-- Identifier-lite FAILS
SELECT IDENTIFIER('col1') FROM my_table;  -- PARSE ERROR

-- Legacy still works
DECLARE tab_name = 'my_table';
SELECT * FROM IDENTIFIER(tab_name);  -- Returns all rows
```

## Testing

The legacy behavior is tested in:
- `SQLViewTestSuite` - Test "SPARK-51552: Temporary variables under identifiers are not allowed in persisted view"
  - Verifies that legacy `IDENTIFIER(variable)` correctly evaluates at analysis time
  - Ensures proper error messages when temporary objects are referenced in persisted views

## Related Configurations

- `spark.sql.legacy.parameterSubstitution.constantsOnly`: Controls where parameter markers are allowed
- `spark.sql.legacy.setopsPrecedence.enabled`: Controls set operation precedence
- Both follow the same pattern of using grammar predicates for conditional syntax

## Migration Guide

### From Legacy to Identifier-Lite

1. **Audit Code**: Find all uses of `IDENTIFIER(expression)` where `expression` is a variable
2. **Replace with String Literals**:
   ```sql
   -- Before (legacy)
   DECLARE col_name = 'my_col';
   SELECT IDENTIFIER(col_name) FROM t;

   -- After (identifier-lite with parameters)
   SELECT IDENTIFIER(:col) FROM t USING 'my_col' AS col;
   ```
3. **Test**: Verify all queries work with default config
4. **Deploy**: Use default `spark.sql.legacy.identifierClause = false`

### If You Must Stay on Legacy

Set the configuration globally or per-session:
```sql
-- Spark SQL
SET spark.sql.legacy.identifierClause = true;

-- Spark properties file
spark.sql.legacy.identifierClause=true

-- SparkSession builder
spark.conf.set("spark.sql.legacy.identifierClause", "true")
```



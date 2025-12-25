# Cursor Support Implementation Summary

## Overview
This commit implements SQL cursor support for Spark SQL Scripting, enabling procedural iteration over query result sets.

## Implemented Features

### 1. SQL Syntax (Grammar)
- `DECLARE cursor_name [ASENSITIVE | INSENSITIVE] CURSOR FOR query [FOR READ ONLY]`
- `OPEN cursor_name`
- `FETCH [[NEXT] FROM] cursor_name INTO variable_name [, ...]`
- `CLOSE cursor_name`

### 2. Core Components

#### Logical Plan Nodes (`v2Commands.scala`)
- `DeclareCursor`: Represents cursor declaration
- `OpenCursor`: Opens a cursor and executes its query
- `FetchCursor`: Fetches next row into variables (extends `LeafCommand`)
- `CloseCursor`: Closes an open cursor

#### Physical Execution Nodes
- `DeclareCursorExec`: Registers cursor in execution context
- `OpenCursorExec`: Executes query and stores result set
- `FetchCursorExec`: Retrieves row and assigns to variables
- `CloseCursorExec`: Closes cursor and releases resources

#### Cursor State Management (`SqlScriptingExecutionContext.scala`)
```scala
case class CursorDefinition(
  name: String,
  query: LogicalPlan,
  var isOpen: Boolean = false,
  var resultData: Option[Array[InternalRow]] = None,
  var currentPosition: Int = -1,
  asensitive: Boolean = true
)
```

#### Analyzer Rule (`ResolveFetchCursor.scala`)
- Resolves `UnresolvedAttribute` to `VariableReference` in FETCH target variables
- Runs after `ResolveSetVariable` in the Resolution batch
- Handles both wrapped (`SingleStatement`) and unwrapped `FetchCursor` nodes

### 3. Key Design Decisions

#### Variable Resolution
Problem: `ResolveReferences` was resolving FETCH variables to their *values* (Literals) instead of *references* (VariableReference).

Solution: Added skip case in `ResolveReferences.doApply()`:
```scala
case f: FetchCursor => f  // Skip - let ResolveFetchCursor handle it
```

#### Declaration Order
Cursors must be declared:
- After variables and conditions
- Before handlers
- Before statements

Implemented via `CompoundBodyParsingContext` state machine.

#### Error Handling
- `CURSOR_NO_MORE_ROWS` with SQLSTATE `02000` (no data)
- SQLSTATE `02xxx` triggers NOT FOUND handlers
- Cursors automatically closed when exiting scope

### 4. Configuration Changes
- `SQL_SCRIPTING_CONTINUE_HANDLER_ENABLED` default changed to `true`
- Required for cursor iteration patterns with NOT FOUND handlers

## Testing

### Working Test Case
```sql
BEGIN
  DECLARE first, last STRING;
  DECLARE names CURSOR FOR
    SELECT first, last FROM VALUES('Bilbo', 'Baggins'), ('Peregrin', 'Tuck');
  OPEN names;
  FETCH names INTO first, last;
  CLOSE names;
  VALUES (first, last);
END;
```
Expected: Shows "Bilbo Baggins"

### Known Limitation
Complex interaction between CONTINUE handlers and cursor FETCH in iterative contexts (REPEAT/WHILE loops) requires further investigation. The issue is related to statement iterator state after exception handling.

Example with issue:
```sql
BEGIN
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET flag = true;
  REPEAT
    FETCH cursor INTO var;
    -- After exception and handler, iterator state may be corrupted
  UNTIL flag END REPEAT;
END;
```

## Files Modified

### New Files (5)
1. `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveFetchCursor.scala`
2. `sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/DeclareCursorExec.scala`
3. `sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/OpenCursorExec.scala`
4. `sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/FetchCursorExec.scala`
5. `sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/CloseCursorExec.scala`

### Modified Files (12)
1. `sql/api/src/main/antlr4/.../SqlBaseLexer.g4` - Added cursor keywords
2. `sql/api/src/main/antlr4/.../SqlBaseParser.g4` - Added grammar rules
3. `sql/catalyst/.../v2Commands.scala` - Added logical plan nodes
4. `sql/catalyst/.../AstBuilder.scala` - Added visitor methods
5. `sql/catalyst/.../Analyzer.scala` - Added ResolveFetchCursor, skip FetchCursor in ResolveReferences
6. `sql/catalyst/.../ParserUtils.scala` - Updated declaration order validation
7. `sql/catalyst/.../RuleIdCollection.scala` - Registered ResolveFetchCursor
8. `sql/catalyst/.../SQLConf.scala` - Enabled CONTINUE handlers
9. `sql/catalyst/.../SqlScriptingErrors.scala` - Added cursor error messages
10. `sql/core/.../V2CommandStrategy.scala` - Added cursor command mappings
11. `sql/core/.../SqlScriptingExecutionContext.scala` - Added cursor state management
12. `common/utils/.../error-conditions.json` - Added cursor error conditions

## Error Conditions Added
- `CURSOR_ALREADY_EXISTS` (SQLSTATE 42710)
- `CURSOR_ALREADY_OPEN` (SQLSTATE 24502)
- `CURSOR_NOT_FOUND` (SQLSTATE 34000)
- `CURSOR_NOT_OPEN` (SQLSTATE 24501)
- `CURSOR_NO_MORE_ROWS` (SQLSTATE 02000) - Triggers NOT FOUND handlers
- `CURSOR_FETCH_COLUMN_MISMATCH` (SQLSTATE 42826)
- `CURSOR_OUTSIDE_SCRIPT` (SQLSTATE 0A000)
- `INVALID_CURSOR_DECLARATION.WRONG_PLACE_OF_DECLARATION` (SQLSTATE 42K0Q)

## Next Steps

### High Priority
1. Investigate and fix CONTINUE handler + FETCH interaction in loops
   - Issue: Statement iterator state after exception handling
   - Consider: Statement consumption timing vs exception propagation

### Medium Priority
2. Add comprehensive test suite
   - Basic cursor operations
   - Error cases
   - Scope management
   - Handler interactions

3. Performance optimization
   - Consider lazy evaluation options
   - Optimize result set storage

### Low Priority
4. Advanced features (future consideration)
   - Scrollable cursors (PRIOR, FIRST, LAST, ABSOLUTE, RELATIVE)
   - Cursor sensitivity options
   - UPDATE/DELETE WHERE CURRENT OF

## Compilation Status
✅ Both `catalyst` and `sql` modules compile cleanly
✅ All analyzer rules registered
✅ All error conditions defined

## Commit
Branch: `cursors`
Commit: `bdd15a98872`





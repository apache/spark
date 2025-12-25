# Cursor Scoping and State Management - Test Results

## Summary
All cursor scoping and state validation tests are passing successfully and integrated into the Spark SQL test suite.

## Running the Tests

The cursor tests are integrated into Spark's SQL query test framework. To run them:

```bash
# Run all cursor tests
build/sbt "sql/testOnly org.apache.spark.sql.SQLQueryTestSuite -- -z cursors.sql"

# Regenerate golden files if needed
SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.SQLQueryTestSuite -- -z cursors.sql"
```

## Test Results

### Scoping Rules
- ✅ **Test 1a**: Cursors have a separate namespace from local variables
- ✅ **Test 1b**: Duplicate cursor names in same compound statement are not allowed
- ✅ **Test 1c**: Inner scope cursors shadow outer scope cursors

### State Management
- ✅ **Test 2a**: A cursor cannot be opened twice
- ✅ **Test 2b**: A cursor can be closed and then re-opened
- ✅ **Test 2c**: A cursor that is not open cannot be closed
- ✅ **Test 2d**: A cursor cannot be closed twice
- ✅ **Test 2e**: A cursor that is not open cannot be fetched (initial state)
- ✅ **Test 2e variant**: A cursor that is not open cannot be fetched (after closing)

### Additional Tests
- ✅ **Additional test**: Cursor state is independent across scopes

## Implementation Details

### Error Classes Implemented
- `CURSOR_ALREADY_EXISTS` (SQLSTATE 42723): Cursor already declared in current scope
- `CURSOR_ALREADY_OPEN` (SQLSTATE 24502): Cursor already open
- `CURSOR_NOT_FOUND` (SQLSTATE 34000): Cursor not found in scope hierarchy
- `CURSOR_NOT_OPEN` (SQLSTATE 24501): Cursor not open for fetch/close
- `CURSOR_NO_MORE_ROWS` (SQLSTATE 02000): No more data (NOT FOUND condition)
- `CURSOR_FETCH_COLUMN_MISMATCH` (SQLSTATE 42826): Column/variable count mismatch
- `CURSOR_OUTSIDE_SCRIPT` (SQLSTATE 0A000): Cursor operation outside script context

### Validation Rules Enforced
1. **Duplicate Detection**: DeclareCursorExec checks for duplicate cursor names in the current scope
2. **Open Validation**: OpenCursorExec prevents opening an already-open cursor
3. **Close Validation**: CloseCursorExec requires cursor to be open before closing
4. **Fetch Validation**: FetchCursorExec requires cursor to be open before fetching
5. **Scope Independence**: Each scope maintains its own cursor namespace
6. **Shadowing**: Inner scope cursors with the same name shadow outer scope cursors
7. **Declaration Order**: Variables/Conditions must be declared before Cursors, which must be declared before Handlers

### Files Modified
- `SqlBaseParser.g4`: Grammar rules for cursor statements
- `v2Commands.scala`: Logical plan nodes (OpenCursor, FetchCursor, CloseCursor)
- `AstBuilder.scala`: Parser implementation for cursor statements
- `SqlScriptingExecutionContext.scala`: Cursor storage and lookup in scopes
- `DeclareCursorExec.scala`: Duplicate cursor detection
- `OpenCursorExec.scala`: Open state validation
- `CloseCursorExec.scala`: Close state validation
- `FetchCursorExec.scala`: Fetch state validation
- `error-conditions.json`: Error messages and SQLSTATEs
- `ParserUtils.scala`: Declaration order validation (state machine)

## Test Files

### SQL Test Files
- **Input**: `sql/core/src/test/resources/sql-tests/inputs/scripting/cursors.sql`
- **Golden Output**: `sql/core/src/test/resources/sql-tests/results/scripting/cursors.sql.out`

### Test Framework
The tests use Spark's `SQLQueryTestSuite` framework with query delimiters (`--QUERY-DELIMITER-START`/`--QUERY-DELIMITER-END`) to properly handle multi-statement BEGIN/END blocks. The test configuration enables CONTINUE handlers with `--SET spark.sql.scripting.continueHandlerEnabled=true`.

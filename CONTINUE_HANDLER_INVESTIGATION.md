# Investigation: CONTINUE Handler returnCurrentWithoutAdvancing Flag

## Background
The code review identified a concerning pattern in the CONTINUE handler implementation:
- A complex `returnCurrentWithoutAdvancing` flag was added to `CompoundBodyExec`
- A helper method `setReturnCurrentWithoutAdvancing()` with nested logic to find the right execution frame
- The comments were somewhat contradictory about the behavior

## Investigation Results

### Finding 1: Patch Is NOT Needed for General CONTINUE Handler Behavior
**Test Result**: ✅ All 23 continue handler tests in `SqlScriptingExecutionSuite` pass WITHOUT the patch

This includes:
- CONTINUE handlers in IF/ELSE/CASE statements
- CONTINUE handlers in WHILE/REPEAT/FOR loops
- Nested loops with error handling
- Complex control flow scenarios

**Conclusion**: The patch is NOT needed for general CONTINUE handler correctness.

### Finding 2: Patch IS Specific to CURSOR Behavior
**Test Result**: ❌ 11 cursor tests fail when the patch is removed

Failing tests:
- Test 77: Unhandled NO DATA condition
- Test 81: EXIT HANDLER vs CONTINUE HANDLER precedence with loops
- Test 86: FETCH and CLOSE in handler BEGIN block

**Error observed**:
```
org.apache.spark.sql.AnalysisException: [CURSOR_NO_MORE_ROWS] No more rows available to fetch from cursor `cur`. SQLSTATE: 02000
```

### Finding 3: The Issue Is About CURSOR Completion Conditions

The SQL Standard defines `CURSOR_NO_MORE_ROWS` (SQLSTATE '02000') as a **completion condition**, not an error:
- **Without handler**: Execution should CONTINUE (like a warning)
- **With CONTINUE handler**: Handler executes, then execution continues
- **With EXIT handler**: Handler executes, then exits the compound

The test that fails:
```sql
BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1 AS val;

  OPEN cur;
  FETCH cur INTO x;  -- Succeeds, x = 1
  VALUES ('After first fetch', x);

  FETCH cur INTO x;  -- No more rows, raises CURSOR_NO_MORE_ROWS (SQLSTATE 02000)
                     -- No handler declared, so per SQL Standard this is a completion
                     -- condition (not exception) and execution continues

  VALUES ('After second fetch - script continued', x);  -- Should execute, x still = 1
  CLOSE cur;
END;
```

**Expected**: Script continues after the second FETCH
**Actual** (without patch): Exception propagates and aborts the script

## Analysis

### The Real Problem - YOU'RE EXACTLY RIGHT!

**Key Insight**: `CURSOR_NO_MORE_ROWS` is the **FIRST and ONLY** statement in Spark that raises a SQLSTATE '02xxx' (NOT FOUND / NO DATA) condition!

Verification:
```bash
$ grep '"sqlState" : "02' error-conditions.json
    "sqlState" : "02000"  # ONLY CURSOR_NO_MORE_ROWS
```

**This means**:
1. Before cursors, Spark never encountered a "completion condition" (SQLSTATE '02xxx')
2. All existing exception handling assumes exceptions are fatal errors
3. The SQL Standard requires SQLSTATE '02xxx' to be **completion conditions**, not exceptions:
   - They should allow script continuation even WITHOUT a handler
   - They're more like warnings than errors
4. The exception handling code in `SqlScriptingExecution` doesn't distinguish between:
   - **Completion conditions** (SQLSTATE '02xxx') - should continue
   - **True exceptions** (other SQLSTATE) - should abort without handler

### Why The Patch "Worked"
The `returnCurrentWithoutAdvancing` flag was a workaround that:
- Made the iterator behavior different after exception handling
- Accidentally allowed continuation in some cursor scenarios
- But it's NOT the right solution because:
  - It's too complex and hard to understand
  - It doesn't address the root cause (completion condition vs exception)
  - It couples cursor behavior to iterator state management

## The Right Solution

Instead of patching the iterator behavior, we should:

1. **Distinguish completion conditions from exceptions**:
   - Add a marker trait `CompletionCondition` for exceptions that should allow continuation
   - Mark `CURSOR_NO_MORE_ROWS` as a completion condition

2. **Update the exception handling logic**:
   - In `SqlScriptingExecution.handleException()`:
     - If no handler found AND exception is a completion condition → continue execution
     - If no handler found AND exception is a true exception → propagate
     - If handler found → execute handler regardless of completion condition status

3. **This aligns with SQL Standard behavior**:
   - Completion conditions (SQLSTATE class '02') allow continuation
   - Exceptions (other SQLSTATE classes) require handlers or abort

## Non-Cursor Reproducer - THIS IS NOT ABOUT CONTINUE HANDLERS!

**Critical Point**: This issue exists even WITHOUT any CONTINUE handlers declared!

The following test demonstrates that this is purely about completion condition handling:

```sql
-- NO HANDLER DECLARED AT ALL!
-- This test shows the issue is about completion conditions, not CONTINUE handlers

BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1 AS val;

  OPEN cur;
  FETCH cur INTO x;  -- Success, x = 1
  SELECT 'After first fetch', x;

  -- This FETCH raises CURSOR_NO_MORE_ROWS (SQLSTATE 02000)
  -- Per SQL Standard: SQLSTATE '02xxx' = completion condition (not exception)
  -- Expected behavior: Script continues without aborting (NO HANDLER NEEDED!)
  FETCH cur INTO x;

  SELECT 'Script continued - completion condition did not abort', x;
  CLOSE cur;
END;
```

**On master (without patch)**: ❌ Fails with `CURSOR_NO_MORE_ROWS` exception - script aborts
**Expected per SQL Standard**: ✅ Should pass - completion conditions don't abort without handler
**With returnCurrentWithoutAdvancing patch**: ✅ Passes (but for wrong reasons)

This proves:
1. **The issue is NOT about CONTINUE handler iteration logic**
2. **The issue IS about treating SQLSTATE '02xxx' as completion conditions**
3. **Cursors introduced the first completion condition to Spark**
4. **Spark's exception handling doesn't distinguish completion conditions from exceptions**

## Recommendation

1. **Remove the `returnCurrentWithoutAdvancing` patch** ✅ Done
2. **Implement proper completion condition handling**:
   - Create `CompletionCondition` trait
   - Mark `CURSOR_NO_MORE_ROWS` appropriately
   - Update exception handling logic
3. **Add clear documentation** about completion conditions vs exceptions
4. **Add tests** that specifically verify completion condition behavior

## Files Affected by Patch (Now Reverted)

- `sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala`
  - Removed `returnCurrentWithoutAdvancing` flag
  - Removed special logic in `next()` method

- `sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala`
  - Removed `setReturnCurrentWithoutAdvancing()` helper method
  - Removed call to this method in exception handling

## Next Steps

1. ✅ Revert the patch (done)
2. ⏭️ Implement proper completion condition handling
3. ⏭️ Verify all tests pass with the correct solution
4. ⏭️ Add documentation about SQL Standard completion conditions

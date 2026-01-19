# Iterator State Management Issue - Debug Branch

## Purpose of This Branch

This branch (`cursors-debug-iterator-issue`) is a subbranch of `cursors` created specifically for reviewers to debug and understand the iterator state management issue that occurs when completion conditions are raised within loops.

## What Was Backed Out

The `returnCurrentWithoutAdvancing` flag and its associated logic have been removed from:
1. **SqlScriptingExecutionNode.scala**: Removed the flag declaration and the special handling in the `next()` method
2. **SqlScriptingExecution.scala**: Removed the `setReturnCurrentWithoutAdvancing()` helper method and its call site

## What Remains

The completion condition handling logic remains in place:
- `handleException()` in `SqlScriptingExecution.scala` distinguishes between completion conditions (SQLSTATE '02xxx') and true exceptions
- Unhandled completion conditions allow execution to continue instead of aborting the script
- This aligns with the SQL Standard where SQLSTATE class '02' represents "no data" completion conditions, not errors

## The Issue

### Symptoms
When running cursor tests that involve FETCH statements inside REPEAT/WHILE loops with CONTINUE handlers:
```
org.apache.spark.SparkException: [INTERNAL_ERROR] No more elements to iterate 
through in the current SQL compound statement. SQLSTATE: XX000
```

### Affected Tests
7 cursor tests fail with this error (all involve loops with FETCH statements):
- Test 20: Cursor sensitivity - cursor captures snapshot when opened
- Test 21: Basic parameterized cursor with positional parameters  
- Test 22: Parameterized cursor with named parameters
- Test 23: Parameterized cursor - reopen with different parameters
- Test 24: Parameterized cursor with expressions
- Test 72: Complex SQL in DECLARE - Recursive CTE
- Test 81: EXIT HANDLER vs CONTINUE HANDLER precedence with loops

### Example Failing Test (Test 21)
```sql
BEGIN
  DECLARE nomorerows BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT id, value FROM ... WHERE id >= ? AND id <= ?;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET nomorerows = true;

  OPEN cur USING min_id, max_id;

  REPEAT
    FETCH cur INTO fetched_id, fetched_value;  -- Eventually raises CURSOR_NO_MORE_ROWS
    IF NOT nomorerows THEN
      SET result = result || fetched_value;
    END IF;
  UNTIL nomorerows END REPEAT;

  CLOSE cur;
  SELECT result;
END;
```

### Root Cause Analysis

#### The Problem
When an exception or completion condition occurs:
1. The failing statement (e.g., `FETCH cur INTO ...`) is executed
2. It throws an exception mid-execution
3. The exception is caught in `getNextResult()` which calls `handleException()`
4. If there's a handler, a handler frame is created and pushed onto the stack
5. `getNextResult()` is called recursively to continue execution
6. Eventually, `getNextStatement()` is called which calls `next()` on the iterator
7. But the iterator's state is inconsistent: the failing statement has been consumed but `curr` hasn't advanced properly
8. Result: `curr` is `None` when `next()` is called, causing "No more elements" error

#### Why Non-Cursor Tests Don't Fail
Non-cursor continue handler tests don't typically raise exceptions inside tight loops with completion conditions. The cursor tests have a unique pattern:
- REPEAT/WHILE loop
- FETCH statement inside the loop that eventually raises `CURSOR_NO_MORE_ROWS` 
- CONTINUE handler that sets a variable
- Loop continues based on that variable

This creates a scenario where:
- The FETCH raises CURSOR_NO_MORE_ROWS
- The handler executes and sets `nomorerows = true`
- We return to the loop body to continue iteration
- The iterator tries to advance to get the next statement
- But there's confusion about whether we're still on the FETCH statement or should move to the next one

#### The Backed Out Fix
The `returnCurrentWithoutAdvancing` flag was introduced to address this by:
1. After handling any exception, set the flag on the relevant `CompoundBodyExec`
2. When the iterator's `next()` method is called with this flag set:
   - Return the current statement without advancing
   - Then advance `curr` to the next statement
   - Clear the flag
3. This prevents double-advancement of the iterator

The problems with the original implementation:
- ❌ Poor documentation (contradictory comments)
- ❌ Complex nested logic to find the right execution frame
- ❌ Didn't clearly explain WHY it was needed
- ✅ But it DID solve the iterator state problem (all 87 tests passed)

## How to Reproduce and Debug

### Quick Test
```bash
cd /Users/serge.rielau/spark
build/sbt "sql/testOnly *SqlScriptingCursorE2eSuite -- -z \"Test 21:\""
```

You should see:
```
org.apache.spark.SparkException: [INTERNAL_ERROR] No more elements to iterate 
through in the current SQL compound statement.
```

### Run All Affected Tests
```bash
build/sbt "sql/testOnly *SqlScriptingCursorE2eSuite"
```

Expected: 80 succeeded, 7 failed

### Test Completion Condition Handling
The simpler test case (without loops) works fine:
```bash
cat > /tmp/test_completion_condition.sql << 'EOF'
BEGIN
  DECLARE x INT;
  DECLARE cur CURSOR FOR SELECT 1 AS val;

  OPEN cur;
  FETCH cur INTO x;  -- Success, x = 1
  SELECT 'After first fetch', x;
  
  FETCH cur INTO x;  -- Raises CURSOR_NO_MORE_ROWS (SQLSTATE 02000)
  
  SELECT 'Script continued - completion condition did not abort', x;
  CLOSE cur;
END;
EOF

build/spark-sql --conf spark.sql.scripting.cursor.enabled=true -f /tmp/test_completion_condition.sql
```

This works because there's no loop iterator involved.

## Questions for Investigation

1. **When should `curr` be advanced?**
   - After a statement executes successfully?
   - After a statement throws an exception?
   - After an exception handler completes?

2. **Is the issue specific to CONTINUE handlers, or does it affect EXIT handlers too?**
   - Test 81 involves both EXIT and CONTINUE handlers

3. **Is there a cleaner way to manage iterator state than the flag approach?**
   - Could we explicitly track "statement already consumed" vs "needs to be consumed"?
   - Should exception handling happen at a different level in the iterator?

4. **Why do FOR loops work but REPEAT loops don't?**
   - Both are loops, but maybe they manage their iterators differently?

5. **Is this issue specific to cursors, or could it affect other statement types?**
   - Why haven't we seen this with variable assignments in loops?
   - Is it because FETCH is unique in raising completion conditions inside loops?

## Testing Strategy

### Verify Non-Cursor Tests Still Pass
```bash
# Continue handler tests without cursors
build/sbt "sql/testOnly *SqlScriptingExecutionSuite -- -z \"continue handler\""

# Expected: All 23 tests pass
```

### Create Minimal Reproducer
Try to create the simplest possible test case that reproduces the issue:
```sql
BEGIN
  DECLARE x INT;
  DECLARE done BOOLEAN DEFAULT false;
  DECLARE cur CURSOR FOR SELECT 1;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;

  OPEN cur;
  REPEAT
    FETCH cur INTO x;
  UNTIL done END REPEAT;
  CLOSE cur;
END;
```

## Comparison with Main Cursors Branch

The main `cursors` branch has:
- ✅ Completion condition handling (SQLSTATE '02xxx')
- ✅ `returnCurrentWithoutAdvancing` flag and logic
- ✅ All 87 cursor tests passing

This debug branch has:
- ✅ Completion condition handling (SQLSTATE '02xxx')  
- ❌ `returnCurrentWithoutAdvancing` flag and logic removed
- ❌ 7 cursor tests failing with iterator state errors

## Recommended Next Steps

1. **Add detailed logging** to understand the iterator state:
   - Log when `curr` is set/cleared
   - Log when `next()` is called
   - Log when exceptions are raised/handled
   - Log the call stack at each of these points

2. **Step through with debugger** on Test 21:
   - Set breakpoint in `CompoundBodyExec.next()`
   - Watch `curr` variable
   - Watch `localIterator.hasNext`
   - Follow execution through exception handling

3. **Analyze the execution flow** for:
   - Normal case: FETCH succeeds
   - Edge case: FETCH raises CURSOR_NO_MORE_ROWS
   - Compare: What's different about the iterator state in each case?

4. **Consider alternative solutions**:
   - Should exception handling reset iterator state explicitly?
   - Should handlers be injected into the iterator rather than using frames?
   - Is there a way to make iterator advancement more explicit/traceable?

## Files to Review

Key files for understanding this issue:
- `/Users/serge.rielau/spark/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala` - Iterator logic
- `/Users/serge.rielau/spark/sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala` - Exception handling
- `/Users/serge.rielau/spark/sql/core/src/test/scala/org/apache/spark/sql/scripting/SqlScriptingCursorE2eSuite.scala` - Failing tests

For comparison with working version:
```bash
# Show what was removed
git diff cursors sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionNode.scala
git diff cursors sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecution.scala
```

## Documentation Created

Additional context in the main cursors branch:
- `COMPLETION_CONDITION_ROOT_CAUSE.md` - Explains why CURSOR_NO_MORE_ROWS is special
- `COMPLETION_CONDITION_FIX_STATUS.md` - Status of the fix implementation
- `CONTINUE_HANDLER_INVESTIGATION.md` - Initial investigation of handler behavior

---

**Branch Created:** 2026-01-19  
**Created From:** `cursors` branch  
**Purpose:** Debug iterator state management issue with completion conditions in loops  
**Status:** Ready for reviewer investigation

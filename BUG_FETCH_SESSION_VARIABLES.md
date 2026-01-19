# BUG: FETCH INTO Cannot Resolve Session Variables

## Issue Summary

FETCH INTO statements inside BEGIN...END blocks cannot resolve session variables that were declared at the top level, even though those same variables can be accessed via SELECT statements.

## Evidence

### Test Case: Test 27 (cursors.sql lines 783-797)

```sql
-- Declare session variables at top level
DECLARE session_x INT DEFAULT 0;
DECLARE session_y STRING DEFAULT '';

-- Try to FETCH INTO them inside a script
BEGIN
  DECLARE cur CURSOR FOR SELECT 42 AS num, 'hello' AS text;
  OPEN cur;
  FETCH cur INTO session_x, session_y;  -- FAILS HERE
  CLOSE cur;
END;

-- But SELECT can access them
SELECT session_x, session_y;  -- SUCCEEDS, returns 0 and ''
```

### Actual Result (cursors.sql.out lines 944-970)

1. **DECLARE statements succeed** (lines 928-940): Session variables are created
2. **FETCH fails** (lines 953-961):
   ```
   org.apache.spark.sql.AnalysisException
   {
     "errorClass" : "UNRESOLVED_VARIABLE",
     "sqlState" : "42883",
     "messageParameters" : {
       "searchPath" : "`session`",
       "variableName" : "`session_x`"
     }
   }
   ```
3. **SELECT succeeds** (lines 965-970): Returns `0` and empty string (the defaults)

## Analysis

The session variables **DO exist** (proven by SELECT working), but **FETCH cannot find them**.

### Variable Resolution Logic

`ResolveFetchCursor` (lines 60-66) uses `VariableResolution.lookupVariable()` which should:
1. First check scripting local variables (`SqlScriptingContextManager`)
2. Then check session variables (`tempVariableManager`)

### Hypothesis

The issue is likely in how the variable lookup is performed **within the context of a BEGIN...END block**:
- When FETCH runs inside a script, it may be looking for variables in the **scripting execution context**
- Session variables are in `tempVariableManager` which is separate
- The error message shows `searchPath: "session"` suggesting it IS checking session scope
- But it's not finding the variable even though it exists

### Possible Root Causes

1. **Scope isolation**: BEGIN...END creates a new variable scope that doesn't inherit session variables
2. **Resolution order bug**: The lookup might be failing to fall through to session variables
3. **Context mismatch**: The `tempVariableManager` instance used during resolution might be different from the one where variables were stored

## Impact

**Tests 27-31 all fail** due to this bug:
- Test 27: FETCH INTO session variables
- Test 28: Mixing local and session variables
- Test 29: Session variables with type casting
- Test 30: Duplicate session variable (can't test until basic case works)
- Test 31: Duplicate across local/session (can't test until basic case works)

## Expected Behavior

FETCH INTO should be able to access session variables just like SELECT can. The SQL/PSM standard and the test expectations both indicate this should work.

## Recommendation

This is a **BUG that needs to be fixed**, not a limitation to document. The implementation should allow FETCH INTO to resolve and write to session variables declared at the top level.

## Files Involved

- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveFetchCursor.scala`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/VariableResolution.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/command/v2/FetchCursorExec.scala`

## Next Steps

1. Debug why `VariableResolution.lookupVariable()` isn't finding session variables in script context
2. Verify that `tempVariableManager` is accessible during FETCH resolution
3. Fix the resolution logic to properly fall through to session variables
4. Verify Tests 27-31 pass after fix

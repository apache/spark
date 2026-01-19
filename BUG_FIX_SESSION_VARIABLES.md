# Bug Fix: FETCH INTO Session Variables

## Summary
Fixed a bug where `FETCH cursor INTO session_variable` failed with `UNRESOLVED_VARIABLE` error when attempting to fetch into session variables from inside `BEGIN...END` blocks. The bug was in `FetchCursorExec` which always used the scripting local variable manager, ignoring the `catalog` field in `VariableReference` that indicates whether a variable is local or session-scoped.

## Root Cause
The bug was introduced when cursor support was implemented. `FetchCursorExec` was hardcoded to use only the scripting local variable manager:

```scala
val variableManager = SqlScriptingContextManager.get().get.getVariableManager
```

This worked fine for local variables but failed for session variables because session variables are managed by `TempVariableManager`, not by the scripting context's local variable manager.

## The Fix
Updated `FetchCursorExec.assignToVariable()` to check the `varRef.catalog` field and select the appropriate variable manager, matching the logic already present in `SetVariableExec.setVariable()`:

```scala
val variableManager = varRef.catalog match {
  case FakeLocalCatalog => scriptingVariableManager.get
  case FakeSystemCatalog => tempVariableManager
  case c => throw SparkException.internalError("Unexpected catalog: " + c)
}
```

## Investigation Process
1. **Initial hypothesis**: Variable resolution was failing during analysis
   - Added debug logging to `VariableResolution.lookupVariable()`
   - **Result**: Resolution was working correctly! Session variables were found.

2. **Key insight**: Error was happening at **execution time**, not analysis time
   - Stack trace showed error in `SqlScriptingLocalVariableManager.set()`
   - The resolved variable information was correct, but assignment was using wrong manager

3. **Comparison with SET**:
   - Created test showing `SET` works but `FETCH` doesn't with session variables
   - Both use identical resolution code (`VariableResolution.lookupVariable()`)
   - Difference was in **execution**: `SetVariableExec` checks `catalog`, `FetchCursorExec` didn't

## Files Changed
1. **FetchCursorExec.scala**: Updated `assignToVariable()` to check `varRef.catalog`
2. **VariableAssignmentUtils.scala**: Created shared utility for future refactoring (not yet integrated)
3. **cursors.sql**: Fixed Test 27 to use `DECLARE` instead of `SET VAR` for creating session variables
4. **cursors.sql.out**: Regenerated golden file with correct test results

## Test Results
All tests now pass:
- **Test 27**: `FETCH` into session variables - ✅ Returns `42	hello`
- **Test 28**: `FETCH` mixing local and session variables - ✅ Returns `100	world`
- **Test 29**: `FETCH` with type casting for session variables - ✅ Returns `99	42`
- **Test 30**: Duplicate session variable in `FETCH` - ✅ Correctly errors
- **Test 31**: Duplicate across local/session in `FETCH` - ✅ Correctly errors

## Future Work
The catalog-checking logic is now duplicated between `SetVariableExec` and `FetchCursorExec`. A follow-up refactoring should extract this into the shared `VariableAssignmentUtils.assignVariable()` method that was created as part of this fix.

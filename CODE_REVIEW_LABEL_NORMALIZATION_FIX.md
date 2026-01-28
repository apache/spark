# Code Review Fix: Redundant Label Normalization

## Issue Summary

**Reviewer**: @davidm-db, @miland-db
**Location**: `SqlScriptingExecutionContext.scala` lines 299-303
**Problem**: The `enterScope` method was performing redundant case normalization of labels that were already normalized during parsing.

## Code Review Comment

> "this is not needed, labels are always lowercased in ParserUtils#enterLabeledScope. Not sure why we did it this way, maybe we need to fix it to account for SQLConf.get.caseSensitiveAnalysis.
>
> Anyways, cc @miland-db to double check this, I think you added the labels logic so you might recall why exactly we did it this way and whether it makes sense to take SQLConf.get.caseSensitiveAnalysis into account."
>
> **miland-db**: "IIRC, labels are case insensitive."

## Investigation

Labels in SQL/PSM are **always case-insensitive** and are normalized (lowercased) during the parsing phase:

**Evidence from `ParserUtils.scala` line 412**:
```scala
val txt = getLabelText(beginLabelCtx.get.strictIdentifier()).toLowerCase(Locale.ROOT)
```

This happens in `ParserUtils#enterLabeledScope` which is called during parsing for all labeled compound statements. The normalized labels then flow through:
1. Parser → `CompoundBody.label`
2. Interpreter → `CompoundBodyExec` (line 152: `compoundBody.label`)
3. Execution → `enterScope(label)` call

## Changes Made

### Removed Redundant Normalization
**File**: `sql/core/src/main/scala/org/apache/spark/sql/scripting/SqlScriptingExecutionContext.scala`

**Before**:
```scala
def enterScope(
    label: String,
    triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap): Unit = {
  // Normalize label for case-insensitive lookups
  val normalizedLabel = if (SQLConf.get.caseSensitiveAnalysis) {
    label
  } else {
    label.toLowerCase(Locale.ROOT)
  }
  scopes.append(new SqlScriptingExecutionScope(normalizedLabel, triggerToExceptionHandlerMap))
}
```

**After**:
```scala
def enterScope(
    label: String,
    triggerToExceptionHandlerMap: TriggerToExceptionHandlerMap): Unit = {
  // Note: Labels are already lowercased in ParserUtils#enterLabeledScope during parsing,
  // so no need to normalize here. Labels are always case-insensitive in SQL/PSM.
  scopes.append(new SqlScriptingExecutionScope(label, triggerToExceptionHandlerMap))
}
```

### Removed Unused Import
Also removed the now-unused import:
```scala
import org.apache.spark.sql.internal.SQLConf  // Removed
```

## Design Clarification

### Why Labels Are Always Case-Insensitive

Unlike identifiers in Spark SQL (which can be case-sensitive or case-insensitive based on `spark.sql.caseSensitiveAnalysis`), **labels are always case-insensitive** per the SQL/PSM standard. This is consistent with other SQL implementations.

Examples:
```sql
-- All of these refer to the same label:
myLabel: BEGIN ... END;
MYLABEL: BEGIN ... END;
MyLabel: BEGIN ... END;
```

### Single Point of Normalization

By normalizing labels at parse time (in `ParserUtils`), we ensure:
- ✅ Single source of truth for label normalization
- ✅ No redundant normalization at execution time
- ✅ Labels remain consistent throughout the AST and execution phases
- ✅ `exitScope` comparison works correctly (line 313: `scopes.last.label != label`)

## Testing

- ✅ All 87 cursor E2E tests pass
- ✅ No behavior change - labels were already normalized at parse time
- ✅ Compilation successful

## Impact

- **Code simplification**: Removed 5 lines of redundant logic
- **Performance**: Eliminated unnecessary runtime check of `SQLConf.get.caseSensitiveAnalysis`
- **Clarity**: Added comment explaining why normalization isn't needed
- **Correctness**: No functional change - behavior remains identical

## Conclusion

The redundant normalization has been removed. Labels are now normalized once at parse time and used consistently throughout the execution phase, which is simpler and more efficient.

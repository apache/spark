# Cursor FETCH Performance Optimization

## Issue
**Reported by**: @davidm-db
**Related PR Comment**: [Code Review Comment](https://github.com/apache/spark/pull/...)

## Problem
The original implementation of `FetchCursorExec` was performing an unnecessary round-trip conversion of row data:

1. **InternalRow → Row**: `df.toLocalIterator()` converts internal Spark rows to external Row format
2. **Row → InternalRow**: `FetchCursorExec` immediately converts back using `CatalystTypeConverters`

This double conversion happened on every `FETCH` operation, introducing unnecessary overhead.

### Original Flow
```scala
// In FetchCursorExec (OLD CODE)
val iter = df.toLocalIterator()  // Returns Iterator[Row]
// ... store in CursorFetching state ...

// On each fetch:
val externalRow = iterator.next()  // Get Row
val schema = DataTypeUtils.fromAttributes(analyzedQuery.output)
val converter = CatalystTypeConverters.createToCatalystConverter(schema)
val currentRow = converter(externalRow).asInstanceOf[InternalRow]  // Convert back!
```

## Solution
Use `executeToIterator()` instead of `toLocalIterator()` to work directly with `InternalRow` format throughout the cursor lifecycle.

### Optimized Flow
```scala
// In FetchCursorExec (NEW CODE)
val iter = df.queryExecution.executedPlan.executeToIterator()  // Returns Iterator[InternalRow]
// ... store in CursorFetching state ...

// On each fetch:
val currentRow = iterator.next()  // Get InternalRow directly - no conversion!
```

## Changes

### 1. CursorState.scala
Changed `CursorFetching` to store `Iterator[InternalRow]` instead of `Iterator[Row]`:

```scala
case class CursorFetching(
    analyzedQuery: LogicalPlan,
    resultIterator: Iterator[InternalRow])  // Changed from java.util.Iterator[Row]
  extends CursorState
```

### 2. FetchCursorExec.scala
- Changed from `df.toLocalIterator()` to `df.queryExecution.executedPlan.executeToIterator()`
- Removed the conversion logic (lines 93-98 in the old implementation)
- Iterator now yields `InternalRow` directly

## Benefits

1. **Performance**: Eliminates unnecessary data conversions on every fetch operation
2. **Memory**: Avoids creating intermediate external `Row` objects
3. **Consistency**: Aligns with internal Spark execution patterns (e.g., `ForStatementExec` has similar opportunities)

## Testing
- All 87 existing tests in `SqlScriptingCursorE2eSuite` pass without modification
- No behavior changes - purely a performance optimization
- The partition-by-partition collection behavior is preserved (both `toLocalIterator()` and `executeToIterator()` use the same underlying mechanism)

## Related Work
Similar optimization opportunities exist in:
- `ForStatementExec`: Currently uses public Row values, could directly update variable values with InternalRow
- Other scripting features that iterate over query results

## Performance Impact
The impact will be most noticeable for:
- Cursors with many fetch operations
- Large result sets with complex data types
- High-throughput scripting workloads

The optimization avoids:
- Schema validation on every conversion
- Type converter creation overhead
- Data copying between internal and external formats

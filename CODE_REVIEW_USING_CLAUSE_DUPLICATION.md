# Code Review: USING Clause Duplication Between EXECUTE IMMEDIATE and OPEN CURSOR

## Issue Summary

**Reviewer**: @davidm-db
**Location**: `OpenCursorExec.scala`
**Problem**: EXECUTE IMMEDIATE and OPEN CURSOR have identical USING clause semantics but implement them differently, leading to code duplication and potential divergence.

## Current Implementation Differences

### EXECUTE IMMEDIATE (Analysis Time)
**File**: `ResolveExecuteImmediate.scala` (sql/core)
**Approach**: Handles Alias extraction during **analysis phase**

```scala
// In ResolveExecuteImmediate.buildUnifiedParameters()
args.foreach {
  case alias: Alias =>
    val paramValue = evaluateParameterExpression(alias.child)
    values += paramValue
    names += alias.name  // Extract name from Alias
  case expr =>
    val paramValue = evaluateParameterExpression(expr)
    values += paramValue
    names += "" // unnamed expression
}
```

### OPEN CURSOR (Parse Time + Execution Time)
**File**: `AstBuilder.scala` + `OpenCursorExec.scala` (sql/catalyst + sql/core)
**Approach**: Splits Alias extraction across **parse time** (in AstBuilder) and **execution time** (in OpenCursorExec)

#### Parse Time (AstBuilder):
```scala
// In visitOpenCursorStatement()
val (args, paramNames) = Option(ctx.params).map { params =>
  params.namedExpression().asScala.toSeq.map(visitNamedExpression).map {
    case alias: Alias => (alias.child, alias.name)  // Extract name from Alias
    case expr => (expr, "")  // Empty string for positional
  }.unzip
}.getOrElse((Seq.empty, Seq.empty))
```

#### Execution Time (OpenCursorExec):
```scala
// In buildUnifiedParameters()
args.zipWithIndex.foreach { case (expr, idx) =>
  val paramValue = evaluateParameterExpression(expr)
  values += paramValue
  val paramName = if (idx < paramNames.length) paramNames(idx) else ""
  names += paramName
}
```

## Problems

1. **Code Duplication**: The same Alias-handling logic exists in two places with slightly different structures
2. **Inconsistent Phases**: EXECUTE IMMEDIATE does everything in analysis, OPEN CURSOR splits it across parse and execution
3. **Maintenance Risk**: Changes to parameter handling must be applied to both implementations
4. **Potential Divergence**: Already seeing slight differences (e.g., EXECUTE IMMEDIATE directly extracts from Alias, OPEN CURSOR indexes into pre-extracted names)

## Example of Divergence Risk

Both should handle these identically:
```sql
-- Named parameter
OPEN cur USING (x + 1 AS param1, y + 2 AS param2);
EXECUTE IMMEDIATE 'SELECT :param1' USING x + 1 AS param1;

-- Positional parameter
OPEN cur USING (x + 1, y + 2);
EXECUTE IMMEDIATE 'SELECT ?' USING x + 1;

-- Mixed (if supported in future)
OPEN cur USING (x AS param1, y + 2);
```

## Recommendations

### Option 1: Shared Utility Class (Preferred)
Extract common parameter handling into a shared utility:

```scala
// sql/core/.../command/v2/ParameterBindingUtils.scala
object ParameterBindingUtils {
  /**
   * Builds unified parameter arrays from expressions with optional Alias names.
   * Used by both EXECUTE IMMEDIATE and OPEN CURSOR.
   */
  def buildParameters(
      args: Seq[Expression],
      preExtractedNames: Seq[String] = Seq.empty
  ): (Array[Any], Array[String]) = {
    val values = scala.collection.mutable.ListBuffer[Any]()
    val names = scala.collection.mutable.ListBuffer[String]()

    args.zipWithIndex.foreach { case (expr, idx) =>
      val (paramExpr, paramName) = expr match {
        case alias: Alias =>
          (alias.child, alias.name)
        case other if idx < preExtractedNames.length =>
          (other, preExtractedNames(idx))
        case other =>
          (other, "")
      }

      values += evaluateParameterExpression(paramExpr)
      names += paramName
    }

    (values.toArray, names.toArray)
  }

  def evaluateParameterExpression(expr: Expression): Any = {
    // Shared implementation
  }
}
```

### Option 2: Move OPEN CURSOR to Analysis Time
Change OPEN CURSOR to handle Alias during analysis (like EXECUTE IMMEDIATE):
- Remove Alias extraction from AstBuilder
- Add a `ResolveOpenCursor` analyzer rule to extract Alias names
- Simplifies OpenCursorExec to match ResolveExecuteImmediate

### Option 3: Document and Accept Duplication
If the implementations must differ for architectural reasons:
- Add cross-references in comments
- Create test suite that validates both handle USING identically
- Document why they can't be unified

## Recommended Action

**Short-term**: Add TODO comment cross-referencing the duplication
**Long-term**: Refactor to use shared `ParameterBindingUtils` (Option 1)

This ensures:
- ✓ Single source of truth for parameter binding
- ✓ Consistent behavior across features
- ✓ Easier maintenance and testing
- ✓ Clear extension point for future parameter features

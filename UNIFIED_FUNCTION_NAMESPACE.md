# Unified Function Namespace Resolution

## The Problem

You're absolutely correct! From a **resolution/namespace** perspective, scalar functions and table functions **share the same namespace**, even though they're stored in separate registries for type safety.

### Current Bad Behavior:
```sql
SELECT * FROM current_user()
```
**Current Error**: `UNRESOLVABLE_TABLE_VALUED_FUNCTION` or function not found
**Problem**: This is misleading! `current_user` EXISTS as a scalar function, it's just being used in the wrong context.

**Should Error**: "current_user is a scalar function, not a table function. It cannot be used in FROM clause."

## The Solution: Unified Namespace with Context-Aware Errors

### Key Principle:
**When resolving a function name, check ALL registries (scalar + table), then validate the type matches the context.**

### Error Classes Already Exist:
From `error-conditions.json`:
- `NOT_A_SCALAR_FUNCTION` (42887): Function is table-valued, used in scalar context
- `NOT_A_TABLE_FUNCTION` (42887): Function is scalar, used in table context

## Implementation Changes

### 1. Update `FunctionResolution.scala`

Add unified lookup methods that check both registries:

```scala
/**
 * Check if a function name exists in ANY registry (scalar or table).
 * Returns the function type and registry location.
 */
def lookupFunctionInAnyRegistry(
    name: Seq[String]): Option[FunctionType] = {
  // Check builtin scalar functions
  if (maybeBuiltinFunctionName(name)) {
    val funcName = name.last
    v1SessionCatalog.lookupBuiltinFunction(FunctionIdentifier(funcName))
      .map(_ => ScalarFunctionType)
      .orElse(v1SessionCatalog.lookupBuiltinTableFunction(FunctionIdentifier(funcName))
        .map(_ => TableFunctionType))
  }
  // Check temp scalar/table functions
  else if (maybeTempFunctionName(name)) {
    val funcName = name.last
    v1SessionCatalog.lookupTempFunction(FunctionIdentifier(funcName))
      .map(_ => ScalarFunctionType)
      .orElse(v1SessionCatalog.lookupTempTableFunction(FunctionIdentifier(funcName))
        .map(_ => TableFunctionType))
  }
  // Unqualified - check both temp and builtin, both scalar and table
  else if (name.size == 1) {
    v1SessionCatalog.lookupBuiltinOrTempFunction(name.head)
      .map(_ => ScalarFunctionType)
      .orElse(v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head)
        .map(_ => TableFunctionType))
  } else {
    None
  }
}

sealed trait FunctionType
case object ScalarFunctionType extends FunctionType
case object TableFunctionType extends FunctionType
```

### 2. Update Resolution Logic

#### For Scalar Function Context (UnresolvedFunction):

```scala
def resolveBuiltinOrTempFunction(
    name: Seq[String],
    arguments: Seq[Expression],
    u: UnresolvedFunction): Option[Expression] = {

  // First check if it exists as a table function
  if (v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.lastOption.getOrElse("")).isDefined) {
    // Exists as table function, but used in scalar context
    throw QueryCompilationErrors.notAScalarFunctionError(
      name.mkString("."),
      u.origin)
  }

  // Normal scalar function resolution
  val expression = if (name.size == 1 && u.isInternal) {
    Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
  } else if (maybeBuiltinFunctionName(name)) {
    val funcName = name.last
    v1SessionCatalog.lookupBuiltinFunction(funcName, arguments)
  } else if (maybeTempFunctionName(name)) {
    val funcName = name.last
    v1SessionCatalog.lookupTempFunction(funcName, arguments)
  } else if (name.size == 1) {
    v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
  } else {
    None
  }

  expression.map { func =>
    validateFunction(func, arguments.length, u)
  }
}
```

#### For Table Function Context (UnresolvedTableValuedFunction):

```scala
def resolveBuiltinOrTempTableFunction(
    name: Seq[String],
    arguments: Seq[Expression]): Option[LogicalPlan] = {

  // First check if it exists as a scalar function
  if (v1SessionCatalog.lookupBuiltinOrTempFunction(name.lastOption.getOrElse("")).isDefined) {
    // Exists as scalar function, but used in table context
    throw QueryCompilationErrors.notATableFunctionError(
      name.mkString("."),
      origin)
  }

  // Normal table function resolution
  if (name.length == 1) {
    v1SessionCatalog.resolveBuiltinOrTempTableFunction(name.head, arguments)
  } else if (maybeBuiltinFunctionName(name)) {
    // TODO: Support qualified table functions
    None
  } else if (maybeTempFunctionName(name)) {
    // TODO: Support qualified temp table functions
    None
  } else {
    None
  }
}
```

### 3. Update SessionCatalog Helper Methods

Add helper methods to check table function registries:

```scala
def lookupBuiltinTableFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
  builtinTableFunctionRegistry.lookupFunction(name)
}

def lookupTempTableFunction(name: FunctionIdentifier): Option[ExpressionInfo] = {
  tempTableFunctionRegistry.lookupFunction(name)
}

def lookupBuiltinOrTempTableFunction(name: String): Option[ExpressionInfo] = {
  lookupTempTableFunction(FunctionIdentifier(name)).orElse {
    lookupBuiltinTableFunction(FunctionIdentifier(name))
  }
}
```

### 4. Update Error Methods in QueryCompilationErrors

```scala
def notAScalarFunctionError(
    functionName: String,
    origin: Origin): Throwable = {
  new AnalysisException(
    errorClass = "NOT_A_SCALAR_FUNCTION",
    messageParameters = Map("functionName" -> toSQLId(functionName)),
    origin = origin)
}

def notATableFunctionError(
    functionName: String,
    origin: Origin): Throwable = {
  new AnalysisException(
    errorClass = "NOT_A_TABLE_FUNCTION",
    messageParameters = Map("functionName" -> toSQLId(functionName)),
    origin = origin)
}
```

## Examples

### Example 1: Scalar Function in Table Context
```sql
SELECT * FROM current_user()
```
**Error**: `NOT_A_TABLE_FUNCTION`
**Message**: "current_user appears as a table function here, but the function was defined as a scalar function. Please update the query to move the function call outside the FROM clause."

### Example 2: Table Function in Scalar Context
```sql
SELECT explode(array(1, 2, 3))
```
**Error**: `NOT_A_SCALAR_FUNCTION`
**Message**: "explode appears as a scalar expression here, but the function was defined as a table function. Please update the query to move the function call into the FROM clause."

### Example 3: Function Actually Doesn't Exist
```sql
SELECT * FROM nonexistent_func()
```
**Error**: `UNRESOLVABLE_TABLE_VALUED_FUNCTION` or `UNRESOLVED_ROUTINE`
**Message**: "Could not resolve nonexistent_func to a table-valued function..."

## Benefits

1. **Better Error Messages**: Users know immediately if they're using the right function in the wrong place
2. **Unified Namespace**: Scalar and table functions share the same qualification namespace
3. **Prevents Name Conflicts**: Can't have both `explode` as scalar and table function
4. **SQL Standard Compliant**: SQLSTATE 42887 is the standard for "wrong function type" errors

## Implementation Order

1. ✅ Add helper methods to SessionCatalog for table function lookups
2. ✅ Add error methods to QueryCompilationErrors
3. ✅ Update resolveBuiltinOrTempFunction to check for table function conflicts
4. ✅ Update resolveBuiltinOrTempTableFunction to check for scalar function conflicts
5. ✅ Add test cases for cross-type usage
6. ✅ Update qualification support to work for both scalar and table functions

## Test Cases

```scala
test("scalar function used in table context") {
  val ex = intercept[AnalysisException] {
    sql("SELECT * FROM abs(-5)")
  }
  assert(ex.getErrorClass == "NOT_A_TABLE_FUNCTION")
}

test("table function used in scalar context") {
  val ex = intercept[AnalysisException] {
    sql("SELECT explode(array(1, 2, 3))")
  }
  assert(ex.getErrorClass == "NOT_A_SCALAR_FUNCTION")
}

test("qualified scalar function used in table context") {
  val ex = intercept[AnalysisException] {
    sql("SELECT * FROM builtin.abs(-5)")
  }
  assert(ex.getErrorClass == "NOT_A_TABLE_FUNCTION")
}

test("qualified table function used correctly") {
  // When we implement table function qualification
  sql("SELECT * FROM builtin.explode(array(1, 2, 3))")
    .collect()
}
```

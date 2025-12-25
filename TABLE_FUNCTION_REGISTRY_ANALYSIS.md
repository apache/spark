# Table Function Registry Analysis

## Why is Table Function Registry Separate?

### 1. **Different Return Types**

The fundamental reason for separate registries is **type safety**:

- **FunctionRegistry**: Returns `Expression` objects
  - Type: `FunctionBuilder = Seq[Expression] => Expression`
  - Examples: `abs()`, `upper()`, `sum()`, etc.
  - These are scalar or aggregate functions that produce values

- **TableFunctionRegistry**: Returns `LogicalPlan` objects
  - Type: `TableFunctionBuilder = Seq[Expression] => LogicalPlan`
  - Examples: `explode()`, `range()`, `json_tuple()`, `stack()`, etc.
  - These are table-valued functions that produce rows/tables

### 2. **Different Resolution Paths**

Table functions and scalar functions are resolved at different stages:

#### Scalar Functions (FunctionRegistry):
```scala
// Resolved by ResolveFunctions -> resolveBuiltinOrTempFunction
UnresolvedFunction("abs", ...)
  => Expression (e.g., Abs expression)
```

#### Table Functions (TableFunctionRegistry):
```scala
// Resolved by ResolveFunctions -> resolveBuiltinOrTempTableFunction
UnresolvedTableValuedFunction("explode", ...)
  => LogicalPlan (e.g., Generate with Explode)
```

### 3. **Different Usage Context**

**Scalar Functions** appear in:
- SELECT clauses: `SELECT abs(-5)`
- WHERE clauses: `WHERE length(name) > 5`
- GROUP BY, ORDER BY, etc.

**Table Functions** appear in:
- FROM clauses: `FROM range(1, 10)`
- LATERAL JOIN: `LATERAL VIEW explode(array_col)`
- Table-valued positions

### 4. **Built-in Table Functions**

Current built-in table functions (from lines 1230-1245):
- `range` - generates a range of numbers
- `explode` / `explode_outer` - expands arrays/maps
- `inline` / `inline_outer` - expands structs
- `json_tuple` - parses JSON
- `posexplode` / `posexplode_outer` - explode with position
- `stack` - pivots data
- `collations` - system function
- `sql_keywords` - system function
- `variant_explode` / `variant_explode_outer` - variant type support

## Could They Be Merged?

### Technical Feasibility: NO (without significant refactoring)

The registries are parameterized by different types:
```scala
trait FunctionRegistryBase[T] {
  type FunctionBuilder = Seq[Expression] => T
}

trait FunctionRegistry extends FunctionRegistryBase[Expression]
trait TableFunctionRegistry extends FunctionRegistryBase[LogicalPlan]
```

**To merge them, you would need:**

1. **Common Base Type**: Both `Expression` and `LogicalPlan` would need a common parent
   - Current: They don't share a meaningful common type for this purpose
   - Would require: A union type or existential type system

2. **Type-Safe Lookups**: The lookup methods would need to distinguish return types
   ```scala
   // Current (type-safe):
   lookupFunction(...): Option[Expression]
   lookupTableFunction(...): Option[LogicalPlan]

   // Merged (would need runtime type checking):
   lookup(...): Option[Expression | LogicalPlan]  // Not natively supported in Scala 2
   ```

3. **Separate Builder Types**: Even if merged, you'd still need:
   ```scala
   sealed trait FunctionBuilder
   case class ScalarBuilder(f: Seq[Expression] => Expression) extends FunctionBuilder
   case class TableBuilder(f: Seq[Expression] => LogicalPlan) extends FunctionBuilder
   ```

### Practical Considerations

**Against Merging:**
- **Type Safety Loss**: Would need runtime type checks and casting
- **Clear Separation of Concerns**: Different semantic domains
- **Different Resolution Rules**: Table functions have different name resolution semantics
- **Performance**: No need to search table functions when resolving scalars and vice versa
- **Code Clarity**: Separate registries make intent explicit

**For Merging:**
- **Unified Namespace**: Table and scalar functions can't have same name (conflict)
- **Simpler Qualification**: One namespace = one qualification scheme
- **Easier Management**: Single registry to maintain

## Recommendation for Qualification Support

Given the analysis above, **keep separate registries** but **extend qualification to table functions**:

### Proposed Approach:

1. **Maintain Separate Registries**:
   - `builtinFunctionRegistry: FunctionRegistry` (scalar/aggregate functions)
   - `builtinTableFunctionRegistry: TableFunctionRegistry` (table functions)
   - `tempFunctionRegistry: FunctionRegistry` (temp scalar/aggregate)
   - `tempTableFunctionRegistry: TableFunctionRegistry` (temp table functions)

2. **Extend Qualification to Table Functions**:
   ```sql
   -- Builtin table functions
   SELECT * FROM builtin.explode(array(1, 2, 3))
   SELECT * FROM system.builtin.range(1, 100)

   -- Temp table functions
   SELECT * FROM session.my_table_func(...)
   SELECT * FROM system.session.my_table_func(...)
   ```

3. **Update FunctionResolution**:
   - Add `maybeBuiltinTableFunctionName()` and `maybeTempTableFunctionName()`
   - Update `resolveBuiltinOrTempTableFunction()` to handle qualification
   - Keep separate resolution paths for type safety

### Implementation Benefits:

✅ **Type Safety**: Maintained through separate registries
✅ **Performance**: No cross-registry searches
✅ **Consistency**: Same qualification scheme for all functions
✅ **Backward Compatibility**: Unqualified names work as before
✅ **Clear Semantics**: `builtin.` and `session.` work for all function types

## Summary

**Why Separate?**
- Different return types (Expression vs LogicalPlan)
- Different resolution contexts (expression vs relation)
- Type safety and performance benefits

**Should They Merge?**
- **No** - the type system doesn't support it cleanly
- Merging would require runtime type checks and lose compile-time safety
- The separation reflects a real semantic difference in SQL

**Next Steps:**
- Apply the same two-registry architecture to table functions
- Extend qualification support to table functions
- Keep the registries separate but make them work consistently

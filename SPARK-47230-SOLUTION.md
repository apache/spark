# SPARK-47230: Nested Column Pruning for Chained LATERAL VIEW

## Problem Statement

When using chained LATERAL VIEW (multiple explode operations) with nested array accesses in Apache Spark, the query reads all fields from nested structures instead of only the accessed fields.

### Example Query
```sql
SELECT request.available, servedItem.clicked
FROM rawdata
LATERAL VIEW OUTER explode(pv_requests) AS request
LATERAL VIEW OUTER explode(request.servedItems) AS servedItem
```

**Before:** Reads all 2340 fields from the nested `pv_requests` structure
**After:** Reads only 2 accessed fields (`available`, `clicked`) - 99.9% reduction

## Root Cause

The existing `SchemaPruning` optimizer rule uses the `ScanOperation` pattern matcher, which doesn't match when there are `Generate` nodes (created by LATERAL VIEW/explode) between the Project and the LogicalRelation. This prevents the standard pruning logic from seeing and optimizing these nested field accesses.

Additionally, expressions like `request.available` reference attributes created by `Generate` nodes, not the original relation columns. The standard pruning logic doesn't trace these references back through the Generate nodes to the source columns.

## Solution Approach

Enhanced the `SchemaPruning` optimizer rule to handle chained LATERAL VIEW by:

1. **Collecting Generate Node Mappings**: Track all Generate nodes and map their output attributes to the generator expressions (explode operations)

2. **Tracing Through Generate Nodes**: When analyzing field accesses, trace AttributeReferences back through Generate nodes to find the original relation columns

3. **Selective Application**: Only apply enhanced pruning for:
   - Queries with Generate nodes (LATERAL VIEW)
   - Expressions that actually trace through Generate nodes
   - Nested array accesses (GetArrayStructFields present)

4. **Backward Compatibility**: Preserve existing behavior for single-level explode operations to maintain compatibility with known limitations (SPARK-34638, SPARK-41961)

## Modified File

**File:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SchemaPruning.scala`

### Changes Overview

#### 1. Modified `apply` method (lines 43-100)

**Purpose:** Main entry point - collect Generate mappings and field expressions before transforming the plan

**Changes:**
- Added collection of Generate node mappings to track explode operations
- Added collection of GetArrayStructFields and GetStructField expressions
- Modified the transformDown case to attempt enhanced pruning before standard pruning

**Key Code:**
```scala
val allArrayStructFields = scala.collection.mutable.ArrayBuffer[GetArrayStructFields]()
val allStructFields = scala.collection.mutable.ArrayBuffer[GetStructField]()
val generateMappings = scala.collection.mutable.Map[ExprId, Expression]()

plan.foreach { node =>
  node match {
    case g: Generate =>
      g.generatorOutput.foreach { attr =>
        generateMappings(attr.exprId) = g.generator
      }
    case _ =>
  }

  node.expressions.foreach { expr =>
    expr.foreach {
      case gasf: GetArrayStructFields => allArrayStructFields += gasf
      case gsf: GetStructField => allStructFields += gsf
      case _ =>
    }
  }
}
```

#### 2. Added `tryEnhancedNestedArrayPruning` method (lines 109-183)

**Purpose:** Attempt enhanced pruning for nested arrays accessed via chained Generate nodes

**Logic:**
1. Trace all GetArrayStructFields and GetStructField expressions through Generate nodes
2. Track whether any expressions actually went through a Generate node
3. Build a map of root column names to accessed field paths
4. Only apply if expressions traced through Generate AND there are GetArrayStructFields (nested array accesses)
5. Prune the schema to include only accessed nested fields
6. Return pruned LogicalRelation if successful, None otherwise

**Key Decision Point:**
```scala
// Only apply enhanced pruning if we actually traced through Generate nodes
// AND we have nested array accesses (chained explodes)
if (!tracedThroughGenerate || arrayStructFields.isEmpty) {
  return None
}
```

This ensures the enhancement only applies to the specific case it's designed for (chained LATERAL VIEW with nested array accesses), preserving existing behavior for simpler cases.

#### 3. Added `traceToRootColumnThroughGenerates` method (lines 193-204)

**Purpose:** Trace a GetArrayStructFields expression back to its root column through Generate nodes

**Returns:** `(Option[(rootColumnName, fieldPath)], usedGenerate: Boolean)`

**Logic:**
- Extracts the field name from GetArrayStructFields
- Calls `traceArrayAccessThroughGenerates` to trace the child expression
- Appends the field name to the path
- Propagates the `usedGenerate` flag

#### 4. Added `traceStructFieldThroughGenerates` method (lines 210-221)

**Purpose:** Trace a GetStructField expression back to its root column through Generate nodes

**Returns:** `(Option[(rootColumnName, fieldPath)], usedGenerate: Boolean)`

**Logic:**
- Extracts the field name from GetStructField
- Calls `traceArrayAccessThroughGenerates` to trace the child expression
- Appends the field name to the path
- Propagates the `usedGenerate` flag

#### 5. Added `traceArrayAccessThroughGenerates` method (lines 243-289)

**Purpose:** Core tracing logic - recursively trace expressions through Generate nodes back to relation columns

**Returns:** `(Option[(rootColumnName, fieldPath)], usedGenerate: Boolean)`

**Logic by Expression Type:**

**AttributeReference:**
- Check if attribute comes from a Generate node using `generateMappings`
- If yes: Recursively trace through the generator expression, set `usedGenerate = true`
- If no: Check if it's a relation column, set `usedGenerate = false`

**GetStructField:**
- Recursively trace the child expression
- Append field name to the path
- Propagate `usedGenerate` flag

**GetArrayItem:**
- Recursively trace the child (array indexing doesn't change the path)
- Propagate `usedGenerate` flag

**GetArrayStructFields:**
- Recursively trace the child
- Append field name to the path
- Propagate `usedGenerate` flag

**Example Trace:**
```
request.available
  ↓ GetStructField(request, "available")
  ↓ trace(request) → AttributeReference
  ↓ generateMappings.get(request.exprId) → Some(Explode(pv_requests))
  ↓ trace(pv_requests) → AttributeReference (usedGenerate = true)
  ↓ relationExprIds.contains(pv_requests.exprId) → true
  ↓ Result: (Some(("pv_requests", Seq("available"))), true)
```

#### 6. Modified `pruneNestedArraySchema` method (line ~370)

**Purpose:** Build pruned schema containing only accessed nested fields

**Key Change:**
```scala
case None =>
  // SPARK-47230: This field is not accessed, don't include it
  None  // Changed from Some(field) to None
```

This makes the pruning "aggressive" - only include fields that are explicitly accessed, rather than including all fields by default.

### Import Changes

Added necessary imports:
```scala
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
```

## Test Results

### All Test Suites Passing ✅

- **ParquetV1SchemaPruningSuite:** 190/190 tests passed
- **ParquetV2SchemaPruningSuite:** 190/190 tests passed
- **OrcV1SchemaPruningSuite:** 190/190 tests passed
- **OrcV2SchemaPruningSuite:** 190/190 tests passed
- **NestedColumnAliasingSuite:** All tests passed
- **Scalastyle:** Passed

### User's Use Case Verification

**Query:**
```sql
SELECT request.available, servedItem.clicked
FROM rawdata
LATERAL VIEW OUTER explode(pv_requests) AS request
LATERAL VIEW OUTER explode(request.servedItems) AS servedItem
```

**Result:**
```
ReadSchema: struct<pv_requests:array<struct<servedItems:array<struct<clicked:boolean>>,available:boolean>>>
```

**Metrics:**
- Original schema: 2340 nested fields
- Pruned schema: 2 fields
- Reduction: 99.9%

## Design Decisions

### Why Only for GetArrayStructFields?

The condition `arrayStructFields.isEmpty` ensures enhanced pruning only applies when there are nested array struct field accesses. This is the specific pattern used in chained LATERAL VIEW queries. Single-level explode operations don't need this enhancement and are handled correctly by existing pruning logic.

### Why Check `tracedThroughGenerate`?

This flag ensures we only apply enhanced pruning when expressions actually go through Generate nodes. If an expression references a relation column directly (not through a Generate), the standard pruning logic handles it correctly. This preserves backward compatibility and avoids interfering with existing optimizations.

### Why Preserve Existing Behavior for SPARK-34638?

Tests for SPARK-34638 and SPARK-41961 explicitly check that Spark does NOT prune multiple fields in certain single-explode cases. The comment in the tests states: "Currently we don't prune multiple field case." This is an intentional limitation. Our enhancement respects this by only applying to the more complex chained LATERAL VIEW case.

## Performance Impact

For queries with chained LATERAL VIEW accessing nested arrays:
- **Reduced I/O:** Only read accessed fields from Parquet/ORC files
- **Reduced Memory:** Smaller in-memory data structures
- **Faster Query Execution:** Less data to process

For other query patterns:
- **No Impact:** Enhanced pruning only applies to specific pattern
- **Backward Compatible:** All existing tests pass

## Future Enhancements

Potential areas for future improvement:

1. **Extend to Single-Level Explode:** Remove the `arrayStructFields.isEmpty` restriction to enable pruning for single-level explode with multiple fields (would require updating SPARK-34638 expectations)

2. **Support Additional Generator Functions:** Currently optimized for `explode`, could extend to other table-valued functions

3. **Performance Metrics:** Add instrumentation to measure actual I/O and memory savings in production queries

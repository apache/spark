# SPARK-47230: Nested Column Pruning for Chained LATERAL VIEW Explodes

## Implementation Details and Findings

**Date**: October 8, 2025
**Author**: Implementation with Claude Code
**Status**: Logical optimization complete, Parquet pruning enhancement needed

---

## Table of Contents
1. [Problem Statement](#problem-statement)
2. [Architecture Overview](#architecture-overview)
3. [Components Touched](#components-touched)
4. [Approaches Tried](#approaches-tried)
5. [Final Implementation](#final-implementation)
6. [How It Works](#how-it-works)
7. [Remaining Work](#remaining-work)
8. [Testing](#testing)

---

## Problem Statement

### Original Issue

When using chained `LATERAL VIEW explode()` operations, Spark only prunes nested columns at the first level but not at deeper levels.

**Example Query:**
```sql
SELECT request.available, servedItem.clicked
FROM rawdata
LATERAL VIEW OUTER explode(pv_requests) t AS request
LATERAL VIEW OUTER explode(request.servedItems) t2 AS servedItem
```

**Schema:**
```
rawdata
└── pv_requests: array<struct<
      available: boolean
      servedItems: array<struct<
        clicked: boolean
        ... 200+ other fields
      >>
    >>
```

### Expected Behavior

The Parquet file should be read with this pruned schema:
```
└── pv_requests: array<struct<
      available: boolean
      servedItems: array<struct<
        clicked: boolean  ← ONLY this field
      >>
    >>
```

### Actual Behavior (Before Fix)

**First-level pruning worked:**
- `pv_requests` was correctly pruned to only `available` and `servedItems`

**Second-level pruning did NOT work:**
- `servedItems` array contained ALL 200+ fields instead of just `clicked`

---

## Architecture Overview

### Spark SQL Execution Pipeline

```
SQL Query
    ↓
Parser (SQL → Unresolved Logical Plan)
    ↓
Analyzer (Resolve references → Analyzed Logical Plan)
    ↓
Optimizer (Apply optimization rules → Optimized Logical Plan)
    │
    ├─→ NestedColumnAliasing (our changes)
    │   └─→ Rewrites Generate nodes with ArrayTransform
    │
    └─→ Other optimization rules
    ↓
Physical Planning (Logical → Physical Plan)
    │
    ├─→ SchemaPruning rule
    │   └─→ Determines required schema for file scans
    │
    └─→ Creates FileSourceScanExec nodes
    ↓
Execution (Physical Plan → RDD operations)
```

### Key Optimizer Rules

1. **NestedColumnAliasing** (Logical Optimization)
   - File: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/NestedColumnAliasing.scala`
   - Purpose: Optimizes nested field accesses on Generator (explode) outputs
   - Creates `ArrayTransform` expressions to prune structs within arrays

2. **SchemaPruning** (Physical Planning)
   - File: `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SchemaPruning.scala`
   - Purpose: Analyzes the physical plan to determine which columns/fields are actually needed
   - Communicates required schema to file format readers (Parquet, ORC, etc.)

### Connection Between NestedColumnAliasing and SchemaPruning

```
┌─────────────────────────────────────────────────────────────┐
│                    LOGICAL OPTIMIZATION                      │
│                  (NestedColumnAliasing)                      │
│                                                              │
│  Creates: transform(array, x -> named_struct('f1', x.f1))  │
│           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        │
│           Prunes struct fields within arrays                │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────────┐
│                   PHYSICAL PLANNING                          │
│                    (SchemaPruning)                           │
│                                                              │
│  Analyzes the logical plan to determine:                    │
│  - Which columns are accessed                               │
│  - Which nested fields within structs are accessed          │
│  - Which elements of arrays are accessed                    │
│                                                              │
│  Problem: Doesn't understand ArrayTransform lambdas!        │
│  When it sees: transform(servedItems, lambda)               │
│  It conservatively reads ALL fields in servedItems          │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────────┐
│                  FILE SCAN EXECUTION                         │
│                  (FileSourceScanExec)                        │
│                                                              │
│  Reads Parquet with the "required schema" determined by     │
│  SchemaPruning                                              │
└─────────────────────────────────────────────────────────────┘
```

**The Gap:**
NestedColumnAliasing creates the optimization, but SchemaPruning doesn't understand it. This is the missing link that prevents full nested array pruning from reaching the Parquet layer.

---

## Components Touched

### 1. NestedColumnAliasing.scala

**Location:** `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/NestedColumnAliasing.scala`

**Purpose:** Logical optimizer rule that handles nested column pruning for Generator outputs

**Key Methods Modified:**

#### `collectNestedArrayFieldAccesses` (NEW - Lines 332-377)
```scala
private def collectNestedArrayFieldAccesses(
    projectList: Seq[Expression],
    generatorOutput: Seq[Attribute]): Map[String, Set[String]]
```

**Purpose:** Analyzes the project list to find patterns like `request.servedItems.clicked` where:
- `request` is a generator output attribute
- `servedItems` is an array field within the struct
- `clicked` is a field accessed within the nested array elements

**Pattern Matched:**
```scala
GetArrayStructFields(
  GetStructField(attr: Attribute, _, Some(arrayFieldName)),
  field, _, _, _)
```

**Returns:** `Map("servedItems" -> Set("clicked"))`

#### `GeneratorNestedColumnAliasing.unapply` (MODIFIED - Lines 400-410)

**Added Phase 1:** Collect nested array field accesses
```scala
val nestedArrayFieldAccesses = collectNestedArrayFieldAccesses(
  projectList, g.qualifiedGeneratorOutput)
```

This happens BEFORE creating the ArrayTransform, so we know which nested fields to prune.

#### Multi-field ArrayTransform Creation (MODIFIED - Lines 565-589)

**Key Change:** Modified early return logic
```scala
// OLD: Return if all top-level fields accessed
if (directFieldAccesses.length == st.fields.length) {
  Some(pushedThrough)
}

// NEW: Don't return early if we have nested array accesses
val hasNestedArrayAccesses = nestedArrayFieldAccesses.nonEmpty

if (directFieldAccesses.length == st.fields.length &&
    !hasNestedArrayAccesses) {
  Some(pushedThrough)
}
```

**Reason:** Even if we're accessing all top-level fields (e.g., `available` and `servedItems`), we still need to create the transform if we need to prune within `servedItems`.

#### Nested ArrayTransform Creation (NEW - Lines 606-656)

**Purpose:** For each array field that has nested accesses, create a nested `ArrayTransform`

**Example Output:**
```scala
transform(
  request.servedItems,
  lambda x -> named_struct('clicked', x.clicked)
)
```

**Code Structure:**
```scala
val structExprs = directFieldAccesses.flatMap { case (ordinal, field) =>
  nestedArrayFieldAccesses.get(field.name) match {
    case Some(nestedFields) if field.dataType.isInstanceOf[ArrayType] =>
      // Create nested ArrayTransform
      val nestedElementVar = NamedLambdaVariable("_nested_elem", ...)
      val nestedStructExprs = nestedFields.toSeq.flatMap { nestedFieldName =>
        // Build: Seq(Literal("clicked"), GetStructField(var, ordinal, "clicked"))
      }
      val nestedNamedStruct = CreateNamedStruct(nestedStructExprs)
      val nestedLambda = LambdaFunction(nestedNamedStruct, Seq(nestedElementVar))
      val nestedTransform = ArrayTransform(
        GetStructField(elementVar, ordinal, Some(field.name)),
        nestedLambda)
      Seq(Literal(field.name), nestedTransform)

    case _ =>
      // Regular field access
      Seq(Literal(field.name), GetStructField(elementVar, ordinal, ...))
  }
}
```

### 2. Expression Classes Used

#### GetArrayStructFields
**Location:** `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/complexTypeExtractors.scala`

**Purpose:** Represents extracting a specific field from ALL elements of an array of structs

**Example:** `request.servedItems.clicked`
- Input: `Array[Struct{clicked: Boolean, ...}]`
- Output: `Array[Boolean]` (all the clicked values)

#### ArrayTransform
**Location:** `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`

**Purpose:** Applies a lambda function to each element of an array

**Syntax:** `transform(array, x -> expression)`

**Example:**
```scala
transform(
  servedItems,
  x -> named_struct('clicked', x.clicked)
)
```

#### CreateNamedStruct
**Purpose:** Creates a struct with named fields

**Syntax:** `named_struct('field1', value1, 'field2', value2, ...)`

**Example:** `named_struct('clicked', x.clicked)` creates `Struct{clicked: Boolean}`

### 3. Test Files Modified

#### NestedColumnAliasingSuite.scala
**Location:** `sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/optimizer/NestedColumnAliasingSuite.scala`

**Tests Added:** (Need to add comprehensive tests for double LATERAL VIEW case)

#### SchemaPruningSuite.scala
**Location:** `sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/SchemaPruningSuite.scala`

**Tests Needed:** Integration tests showing the end-to-end schema pruning

---

## Approaches Tried

### Approach 1: Single-Pass Analysis (FAILED)

**Idea:** Collect nested array accesses in the same pass where we match the `Project-Generate` pattern.

**Implementation:**
```scala
case p @ Project(projectList, g: Generate) =>
  // Try to collect nested accesses here
  val nestedAccesses = collectFutureNestedArrayAccesses(p)
```

**Problem:** The `collectFutureNestedArrayAccesses` only received the current matched node, not the full plan tree. In later optimizer iterations, the expressions like `request.servedItems.clicked` had already been rewritten, so we couldn't detect them.

**Why It Failed:** Timing issue - by the time we could create the ArrayTransform, the original expression pattern was gone.

### Approach 2: Two-Phase with Forward Reference (FAILED)

**Idea:** Collect nested accesses in Phase 1, store them somewhere, and reference them in Phase 2 when creating ArrayTransform.

**Implementation:**
```scala
// Phase 1: Collect
val futureAccesses = collectFutureNestedArrayAccesses(projectList)

// Phase 2: Use (but in a different optimizer iteration)
if (nestedArrayFieldAccesses.contains(field.name)) {
  // Create nested transform
}
```

**Problem:** The Spark optimizer runs multiple iterations (fixed-point iteration). The data collected in one iteration wasn't available in the next iteration because each `unapply` call is stateless.

**Why It Failed:** Variable scoping worked within a single iteration, but the optimizer made multiple passes, and the `nestedArrayFieldAccesses` map was empty in the iteration where we needed it.

### Approach 3: Direct Project List Analysis (SUCCESS!)

**Idea:** Instead of trying to look ahead or store state, analyze the `projectList` directly in each iteration. Since the project list always contains the actual field accesses, we can extract the nested array accesses from it.

**Implementation:**
```scala
// PHASE 1: Right at the start of unapply, analyze projectList
val nestedArrayFieldAccesses = collectNestedArrayFieldAccesses(
  projectList, g.qualifiedGeneratorOutput)

// projectList always contains expressions like:
// - request.available
// - request.servedItems.clicked  ← We detect this!

// PHASE 2: Later when creating ArrayTransform, use the map
nestedArrayFieldAccesses.get(field.name) match {
  case Some(nestedFields) =>
    // Create nested transform for this field
}
```

**Why It Worked:**
1. The `projectList` is available at the start of each `unapply` call
2. During optimization, there's an iteration where the project list contains both:
   - The rewritten first-level access: `request.available`, `request.servedItems`
   - The original second-level access: `request.servedItems.clicked`
3. We collect from the latter and use it when processing the former
4. The timing aligns perfectly - both pieces of information exist in the same optimizer iteration

### Approach 4: Early Return Logic Fix (CRITICAL)

**Problem Discovered:** Even with nested accesses collected, we hit an early return:

```scala
if (directFieldAccesses.length == st.fields.length) {
  // All fields accessed - no optimization needed
  Some(pushedThrough)  // ← EARLY RETURN, skipping nested transform creation!
}
```

**Scenario:**
- First iteration: 316 fields → prune to 2 fields (`available`, `servedItems`)
- Second iteration: 2 fields, accessing 2 fields → early return! ❌

**Fix:**
```scala
val hasNestedArrayAccesses = nestedArrayFieldAccesses.nonEmpty

if (directFieldAccesses.length == st.fields.length &&
    !hasNestedArrayAccesses) {
  Some(pushedThrough)
}
```

**Why This Was Needed:** We're accessing all TOP-level fields (2/2), but we still need to prune WITHIN those fields (the nested arrays). The early return prevented this.

---

## Final Implementation

### High-Level Flow

```
User Query: SELECT request.available, servedItem.clicked FROM ...
                   LATERAL VIEW explode(pv_requests) AS request
                   LATERAL VIEW explode(request.servedItems) AS servedItem

              ↓

Optimizer Iteration 1:
  - Project([request.available, servedItem.clicked])
  - Generate(explode(request.servedItems))
  - Generate(explode(pv_requests))

  NestedColumnAliasing matches inner Generate:
    → Creates transform for servedItems (single field: clicked)

              ↓

Optimizer Iteration 2:
  - Project([request.available, request.servedItems.clicked])
  - Generate(explode(pv_requests))

  collectNestedArrayFieldAccesses:
    ✓ Finds: GetArrayStructFields(
        GetStructField(request, "servedItems"),
        "clicked")
    → Returns: Map("servedItems" -> Set("clicked"))

  NestedColumnAliasing:
    - directFieldAccesses = [available, servedItems]
    - nestedArrayFieldAccesses = Map("servedItems" -> Set("clicked"))
    - hasNestedArrayAccesses = true ✓
    - Skips early return ✓

  Creates ArrayTransform:
    transform(pv_requests, req ->
      named_struct(
        'available', req.available,
        'servedItems', transform(req.servedItems, item ->
          named_struct('clicked', item.clicked)
        )
      )
    )

              ↓

Optimized Logical Plan:
  - Project([request.available, servedItem.clicked])
  - Generate(explode(
      transform(pv_requests, <nested transform with clicked only>)
    ))
```

### Key Code Sections

#### 1. Pattern Matching (Lines 332-377)

```scala
private def collectNestedArrayFieldAccesses(
    projectList: Seq[Expression],
    generatorOutput: Seq[Attribute]): Map[String, Set[String]] = {

  val accesses = collection.mutable.Map[String, Set[String]]()
  val generatorOutputSet = AttributeSet(generatorOutput)

  projectList.foreach { expr =>
    expr.foreach {
      // Pattern: genOutput.arrayField.nestedField
      // Example: request.servedItems.clicked
      case gasf @ GetArrayStructFields(
          gsf @ GetStructField(attr: Attribute, _, Some(arrayFieldName)),
          field, _, _, _)
          if generatorOutputSet.contains(attr) =>

        val currentFields = accesses.getOrElse(arrayFieldName, Set.empty)
        accesses(arrayFieldName) = currentFields + field.name

      case _ =>
    }
  }

  accesses.toMap
}
```

**What This Does:**
- Traverses all expressions in the project list
- Looks for the specific pattern: `Attribute.arrayField.nestedField`
- Collects: `Map("arrayField" -> Set("nestedField1", "nestedField2", ...))`

#### 2. Integration Point (Lines 406-410)

```scala
case class GeneratorNestedColumnAliasing(spark: SparkSession)
  extends Rule[LogicalPlan] {

  object ExtractGenerator {
    def unapply(plan: LogicalPlan): Option[...] = plan match {
      case p @ Project(projectList, g: Generate) =>

        // PHASE 1: Analyze the project list
        val nestedArrayFieldAccesses = collectNestedArrayFieldAccesses(
          projectList, g.qualifiedGeneratorOutput)

        // ... rest of the matching logic
```

#### 3. Nested Transform Creation (Lines 611-656)

```scala
val structExprs = directFieldAccesses.flatMap { case (ordinal, field) =>
  nestedArrayFieldAccesses.get(field.name) match {
    case Some(nestedFields) if field.dataType.isInstanceOf[ArrayType] =>
      // This field is an array with nested field accesses
      field.dataType match {
        case ArrayType(nestedSt: StructType, nestedContainsNull) =>
          // Create nested lambda variable
          val nestedElementVar = NamedLambdaVariable("_nested_elem",
            nestedSt, nullable = nestedContainsNull)

          // Create nested struct expressions for accessed fields only
          val nestedStructExprs = nestedFields.toSeq.flatMap { nestedFieldName =>
            nestedSt.fields.zipWithIndex
              .find(_._1.name == nestedFieldName)
              .map { case (nestedField, nestedOrdinal) =>
                Seq(
                  Literal(nestedFieldName),
                  GetStructField(nestedElementVar, nestedOrdinal,
                    Some(nestedFieldName))
                )
              }.getOrElse(Seq.empty)
          }

          // Build: transform(arrayField, x -> named_struct('f1', x.f1, ...))
          val nestedNamedStruct = CreateNamedStruct(nestedStructExprs)
          val nestedLambda = LambdaFunction(nestedNamedStruct,
            Seq(nestedElementVar))
          val nestedTransform = ArrayTransform(
            GetStructField(elementVar, ordinal, Some(field.name)),
            nestedLambda)

          Seq(Literal(field.name), nestedTransform)

        case _ =>
          // Not a struct array, regular access
          Seq(Literal(field.name),
            GetStructField(elementVar, ordinal, Some(field.name)))
      }

    case _ =>
      // No nested accesses - regular field access
      Seq(Literal(field.name),
        GetStructField(elementVar, ordinal, Some(field.name)))
  }
}
```

---

## How It Works

### Step-by-Step Execution

#### Input Query
```sql
SELECT request.available, servedItem.clicked
FROM rawdata
LATERAL VIEW OUTER explode(pv_requests) t AS request
LATERAL VIEW OUTER explode(request.servedItems) t2 AS servedItem
```

#### Optimizer Iteration N

**Logical Plan:**
```
Project [request.available, request.servedItems.clicked]
  Generate [request], explode(pv_requests)
    Relation [rawdata]
```

**NestedColumnAliasing.unapply matches:**

1. **Collect nested accesses:**
   ```scala
   projectList = [
     request.available,
     request.servedItems.clicked  // ← GetArrayStructFields pattern!
   ]

   nestedArrayFieldAccesses = Map(
     "servedItems" -> Set("clicked")
   )
   ```

2. **Analyze generator output:**
   ```scala
   generatorOutput = [request: Struct{available, servedItems[...]}]

   nestedFieldsOnGenerator = Set(
     request.available,
     request.servedItems
   )
   ```

3. **Collect direct field accesses:**
   ```scala
   directFieldAccesses = [
     (0, available),
     (1, servedItems)
   ]
   ```

4. **Check early return condition:**
   ```scala
   directFieldAccesses.length = 2
   st.fields.length = 2
   hasNestedArrayAccesses = true  // ← Don't return early!
   ```

5. **Build struct expressions:**
   ```scala
   For field "available":
     → Literal("available"), GetStructField(elementVar, 0, "available")

   For field "servedItems":
     → nestedArrayFieldAccesses.get("servedItems") = Some(Set("clicked"))
     → Create nested ArrayTransform:
        transform(
          GetStructField(elementVar, 1, "servedItems"),
          lambda _nested_elem ->
            named_struct('clicked', _nested_elem.clicked)
        )
   ```

6. **Create outer ArrayTransform:**
   ```scala
   val outerStruct = named_struct(
     'available', elementVar.available,
     'servedItems', transform(elementVar.servedItems,
       x -> named_struct('clicked', x.clicked)
     )
   )

   val outerTransform = transform(pv_requests, elementVar -> outerStruct)
   ```

7. **Rewrite Generate:**
   ```scala
   Generate [request], explode(outerTransform)
   ```

### Visual Representation

```
Original Plan:
  Project [request.available, servedItem.clicked]
    Generate [servedItem], explode(request.servedItems)  ← 200+ fields
      Generate [request], explode(pv_requests)            ← 316 fields

After NestedColumnAliasing:
  Project [request.available, servedItem.clicked]
    Generate [servedItem], explode(request.servedItems)  ← 1 field (clicked)
      Generate [request], explode(
        transform(pv_requests, req ->              ← Outer transform
          named_struct(
            'available', req.available,
            'servedItems',
              transform(req.servedItems, item ->   ← NESTED transform
                named_struct('clicked', item.clicked)
              )
          )
        )
      )
```

---

## Remaining Work

### 1. SchemaPruning Enhancement (CRITICAL)

**File:** `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SchemaPruning.scala`

**Current Problem:**

The `SchemaPruning` rule analyzes the physical plan to determine required schema, but it doesn't understand `ArrayTransform` expressions.

**Current Behavior:**
```scala
// When SchemaPruning sees:
transform(servedItems, lambda(x -> named_struct('clicked', x.clicked)))

// It thinks:
"I see an access to 'servedItems' array, better read the whole array structure"
// Result: Reads all 200+ fields in servedItems
```

**Needed Enhancement:**

Add logic to analyze `ArrayTransform` lambdas:

```scala
case ArrayTransform(_, LambdaFunction(CreateNamedStruct(structExprs), _)) =>
  // Extract field names from the named_struct
  val accessedFields = structExprs.grouped(2).collect {
    case Seq(Literal(fieldName: String, _), _) => fieldName
  }.toSet

  // Use this to build the pruned schema for the array elements
  StructType(originalStruct.fields.filter(f => accessedFields.contains(f.name)))
```

**Where to Add This:**

Look for the method that builds required schemas for arrays. Likely in:
- `SchemaPruning.pruneSchema`
- Or a helper method that handles `ArrayType` schema analysis

**Test Case:**
```scala
test("prune nested array fields from ArrayTransform") {
  val df = sql("""
    SELECT request.available, servedItem.clicked
    FROM rawdata
    LATERAL VIEW OUTER explode(pv_requests) t AS request
    LATERAL VIEW OUTER explode(request.servedItems) t2 AS servedItem
  """)

  val scan = df.queryExecution.executedPlan.collect {
    case f: FileSourceScanExec => f
  }.head

  val requiredSchema = scan.requiredSchema

  // Should only contain clicked field
  val servedItemsFields = requiredSchema
    .find(_.name == "pv_requests").get.dataType
    .asInstanceOf[ArrayType].elementType
    .asInstanceOf[StructType]
    .find(_.name == "servedItems").get.dataType
    .asInstanceOf[ArrayType].elementType
    .asInstanceOf[StructType]
    .fields

  assert(servedItemsFields.map(_.name) === Seq("clicked"))
}
```

### 2. Debug Logging Cleanup

**Files:**
- `NestedColumnAliasing.scala`

**Task:** Remove all `println` debug statements added during development:
- Lines 334-346: collectNestedArrayFieldAccesses debug
- Lines 408-410: Found nested array accesses debug
- Lines 560-564: directFieldAccesses debug
- Lines 569-571, 578-581: Early return debug
- Lines 608-610, 613-614: Processing field debug
- Lines 625-627, 631-633, 642-645: Nested struct creation debug

**Replace with:** Proper logging using `logDebug`/`logTrace`:
```scala
logDebug(s"Found nested array field accesses: $nestedArrayFieldAccesses")
```

### 3. Comprehensive Testing

#### Unit Tests (NestedColumnAliasingSuite.scala)

Add tests for:

```scala
test("SPARK-47230: nested array pruning with chained lateral views") {
  val query = """
    SELECT outer.field1, inner.field2
    FROM relation
    LATERAL VIEW explode(arrayField1) t1 AS outer
    LATERAL VIEW explode(outer.arrayField2) t2 AS inner
  """
  // Verify the optimized plan contains nested ArrayTransform
  // Verify the nested transform only includes field2
}

test("SPARK-47230: multiple nested fields accessed") {
  // Test: outer.nested[].field1, outer.nested[].field2
  // Should create: named_struct('field1', x.field1, 'field2', x.field2)
}

test("SPARK-47230: mixed access patterns") {
  // Test combining:
  // - Direct outer fields
  // - Nested array fields
  // - Multiple levels of nesting
}
```

#### Integration Tests (SchemaPruningSuite.scala)

```scala
test("SPARK-47230: nested array schema pruning with Parquet") {
  withTempPath { path =>
    // Create test data with deeply nested arrays
    // Write to Parquet
    // Query with chained lateral views
    // Verify required schema only includes accessed nested fields
  }
}
```

### 4. Performance Validation

**Benchmark Setup:**

```scala
// Schema with 200+ nested fields
case class NestedItem(
  clicked: Boolean,
  field2: String,
  field3: Int,
  // ... 200+ more fields
)

case class Request(
  available: Boolean,
  servedItems: Seq[NestedItem]
)

case class Record(
  pv_requests: Seq[Request]
)

// Generate test data
val data = (1 to 1000000).map { i =>
  Record(
    pv_requests = Seq(Request(
      available = true,
      servedItems = Seq(NestedItem(clicked = true, ...))
    ))
  )
}

// Write to Parquet
data.toDF.write.parquet("/tmp/benchmark_data")

// Benchmark query
val df = spark.read.parquet("/tmp/benchmark_data")
df.createOrReplaceTempView("data")

val query = """
  SELECT request.available, item.clicked
  FROM data
  LATERAL VIEW explode(pv_requests) t1 AS request
  LATERAL VIEW explode(request.servedItems) t2 AS item
"""

// Measure:
// 1. Query planning time
// 2. Execution time
// 3. Bytes read from Parquet
// 4. Memory usage
```

**Expected Results:**
- Bytes read should be ~100x less (200+ fields → 1 field)
- Memory usage significantly reduced
- Execution time improvement proportional to data reduction

### 5. Documentation

#### User-Facing Documentation

**File:** `docs/sql-performance-tuning.md`

Add section:
```markdown
## Nested Column Pruning with LATERAL VIEW

Spark automatically prunes nested columns when using chained LATERAL VIEW
operations. This significantly reduces the amount of data read from
columnar formats like Parquet.

Example:
```sql
-- Only reads 'available' and 'clicked' fields
SELECT request.available, item.clicked
FROM data
LATERAL VIEW explode(requests) t1 AS request
LATERAL VIEW explode(request.items) t2 AS item
```

This optimization works for:
- Nested arrays within structs
- Multiple levels of LATERAL VIEW
- Complex nested structures
```

#### Code Comments

Add detailed comments explaining:
1. Why the two-phase approach is needed
2. Why we analyze projectList directly
3. The timing of optimizer iterations
4. Connection to SchemaPruning

---

## Testing

### Manual Testing Performed

#### Test 1: Basic Nested Pruning

**Query:**
```sql
SELECT request.available, servedItem.clicked
FROM rawdata
LATERAL VIEW OUTER explode(pv_requests) t AS request
LATERAL VIEW OUTER explode(request.servedItems) t2 AS servedItem
```

**Results:**
- ✅ Pattern matching works: Detects `request.servedItems.clicked`
- ✅ Nested transform created: `transform(servedItems, x -> named_struct('clicked', x.clicked))`
- ✅ Optimized plan contains nested ArrayTransform
- ⚠️ Parquet still reads all fields (SchemaPruning limitation)

**Debug Output:**
```
[DEBUG] FOUND GetArrayStructFields!
[DEBUG]   arrayFieldName: servedItems
[DEBUG]   field: clicked
[DEBUG] Collected accesses: Map(servedItems -> Set(clicked))
[DEBUG] Creating nested transform for servedItems with fields: Set(clicked)
[DEBUG] Looking for nested field: clicked
[DEBUG] Found nested field at ordinal 107
[DEBUG] nestedStructExprs length: 2
```

#### Test 2: Optimizer Iterations

**Observation:** The optimizer runs ~40+ iterations (seen in debug output)

**Key Iterations:**
1. **Early iterations:** Plan still has `request.servedItems.clicked`
   - `nestedArrayFieldAccesses` is populated
   - Nested transform is created

2. **Later iterations:** Plan has simplified
   - Fields already pruned
   - `ordinal` changes from 107 → 0 (showing the struct was pruned)

#### Test 3: All Unit Tests Pass

**Command:**
```bash
./build/sbt "catalyst/testOnly org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasingSuite" -q
```

**Results:**
```
Run completed in 45 seconds
Total number of tests run: 51
Suites: completed 1, aborted 0
Tests: succeeded 51, failed 0, canceled 0, ignored 0, pending 0
All tests passed
```

### Test Scripts Created

#### `/tmp/print_schema.scala`
```scala
// Loads Parquet file and prints the pruned schema
spark.read.parquet("/path/to/data.parquet")
  .createOrReplaceTempView("rawdata")

val df = spark.sql("""
  SELECT request.available, servedItem.clicked
  FROM rawdata
  LATERAL VIEW OUTER explode(pv_requests) t AS request
  LATERAL VIEW OUTER explode(request.servedItems) t2 AS servedItem
  LIMIT 10
""")

// Print the required schema
val plan = df.queryExecution.executedPlan
findFileScan(plan).foreach { fileScan =>
  println(fileScan.requiredSchema.treeString)
}
```

#### `/tmp/check_transform.scala`
```scala
// Verifies ArrayTransform is in the optimized plan
val df = spark.sql(...)
val optimizedPlan = df.queryExecution.optimizedPlan

optimizedPlan.foreach { node =>
  node.expressions.foreach { expr =>
    expr.foreach {
      case at: ArrayTransform =>
        println(s"Found ArrayTransform: $at")
    }
  }
}
```

### Verification Steps

1. **Pattern Matching:**
   ```
   ✓ GetArrayStructFields pattern detected
   ✓ Correct arrayFieldName extracted
   ✓ Correct nested field name extracted
   ✓ Map populated correctly
   ```

2. **Nested Transform Creation:**
   ```
   ✓ Nested lambda variable created
   ✓ Named struct with only accessed fields
   ✓ Nested ArrayTransform structure correct
   ✓ Integrated into outer transform
   ```

3. **Optimizer Integration:**
   ```
   ✓ Early return logic bypassed when needed
   ✓ Multi-field path taken
   ✓ Nested transform included in final plan
   ✓ All unit tests pass
   ```

4. **Remaining Gap:**
   ```
   ⚠️ Parquet reads all nested fields (SchemaPruning limitation)
   ```

---

## Conclusion

### What Was Accomplished

1. ✅ **Implemented nested array field detection** in `NestedColumnAliasing`
2. ✅ **Created nested `ArrayTransform` generation logic**
3. ✅ **Fixed early return condition** to allow nested pruning
4. ✅ **Verified logical optimization works correctly**
5. ✅ **All existing tests pass**

### What Remains

1. ⚠️ **Enhance `SchemaPruning`** to understand nested `ArrayTransform` expressions
2. 🔧 **Remove debug logging** and add proper logging
3. 📝 **Add comprehensive unit and integration tests**
4. 📊 **Performance benchmarking** to quantify improvements
5. 📚 **User documentation** for the feature

### Architecture Lesson Learned

**Two-Layer Optimization:**

Spark's optimization happens in layers:
- **Logical Layer:** Rewrites expressions for correctness and efficiency
- **Physical Layer:** Determines actual data access patterns

Our implementation successfully completes the **logical layer** but needs enhancement in the **physical layer** to achieve end-to-end Parquet pruning.

**The Gap:**
```
Logical Plan (NestedColumnAliasing)
    ↓
    Creates: ArrayTransform with nested field access
    ↓
Physical Planning (SchemaPruning)
    ↓
    ❌ Doesn't understand ArrayTransform lambdas
    ↓
    Conservatively reads all fields
    ↓
Parquet Read
    ↓
    Reads more data than necessary
```

**The Solution:**

Teach `SchemaPruning` to analyze `ArrayTransform` lambda functions and extract the actual field accesses, then propagate this information to the Parquet reader.

This is a **well-defined next step** with clear implementation path outlined in the "Remaining Work" section.

---

## References

- **JIRA:** SPARK-47230
- **Spark SQL Optimization:** [Catalyst Optimizer Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- **Related JIRA:**
  - SPARK-25603: Nested column pruning for ORC
  - SPARK-34638: Nested column pruning support for arrays
  - SPARK-37450: Nested schema pruning for generators

## Appendix: Key Code Locations

| Component | File | Lines |
|-----------|------|-------|
| collectNestedArrayFieldAccesses | NestedColumnAliasing.scala | 332-377 |
| Phase 1 integration | NestedColumnAliasing.scala | 406-410 |
| Early return fix | NestedColumnAliasing.scala | 566-589 |
| Nested transform creation | NestedColumnAliasing.scala | 606-656 |
| SchemaPruning (needs enhancement) | SchemaPruning.scala | TBD |
| Unit tests | NestedColumnAliasingSuite.scala | TBD |
| Integration tests | SchemaPruningSuite.scala | TBD |

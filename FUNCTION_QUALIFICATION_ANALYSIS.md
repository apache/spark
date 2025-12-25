# Function Qualification Analysis

## Current State

### 1. Builtin Function Registry

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala`

The builtin function registry works as follows:

- All builtin functions are registered in `FunctionRegistry.builtin` (a `SimpleFunctionRegistry` instance)
- The registry stores functions in a `HashMap[FunctionIdentifier, (ExpressionInfo, FunctionBuilder)]`
- Functions are registered with only a function name (no database/namespace qualifier)
- Example registration: `FunctionIdentifier("abs")` for the `abs()` function
- The builtin registry is immutable and shared across all sessions

**Key code**:
```scala
val builtin: SimpleFunctionRegistry = {
  val fr = new SimpleFunctionRegistry
  expressions.foreach {
    case (name, (info, builder)) =>
      fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
  }
  fr
}
```

### 2. Session Temporary Functions

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala`

Session temporary functions work as follows:

- Each session has its own `functionRegistry` (cloned from `FunctionRegistry.builtin`)
- When a temporary function is created via `CREATE TEMPORARY FUNCTION`, it's registered with:
  - `FunctionIdentifier(name)` - only the function name, NO database qualifier
  - The function is added to the session's `functionRegistry`
- **CRITICAL**: Temporary functions CAN shadow builtin functions in the session registry
  - Since the session registry is a clone, registering a temp function with same name OVERWRITES the builtin
  - The `HashMap.put()` replaces the builtin entry

**Key methods**:
- `registerFunction()` - adds functions to the session registry (can overwrite builtins!)
- `isTemporaryFunction()` - checks if a function is temporary: `name.database.isEmpty && isRegisteredFunction(name) && !isBuiltinFunction(name)`
  - Note: `isBuiltinFunction()` checks the ORIGINAL `FunctionRegistry.builtin`, not the session registry
  - This is how the system identifies if a registered function is temp (shadows builtin) vs builtin
- `dropTempFunction()` - removes temporary functions

**Shadowing behavior**:
```scala
// isTemporaryFunction checks:
def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
  name.database.isEmpty &&
  isRegisteredFunction(name) &&  // exists in session registry
  !isBuiltinFunction(name)       // NOT in original builtin registry
}

// isBuiltinFunction checks ORIGINAL builtin, NOT session registry
def isBuiltinFunction(name: FunctionIdentifier): Boolean = {
  FunctionRegistry.builtin.functionExists(name) ||
    TableFunctionRegistry.builtin.functionExists(name)
}
```

### 3. Function Resolution Process

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala`

Current resolution flow for `UnresolvedFunction`:

1. **First**: Try `resolveBuiltinOrTempFunction()` - checks if `name.size == 1`
   - If single name, lookup in session registry (which includes both builtin and temp functions)
   - If found, return the function

2. **Second**: If not found as builtin/temp, expand identifier and resolve as persistent function
   - Uses `relationResolution.expandIdentifier()` to determine catalog
   - For v1 session catalog: calls `resolvePersistentFunction()`
   - For v2 catalog: calls `resolveV2Function()`

**Key limitation**: Currently only unqualified names (single part) are checked for builtin/temp functions!

```scala
def resolveBuiltinOrTempFunction(
    name: Seq[String],
    arguments: Seq[Expression],
    u: UnresolvedFunction): Option[Expression] = {
  val expression = if (name.size == 1 && u.isInternal) {
    // Internal functions
    Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
  } else if (name.size == 1) {
    // Builtin and temp functions - ONLY IF UNQUALIFIED!
    v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
  } else {
    None  // Qualified names skip builtin/temp lookup!
  }
  // ... validation
}
```

### 4. Session Variables Pattern (Reference Implementation)

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/VariableResolution.scala`

Session variables already support the qualification pattern we need:

- Variables can be referenced as:
  - `var1` (unqualified)
  - `session.var1` (schema-qualified)
  - `system.session.var1` (fully-qualified)

- Constants defined in `CatalogManager`:
  - `SYSTEM_CATALOG_NAME = "system"`
  - `SESSION_NAMESPACE = "session"`

- The `maybeTempVariableName()` method checks valid qualifications:

```scala
private def maybeTempVariableName(nameParts: Seq[String]): Boolean = {
  nameParts.length == 1 || {
    if (nameParts.length == 2) {
      nameParts.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
    } else if (nameParts.length == 3) {
      nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
      nameParts(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
    } else {
      false
    }
  }
}
```

### 5. FakeSystemCatalog

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/v2ResolutionPlans.scala`

A fake catalog object already exists for system-level objects:

```scala
object FakeSystemCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "system"
}
```

This is currently used for temp views and variables.

---

## Proposed Design

### Goal
Support qualified names for:
- **Builtin functions**: `builtin.abs()` or `system.builtin.abs()`
- **Temporary functions**: `session.my_func()` or `system.session.my_func()`

### Design Approach: Two Separate Registries

**Key Architectural Change**: Stop cloning the builtin registry. Instead, maintain two separate registries:
1. **Builtin Registry**: Global, immutable, shared across all sessions (`FunctionRegistry.builtin`)
2. **Temp Registry**: Per-session, mutable, starts empty (`session.tempFunctionRegistry`)

### Design Principles
1. Follow the same pattern as session variables
2. Maintain backward compatibility - unqualified names continue to work
3. Case-insensitive qualification (like variables)
4. **Memory efficient**: Don't clone 400+ builtin functions per session
5. **Clear separation**: Builtins and temps never mix in storage
6. **Solve the shadowing problem**: Allow explicit access to builtin functions even when shadowed
7. **Foundation for PATH**: Natural separation aligns with future PATH-based resolution

### How This Design Solves the Shadowing Problem

**Current Problem**:
```sql
-- Create a temp function with same name as builtin
CREATE TEMPORARY FUNCTION abs AS 'com.example.MyCustomAbs';

-- Now there's NO WAY to call the builtin abs() function!
SELECT abs(-5);  -- Calls the temp function, builtin is inaccessible
```

Currently, temp functions overwrite builtins in the session's cloned registry, making the original builtin inaccessible.

**Our Solution with Separate Registries**:
```sql
CREATE TEMPORARY FUNCTION abs AS 'com.example.MyCustomAbs';

-- Unqualified: checks temp first, then builtin (temp shadows builtin)
SELECT abs(-5);                    -- Calls temp function

-- Qualified: explicitly access builtin (no shadowing!)
SELECT builtin.abs(-5);            -- Calls builtin function
SELECT system.builtin.abs(-5);    -- Calls builtin function

-- Qualified: explicitly access temp
SELECT session.abs(-5);            -- Calls temp function
SELECT system.session.abs(-5);    -- Calls temp function
```

**Key Implementation Detail with Separate Registries**:

```scala
// Storage: Two completely separate registries
FunctionRegistry.builtin              // { "abs" -> BuiltinAbsBuilder }
session.tempFunctionRegistry          // { "abs" -> CustomAbsBuilder }
// Both can coexist! No overwriting!

// Resolution logic checks qualification:
def resolveBuiltinOrTempFunction(
    name: Seq[String],
    arguments: Seq[Expression],
    u: UnresolvedFunction): Option[Expression] = {

  // 1. If explicitly qualified as builtin
  if (maybeBuiltinFunctionName(name)) {
    val funcName = name.last
    // Look ONLY in builtin registry
    FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(funcName), arguments)
  }

  // 2. If explicitly qualified as session temp
  else if (maybeTempFunctionName(name)) {
    val funcName = name.last
    // Look ONLY in temp registry
    v1SessionCatalog.lookupTempFunction(funcName, arguments)
  }

  // 3. Unqualified: natural shadowing
  else if (name.size == 1) {
    val funcName = name.head
    // Try temp first (shadowing)
    v1SessionCatalog.lookupTempFunction(funcName, arguments)
      .orElse {
        // Fall back to builtin
        FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(funcName), arguments)
      }
  }

  else None
}
```

**Benefits of Separate Registries**:
1. **No overwriting**: Both functions coexist in separate registries
2. **Natural shadowing**: Unqualified lookup checks temp first, then builtin
3. **Direct access**: Qualification determines which registry to check
4. **Memory efficient**: No cloning of 400+ builtins per session

### Architecture Changes

#### 1. SessionCatalog: Add Separate Temp Registries

**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala`

Add new fields for temp-only registries:

```scala
class SessionCatalog(
    externalCatalogBuilder: () => ExternalCatalog,
    globalTempViewManagerBuilder: () => GlobalTempViewManager,
    functionRegistryBuilder: () => FunctionRegistry,           // REMOVE THIS
    tableFunctionRegistryBuilder: () => TableFunctionRegistry,  // REMOVE THIS
    hadoopConf: Configuration,
    parser: ParserInterface,
    resourceLoader: SessionResourceLoader,
    functionExpressionBuilder: FunctionExpressionBuilder) extends SQLConfHelper with Logging {

  // NEW: Separate registries for temporary functions (start empty)
  private val tempFunctionRegistry: FunctionRegistry = new SimpleFunctionRegistry
  private val tempTableFunctionRegistry: TableFunctionRegistry = new SimpleTableFunctionRegistry

  // Keep references to immutable builtin registries (no cloning!)
  private val builtinFunctionRegistry: FunctionRegistry = FunctionRegistry.builtin
  private val builtinTableFunctionRegistry: TableFunctionRegistry = TableFunctionRegistry.builtin

  // ... rest of class
}
```

#### 2. BaseSessionStateBuilder: Remove Cloning

**File**: `sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala`

Remove the function registry builders (no longer needed):

```scala
// REMOVE these:
protected lazy val functionRegistry: FunctionRegistry = {
  parentState.map(_.functionRegistry.clone())
    .getOrElse(extensions.registerFunctions(FunctionRegistry.builtin.clone()))
}

protected lazy val tableFunctionRegistry: TableFunctionRegistry = {
  parentState.map(_.tableFunctionRegistry.clone())
    .getOrElse(extensions.registerTableFunctions(TableFunctionRegistry.builtin.clone()))
}

// SessionCatalog constructor changes:
protected lazy val catalog: SessionCatalog = {
  val catalog = new SessionCatalog(
    () => session.sharedState.externalCatalog,
    () => session.sharedState.globalTempViewManager,
    // REMOVE: functionRegistry,
    // REMOVE: tableFunctionRegistry,
    SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
    sqlParser,
    resourceLoader,
    new SparkUDFExpressionBuilder)
  parentState.foreach(_.catalog.copyStateTo(catalog))
  catalog
}
```

#### 3. SessionCatalog: Update Function Registration

Update `registerFunction` to route to the correct registry:

```scala
def registerFunction(
    name: FunctionIdentifier,
    info: ExpressionInfo,
    builder: FunctionBuilder): Unit = synchronized {

  if (name.database.isEmpty) {
    // Temporary function - register in temp registry only
    tempFunctionRegistry.registerFunction(name, info, builder)
  } else {
    // Persistent function - register in external catalog
    val db = formatDatabaseName(name.database.get)
    requireDbExists(db)
    val qualified = name.copy(database = Some(db))
    // ... external catalog registration logic
  }
}

// Similar for table functions
def registerTableFunction(
    name: FunctionIdentifier,
    info: ExpressionInfo,
    builder: TableFunctionBuilder): Unit = synchronized {

  if (name.database.isEmpty) {
    tempTableFunctionRegistry.registerFunction(name, info, builder)
  } else {
    // Persistent table function registration
    // ...
  }
}
```

#### 4. SessionCatalog: Add Lookup Methods

Add new methods to lookup in specific registries:

```scala
// Lookup only in temp registry
def lookupTempFunction(name: String, arguments: Seq[Expression]): Option[Expression] = synchronized {
  val funcIdent = FunctionIdentifier(name)
  if (tempFunctionRegistry.functionExists(funcIdent)) {
    Some(tempFunctionRegistry.lookupFunction(funcIdent, arguments))
  } else {
    None
  }
}

// Lookup only in builtin registry
def lookupBuiltinFunction(name: String, arguments: Seq[Expression]): Option[Expression] = {
  val funcIdent = FunctionIdentifier(name)
  if (builtinFunctionRegistry.functionExists(funcIdent)) {
    Some(builtinFunctionRegistry.lookupFunction(funcIdent, arguments))
  } else {
    None
  }
}

// Lookup with fallback (temp shadows builtin)
def resolveBuiltinOrTempFunction(name: String, arguments: Seq[Expression]): Option[Expression] = {
  // Try temp first
  lookupTempFunction(name, arguments)
    .orElse {
      // Fall back to builtin
      lookupBuiltinFunction(name, arguments)
    }
}

// Similar methods for table functions
def lookupTempTableFunction(name: String, arguments: Seq[Expression]): Option[LogicalPlan] = synchronized {
  val funcIdent = FunctionIdentifier(name)
  if (tempTableFunctionRegistry.functionExists(funcIdent)) {
    Some(tempTableFunctionRegistry.lookupFunction(funcIdent, arguments))
  } else {
    None
  }
}

def lookupBuiltinTableFunction(name: String, arguments: Seq[Expression]): Option[LogicalPlan] = {
  val funcIdent = FunctionIdentifier(name)
  if (builtinTableFunctionRegistry.functionExists(funcIdent)) {
    Some(builtinTableFunctionRegistry.lookupFunction(funcIdent, arguments))
  } else {
    None
  }
}

def resolveBuiltinOrTempTableFunction(
    name: String, arguments: Seq[Expression]): Option[LogicalPlan] = {
  lookupTempTableFunction(name, arguments)
    .orElse(lookupBuiltinTableFunction(name, arguments))
}
```

#### 5. SessionCatalog: Update Utility Methods

Update methods that check function properties:

```scala
// Now checks only temp registry
def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
  name.database.isEmpty && tempFunctionRegistry.functionExists(name)
}

// Checks immutable builtin registry
def isBuiltinFunction(name: FunctionIdentifier): Boolean = {
  builtinFunctionRegistry.functionExists(name) ||
    builtinTableFunctionRegistry.functionExists(name)
}

// Checks if registered anywhere
def isRegisteredFunction(name: FunctionIdentifier): Boolean = {
  tempFunctionRegistry.functionExists(name) ||
    tempTableFunctionRegistry.functionExists(name) ||
    builtinFunctionRegistry.functionExists(name) ||
    builtinTableFunctionRegistry.functionExists(name)
}

// Drop from temp registry
def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
  if (!tempFunctionRegistry.dropFunction(FunctionIdentifier(name)) &&
      !tempTableFunctionRegistry.dropFunction(FunctionIdentifier(name)) &&
      !ignoreIfNotExists) {
    throw new NoSuchTempFunctionException(name)
  }
}

// List only temp functions
def listTemporaryFunctions(): Seq[FunctionIdentifier] = {
  tempFunctionRegistry.listFunction() ++ tempTableFunctionRegistry.listFunction()
}

// Update reset() method
def reset(): Unit = synchronized {
  // ... existing reset logic ...

  // Clear temp registries (no need to restore builtins!)
  tempFunctionRegistry.clear()
  tempTableFunctionRegistry.clear()

  // No need to restore builtins - they're in separate immutable registries!
}
```

#### 6. Add Constants to CatalogManager

Add a constant for builtin namespace alongside SESSION_NAMESPACE:

```scala
object CatalogManager {
  val SESSION_CATALOG_NAME = "spark_catalog"
  val SYSTEM_CATALOG_NAME = "system"
  val SESSION_NAMESPACE = "session"
  val BUILTIN_NAMESPACE = "builtin"  // NEW
}
```

#### 7. Modify Function Resolution (FunctionResolution.scala)

Update `resolveBuiltinOrTempFunction()` to handle qualified names with separate registries:

```scala
def resolveBuiltinOrTempFunction(
    name: Seq[String],
    arguments: Seq[Expression],
    u: UnresolvedFunction): Option[Expression] = {

  // Handle internal functions (unchanged)
  if (name.size == 1 && u.isInternal) {
    return Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
      .map(func => validateFunction(func, arguments.length, u))
  }

  // Check if this is a builtin or temp function reference
  val expression = if (maybeBuiltinFunctionName(name)) {
    // Explicitly qualified as builtin - look in builtin registry only
    val funcName = name.last
    v1SessionCatalog.lookupBuiltinFunction(funcName, arguments).orNull

  } else if (maybeTempFunctionName(name)) {
    // Explicitly qualified as temp - look in temp registry only
    val funcName = name.last
    v1SessionCatalog.lookupTempFunction(funcName, arguments).orNull

  } else if (name.size == 1) {
    // Unqualified name - check temp first (shadowing), then builtin
    val funcName = name.head
    v1SessionCatalog.resolveBuiltinOrTempFunction(funcName, arguments).orNull

  } else {
    null
  }

  Option(expression).map { func =>
    validateFunction(func, arguments.length, u)
  }
}

// Helper method to check if name could be a builtin function
private def maybeBuiltinFunctionName(nameParts: Seq[String]): Boolean = {
  nameParts.length match {
    case 2 => nameParts.head.equalsIgnoreCase(CatalogManager.BUILTIN_NAMESPACE)
    case 3 =>
      nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
      nameParts(1).equalsIgnoreCase(CatalogManager.BUILTIN_NAMESPACE)
    case _ => false
  }
}

// Helper method to check if name could be a temp function
private def maybeTempFunctionName(nameParts: Seq[String]): Boolean = {
  nameParts.length match {
    case 2 => nameParts.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
    case 3 =>
      nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
      nameParts(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)
    case _ => false
  }
}
```

#### 8. Update lookupBuiltinOrTempFunction (for metadata lookup)

```scala
def lookupBuiltinOrTempFunction(
    name: Seq[String],
    u: Option[UnresolvedFunction]): Option[ExpressionInfo] = {
  if (name.size == 1 && u.exists(_.isInternal)) {
    FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head))
  } else if (maybeBuiltinFunctionName(name)) {
    v1SessionCatalog.lookupBuiltinFunctionInfo(name.last)
  } else if (maybeTempFunctionName(name)) {
    v1SessionCatalog.lookupTempFunctionInfo(name.last)
  } else if (name.size == 1) {
    v1SessionCatalog.lookupBuiltinOrTempFunctionInfo(name.head)
  } else {
    None
  }
}
```

Add corresponding methods to SessionCatalog:

```scala
// In SessionCatalog:
def lookupTempFunctionInfo(name: String): Option[ExpressionInfo] = synchronized {
  tempFunctionRegistry.lookupFunction(FunctionIdentifier(name))
}

def lookupBuiltinFunctionInfo(name: String): Option[ExpressionInfo] = {
  builtinFunctionRegistry.lookupFunction(FunctionIdentifier(name))
}

def lookupBuiltinOrTempFunctionInfo(name: String): Option[ExpressionInfo] = {
  lookupTempFunctionInfo(name).orElse(lookupBuiltinFunctionInfo(name))
}
```

#### 9. Table Function Support

Apply the same pattern to table functions in `resolveBuiltinOrTempTableFunction()`:

```scala
def resolveBuiltinOrTempTableFunction(
    name: Seq[String],
    arguments: Seq[Expression]): Option[LogicalPlan] = {
  if (maybeBuiltinFunctionName(name)) {
    v1SessionCatalog.lookupBuiltinTableFunction(name.last, arguments)
  } else if (maybeTempFunctionName(name)) {
    v1SessionCatalog.lookupTempTableFunction(name.last, arguments)
  } else if (name.length == 1) {
    v1SessionCatalog.resolveBuiltinOrTempTableFunction(name.head, arguments)
  } else {
    None
  }
}

def lookupBuiltinOrTempTableFunction(name: Seq[String]): Option[ExpressionInfo] = {
  if (maybeBuiltinFunctionName(name)) {
    v1SessionCatalog.lookupBuiltinTableFunctionInfo(name.last)
  } else if (maybeTempFunctionName(name)) {
    v1SessionCatalog.lookupTempTableFunctionInfo(name.last)
  } else if (name.length == 1) {
    v1SessionCatalog.lookupBuiltinOrTempTableFunctionInfo(name.head)
  } else {
    None
  }
}
```

#### 10. Error Messages

Update error messages to mention valid qualifications. In `SimpleFunctionRegistryBase.lookupFunction()`:

```scala
override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): T = {
  val func = synchronized {
    functionBuilders.get(normalizeFuncName(name)).map(_._2).getOrElse {
      throw QueryCompilationErrors.unresolvedRoutineError(
        name,
        Seq("system.builtin", "builtin")  // Show valid qualifications
      )
    }
  }
  func(children)
}
```

---

## Testing Strategy

### Test Cases

1. **Builtin Function Qualification**
   ```sql
   -- All should work
   SELECT abs(-5);
   SELECT builtin.abs(-5);
   SELECT system.builtin.abs(-5);

   -- Case insensitivity
   SELECT BUILTIN.ABS(-5);
   SELECT System.Builtin.Abs(-5);
   ```

2. **Temporary Function Qualification**
   ```sql
   CREATE TEMPORARY FUNCTION my_func AS 'com.example.MyFunc';

   -- All should work
   SELECT my_func(1);
   SELECT session.my_func(1);
   SELECT system.session.my_func(1);

   -- Case insensitivity
   SELECT SESSION.my_func(1);
   SELECT System.Session.my_func(1);
   ```

3. **Name Conflict Resolution (Shadowing)**
   ```sql
   -- Scenario 1: Temp function shadows builtin
   CREATE TEMPORARY FUNCTION abs AS 'com.example.MyAbs';

   SELECT abs(-5);                -- Uses temp function (shadowing)
   SELECT builtin.abs(-5);        -- Uses builtin (bypasses shadow)
   SELECT session.abs(-5);        -- Uses temp function

   -- Verify both work correctly
   SELECT typeof(abs(-5)), typeof(builtin.abs(-5));

   -- Drop temp, builtin still accessible
   DROP TEMPORARY FUNCTION abs;
   SELECT abs(-5);                -- Now uses builtin again

   -- Scenario 2: Error when trying to create temp with builtin qualifier
   CREATE TEMPORARY FUNCTION builtin.my_func AS 'com.example.Func';  -- Should error

   -- Scenario 3: Persistent function vs builtin
   CREATE FUNCTION mydb.abs AS 'com.example.PersistentAbs';
   SELECT abs(-5);                -- Uses builtin (unqualified)
   SELECT mydb.abs(-5);           -- Uses persistent
   SELECT builtin.abs(-5);        -- Uses builtin
   ```

4. **Table Functions**
   ```sql
   SELECT * FROM range(10);
   SELECT * FROM builtin.range(10);
   SELECT * FROM system.builtin.range(10);
   ```

5. **Error Cases**
   ```sql
   -- Invalid qualifications
   SELECT wrong.abs(-5);          -- Error
   SELECT system.wrong.abs(-5);   -- Error
   SELECT session.builtin.abs(-5); -- Error
   ```

### SQL Test File Pattern

Create `sql-session-functions.sql` following the pattern of `sql-session-variables.sql`:

```sql
SET spark.sql.ansi.enabled = true;

-- Test builtin function qualifiers
SELECT 'Test builtin qualifiers - success' AS title;
SELECT abs(-5) as Unqualified,
       builtin.abs(-5) AS SchemaQualified,
       system.builtin.abs(-5) AS fullyQualified;

-- Test case insensitivity
SELECT BUILTIN.ABS(-5), System.Builtin.ABS(-5);

-- Test temp function qualifiers
CREATE TEMPORARY FUNCTION temp_upper AS 'org.apache.spark.sql.catalyst.expressions.Upper';
SELECT temp_upper('hello') as Unqualified,
       session.temp_upper('hello') AS SchemaQualified,
       system.session.temp_upper('hello') AS fullyQualified;

DROP TEMPORARY FUNCTION temp_upper;

-- Test qualifiers - fail cases
SELECT wrong.abs(-5);
SELECT system.wrong.abs(-5);
```

---

## Implementation Plan for Approach 2 (Separate Registries)

### Phase 1: Modify SessionCatalog Storage (Core Changes)
**Files**: `SessionCatalog.scala`, `BaseSessionStateBuilder.scala`

1. Add separate temp registries to `SessionCatalog`
2. Remove cloning logic from `BaseSessionStateBuilder`
3. Update `SessionCatalog` constructor to not require registry parameters
4. Add `lookupTempFunction()`, `lookupBuiltinFunction()` methods

**Estimated effort**: 2-3 days

### Phase 2: Update Registration and Lookup Logic
**Files**: `SessionCatalog.scala`

1. Update `registerFunction()` to route to temp registry
2. Update `isTemporaryFunction()`, `isBuiltinFunction()` logic
3. Update `dropTempFunction()` to work with temp registry
4. Update `listTemporaryFunctions()` to list from temp registry
5. Update `reset()` to clear temp registries only

**Estimated effort**: 1-2 days

### Phase 3: Implement Qualified Name Resolution
**Files**: `FunctionResolution.scala`, `CatalogManager.scala`

1. Add `BUILTIN_NAMESPACE` constant to `CatalogManager`
2. Implement `maybeBuiltinFunctionName()`, `maybeTempFunctionName()` helpers
3. Update `resolveBuiltinOrTempFunction()` with qualification logic
4. Update `lookupBuiltinOrTempFunction()` for metadata
5. Apply same pattern to table functions

**Estimated effort**: 2-3 days

### Phase 4: Testing & Validation
**Files**: Test files, SQL test resources

1. Create comprehensive SQL test file (`sql-session-functions.sql`)
2. Add unit tests for qualification logic
3. Test shadowing scenarios
4. Test error cases
5. Verify backward compatibility
6. Performance testing (verify memory savings)

**Estimated effort**: 3-4 days

### Phase 5: Documentation
**Files**: Documentation, Javadoc

1. Update function documentation
2. Document qualification syntax
3. Add examples to SQL reference
4. Update migration guide if needed

**Estimated effort**: 1-2 days

**Total Estimated Effort**: 2-3 weeks

---

## Files to Modify

### Core Architecture Changes
1. **sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogManager.scala**
   - Add `BUILTIN_NAMESPACE = "builtin"` constant

2. **sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala**
   - Add `tempFunctionRegistry` and `tempTableFunctionRegistry` fields
   - Remove dependency on cloned registries from constructor
   - Add `builtinFunctionRegistry` and `builtinTableFunctionRegistry` references
   - Update `registerFunction()` to route to temp registry
   - Add `lookupTempFunction()`, `lookupBuiltinFunction()` methods
   - Add `lookupTempFunctionInfo()`, `lookupBuiltinFunctionInfo()` methods
   - Update `isTemporaryFunction()`, `isBuiltinFunction()`, `isRegisteredFunction()`
   - Update `dropTempFunction()`, `listTemporaryFunctions()`
   - Update `reset()` method
   - Similar changes for table functions

3. **sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala**
   - Remove `functionRegistry` lazy val (no longer needed)
   - Remove `tableFunctionRegistry` lazy val (no longer needed)
   - Update `catalog` lazy val to not pass registry parameters

### Resolution Logic Changes
4. **sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionResolution.scala**
   - Add `maybeBuiltinFunctionName()` helper method
   - Add `maybeTempFunctionName()` helper method
   - Update `resolveBuiltinOrTempFunction()` with qualification logic
   - Update `lookupBuiltinOrTempFunction()` with qualification logic
   - Update `resolveBuiltinOrTempTableFunction()` with qualification logic
   - Update `lookupBuiltinOrTempTableFunction()` with qualification logic

### Test Files
5. **sql/core/src/test/resources/sql-tests/inputs/sql-session-functions.sql**
   - Create new comprehensive test file

6. **sql/core/src/test/resources/sql-tests/results/sql-session-functions.sql.out**
   - Expected output file

7. **sql/core/src/test/resources/sql-tests/analyzer-results/sql-session-functions.sql.out**
   - Analyzer output file

8. **Unit test suites**
   - Add tests to `FunctionResolutionSuite`
   - Add tests to `SessionCatalogSuite`
   - Update any tests that depend on registry cloning behavior

### Documentation (Optional but Recommended)
9. **docs/sql-ref-syntax-ddl-create-function.md**
   - Add examples with qualified names

10. **docs/sql-ref-name-resolution.md**
    - Document function name resolution with qualifiers

---

## Open Questions

1. **DROP TEMPORARY FUNCTION**: Should we allow qualified names?
   ```sql
   DROP TEMPORARY FUNCTION session.my_func;  -- Allow this?
   ```
   **Recommendation**: Yes, follow variable pattern for consistency

2. **CREATE TEMPORARY FUNCTION**: Should we allow qualified names?
   ```sql
   CREATE TEMPORARY FUNCTION session.my_func AS ...;  -- Allow this?
   ```
   **Recommendation**: Yes, but with restrictions:
   - ONLY allow `session.*` or `system.session.*` qualifiers
   - Strip the qualification during registration (still stored as unqualified)
   - Reject `builtin.*` qualification (builtin functions are immutable)

   ```scala
   // In CREATE TEMPORARY FUNCTION handling:
   def validateTempFunctionName(nameParts: Seq[String]): String = {
     nameParts.length match {
       case 1 => nameParts.head  // Unqualified, OK
       case 2 if nameParts.head.equalsIgnoreCase("session") =>
         nameParts.last  // session.func -> func
       case 3 if nameParts(0).equalsIgnoreCase("system") &&
                 nameParts(1).equalsIgnoreCase("session") =>
         nameParts.last  // system.session.func -> func
       case _ =>
         throw new AnalysisException(
           "Temporary functions must be unqualified or qualified with 'session' or 'system.session'")
     }
   }
   ```

3. **SHOW FUNCTIONS**: Should output include qualifications?
   ```sql
   SHOW FUNCTIONS;  -- Should this show session.my_func or just my_func?
   ```
   **Recommendation**:
   - Keep current output format (no qualification for backward compatibility)
   - Add optional LIKE clause support for qualified names:
     ```sql
     SHOW FUNCTIONS LIKE 'builtin.*';  -- Show all builtins
     SHOW FUNCTIONS LIKE 'session.*';  -- Show all temp functions
     ```

4. **V2 Function Catalog**: How to handle qualified names with external catalogs?
   **Recommendation**: Only apply `builtin.*` and `session.*` qualifications to v1 session catalog functions. External v2 catalogs use their own catalog names.

5. **Function metadata (DESCRIBE FUNCTION)**: Should show qualification?
   ```sql
   DESCRIBE FUNCTION abs;           -- Show as builtin.abs?
   DESCRIBE FUNCTION session.abs;   -- If temp shadows builtin
   ```
   **Recommendation**:
   - Show actual namespace in output
   - Indicate if function is builtin, temporary, or persistent

---

## Compatibility Considerations

- **Backward Compatibility**: All existing unqualified function calls continue to work
- **No Breaking Changes**: Qualified names are purely additive functionality
- **Case Sensitivity**: Follows existing Spark behavior (case-insensitive identifiers)
- **Resolution Order**: Unchanged - temp functions shadow builtins for unqualified names

---

## Summary: Answering the Shadowing Question

**Question**: If session functions share the same cloned registry as builtin, does the session function actually overwrite the builtin?

**Answer**: YES, currently temp functions DO overwrite builtins in the session registry, and there's no way to access the shadowed builtin.

### Current Behavior (The Problem)

1. **Session initialization**: Each session gets `functionRegistry = FunctionRegistry.builtin.clone()`
   - This creates a HashMap copy with all builtins

2. **Creating temp function**: `CREATE TEMPORARY FUNCTION abs AS 'MyCustomAbs'`
   - Calls `functionRegistry.registerFunction(FunctionIdentifier("abs"), ...)`
   - HashMap.put() **overwrites** the builtin entry
   - The original builtin `abs` is now inaccessible in this session

3. **Detection mechanism**: How does Spark know if a function is temp?
   ```scala
   def isTemporaryFunction(name: FunctionIdentifier): Boolean = {
     name.database.isEmpty &&              // No database qualifier
     isRegisteredFunction(name) &&         // Exists in session registry
     !isBuiltinFunction(name)              // NOT in original builtin registry
   }

   def isBuiltinFunction(name: FunctionIdentifier): Boolean = {
     FunctionRegistry.builtin.functionExists(name)  // Check ORIGINAL
   }
   ```
   - The system checks the ORIGINAL immutable `FunctionRegistry.builtin` to determine if a function is builtin
   - If it exists in session registry but NOT in original builtin = temporary function
   - If it exists in BOTH = builtin (not shadowed)
   - If it exists in original but shadowed in session = user has no way to tell!

### Our Solution (Qualification to the Rescue)

Our design solves this by introducing qualified lookups:

1. **Unqualified lookup** (backward compatible, allows shadowing):
   ```scala
   if (name.size == 1) {
     v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
     // Looks in session registry -> temp shadows builtin
   }
   ```

2. **Builtin-qualified lookup** (bypasses shadowing):
   ```scala
   if (maybeBuiltinFunctionName(name)) {  // builtin.* or system.builtin.*
     FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(name.last), arguments)
     // Goes DIRECTLY to original builtin registry!
   }
   ```

3. **Session-qualified lookup** (explicit temp):
   ```scala
   if (maybeTempFunctionName(name)) {  // session.* or system.session.*
     v1SessionCatalog.resolveBuiltinOrTempFunction(name.last, arguments)
     // Looks in session registry
   }
   ```

### Example Demonstrating the Fix

```sql
-- Before our changes (current Spark):
CREATE TEMPORARY FUNCTION abs AS 'com.example.MyAbs';
SELECT abs(-5);          -- Uses temp, builtin is LOST
-- NO WAY to access builtin abs() for the rest of the session!

-- After our changes:
CREATE TEMPORARY FUNCTION abs AS 'com.example.MyAbs';
SELECT abs(-5);                 -- Uses temp (backward compatible)
SELECT builtin.abs(-5);         -- Uses builtin (NEW - solves shadowing!)
SELECT system.builtin.abs(-5);  -- Uses builtin (fully qualified)
SELECT session.abs(-5);         -- Uses temp (explicit)

-- Drop the temp, builtin automatically accessible again
DROP TEMPORARY FUNCTION abs;
SELECT abs(-5);                 -- Uses builtin
```

### Why This Design Works - The Two Registry Architecture

**CRITICAL INSIGHT**: There are **TWO physically separate registry objects**, not one!

**IMPORTANT**: We are **NOT proposing to change the cloning behavior**. Sessions will continue to clone the builtin registry. We only change WHERE we look during resolution based on qualification.

```scala
// REGISTRY #1: The original, immutable, global builtin registry
// Located at: FunctionRegistry.builtin
// Type: SimpleFunctionRegistry
// Lifetime: Created once at JVM startup, never modified
// Contains: All builtin functions like abs, sum, concat, etc.
object FunctionRegistry {
  val builtin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, (info, builder)) =>
      fr.internalRegisterFunction(FunctionIdentifier(name), info, builder)
    }
    fr  // This object is NEVER modified after initialization
  }
}

// REGISTRY #2: The per-session, mutable, cloned registry
// Located at: session.catalog.functionRegistry
// Type: SimpleFunctionRegistry (a DIFFERENT instance)
// Lifetime: Created per session by cloning builtin registry
// Contains: Initially all builtins, then temp functions can overwrite
class BaseSessionStateBuilder {
  protected lazy val functionRegistry: FunctionRegistry = {
    FunctionRegistry.builtin.clone()  // Creates a NEW HashMap with copies
  }
}
```

**What happens when a temp function is created:**

```scala
CREATE TEMPORARY FUNCTION abs AS 'MyCustomAbs'

// Internally executes:
session.catalog.functionRegistry.registerFunction(
  FunctionIdentifier("abs"),
  myCustomBuilder
)

// This calls HashMap.put() which:
// 1. OVERWRITES the "abs" entry in session.functionRegistry
// 2. Does NOT touch FunctionRegistry.builtin (different object!)
```

**After creating temp `abs`:**
- `FunctionRegistry.builtin` still contains original `abs` → **UNCHANGED**
- `session.functionRegistry` contains custom `abs` → **OVERWRITTEN**

**Resolution with our design:**

```scala
// Case 1: builtin.abs(-5)
if (maybeBuiltinFunctionName(name)) {
  // Look in FunctionRegistry.builtin (ORIGINAL global registry)
  FunctionRegistry.builtin.lookupFunction(FunctionIdentifier("abs"), args)
  // Finds: original builtin abs, because this registry was never modified!
}

// Case 2: session.abs(-5) or just abs(-5)
if (maybeTempFunctionName(name) || name.size == 1) {
  // Look in session.catalog.functionRegistry (PER-SESSION cloned registry)
  v1SessionCatalog.resolveBuiltinOrTempFunction("abs", args)
  // Finds: temp function, because it overwrote the builtin in THIS registry
}
```

**The key**: Both registries use the same key `FunctionIdentifier("abs")`, but they're **different HashMap objects in memory**:
- Original: `FunctionRegistry.builtin.functionBuilders.get("abs")` → builtin
- Session: `session.functionRegistry.functionBuilders.get("abs")` → temp

**Visual representation:**

```
┌─────────────────────────────────────────────────────┐
│ FunctionRegistry.builtin (GLOBAL, IMMUTABLE)        │
│ HashMap: {                                          │
│   "abs" -> (ExpressionInfo, BuiltinAbsBuilder)     │ ← Always here
│   "sum" -> (ExpressionInfo, BuiltinSumBuilder)     │
│   ...                                               │
│ }                                                   │
└─────────────────────────────────────────────────────┘
                      ↓ clone()

┌─────────────────────────────────────────────────────┐
│ session.functionRegistry (PER-SESSION, MUTABLE)     │
│ HashMap: {                                          │
│   "abs" -> (ExpressionInfo, CustomAbsBuilder)      │ ← Overwritten!
│   "sum" -> (ExpressionInfo, BuiltinSumBuilder)     │
│   ...                                               │
│ }                                                   │
└─────────────────────────────────────────────────────┘

When resolving builtin.abs:
  → Look in top HashMap (original) ✓ Found builtin

When resolving abs or session.abs:
  → Look in bottom HashMap (session) ✓ Found temp
```

**IMPORTANT CLARIFICATION - What We're Changing:**

❌ **We are NOT changing**:
- The cloning mechanism - sessions STILL clone the builtin registry
- Session registry initialization - still starts with full copy of all builtins
- How temp functions are stored - still overwrite entries in session registry
- The fact that temp functions shadow builtins in the session registry

✅ **We ARE changing**:
- WHERE we look during resolution based on qualification
- When user writes `builtin.abs()` → look in `FunctionRegistry.builtin` (original)
- When user writes `abs()` → look in `session.functionRegistry` (clone, current behavior)
- This is a **resolution-time** decision, not a **storage-time** change

**The session registry still contains all builtins initially**:
```scala
// This STAYS exactly as it is today:
protected lazy val functionRegistry: FunctionRegistry = {
  FunctionRegistry.builtin.clone()  // Still clones! No change!
}

// When temp function is created, still overwrites in session registry:
CREATE TEMPORARY FUNCTION abs AS 'MyCustomAbs'
// Still calls: session.functionRegistry.put("abs", customBuilder)
// Still overwrites the builtin in THIS registry

// The NEW part is just the resolution logic:
SELECT builtin.abs(-5)  // NEW: bypass session registry, use original
SELECT abs(-5)          // SAME: use session registry (shadowed)
```

This is exactly how session variables work - they don't have separate storage for local vs session variables; the qualification determines which manager to check.

---

## Alternative: Two Separate Registries (Better Design?)

### The User's Excellent Question

**Question**: If we're going to access `FunctionRegistry.builtin` directly for qualified lookups anyway, why clone it? Why not just keep two separate registries from the start?

**Answer**: You're absolutely right! There are TWO possible approaches:

### Approach 1: Minimal Change (What I Originally Proposed)

**Keep current architecture, only change resolution logic:**

✅ **Pros**:
- Minimal code changes
- No risk of breaking existing functionality
- Fast to implement
- Backward compatible storage mechanism

❌ **Cons**:
- Wasteful - each session clones ~400+ builtin functions unnecessarily
- Conceptually confusing - why clone if we have the original?
- Temp functions still "overwrite" builtins in session registry (even though we can bypass it)

```scala
// Current architecture (unchanged):
session.functionRegistry = FunctionRegistry.builtin.clone()  // Memory waste!
// Contains: all 400+ builtins copied

// When temp created:
session.functionRegistry.put("abs", tempBuilder)  // Overwrites clone

// Resolution (ONLY thing that changes):
if (qualified == "builtin") → FunctionRegistry.builtin
else → session.functionRegistry
```

### Approach 2: Two Separate Registries (Cleaner Architecture)

**Eliminate cloning, maintain two distinct registries:**

✅ **Pros**:
- **Memory efficient** - no cloning needed, save ~400+ function entries per session
- **Clearer semantics** - builtins and temps are truly separate
- **No overwriting** - temps never touch builtin registry
- **Better for PATH feature** - each namespace naturally separate

❌ **Cons**:
- More invasive code changes
- Need to update resolution logic in multiple places
- Need to handle registration, listing, dropping differently
- More testing required

```scala
// New architecture:
// 1. Global builtin registry (shared, immutable)
FunctionRegistry.builtin  // Never cloned!

// 2. Per-session temp-only registry (initially EMPTY)
session.tempFunctionRegistry = new SimpleFunctionRegistry()  // Empty!

// When temp created:
session.tempFunctionRegistry.put("abs", tempBuilder)  // No overwriting!

// Resolution (more complex):
if (qualified == "builtin") {
  FunctionRegistry.builtin.lookup(name)
} else if (qualified == "session") {
  session.tempFunctionRegistry.lookup(name)
} else {  // unqualified
  // Check temp first (shadowing), then builtin
  session.tempFunctionRegistry.lookup(name)
    .orElse(FunctionRegistry.builtin.lookup(name))
}
```

### Detailed Comparison

#### Memory Usage

**Approach 1 (Cloning)**:
- Each session: ~400 builtin functions × (ExpressionInfo + Builder) = significant memory
- 100 sessions = 40,000 function entries (mostly duplicates!)

**Approach 2 (Separate)**:
- Global: 400 builtin functions (shared across all sessions)
- Per session: only temp functions (typically 0-10)
- 100 sessions with 5 temps each = 500 function entries total

**Winner**: Approach 2 (much more memory efficient)

#### Code Changes Required

**Approach 1 (Minimal)**:
- Modify: `FunctionResolution.resolveBuiltinOrTempFunction()` - add qualification checks
- Modify: `FunctionResolution.lookupBuiltinOrTempFunction()` - add qualification checks
- Modify: Same for table functions
- No changes to: registration, storage, SessionCatalog

**Approach 2 (Extensive)**:
- Add: `SessionCatalog.tempFunctionRegistry` field
- Modify: `SessionCatalog.registerFunction()` - route to correct registry
- Modify: `SessionCatalog.isTemporaryFunction()` - check temp registry
- Modify: `SessionCatalog.listTemporaryFunctions()` - list from temp registry
- Modify: `SessionCatalog.dropTempFunction()` - drop from temp registry
- Modify: All resolution logic
- Modify: `BaseSessionStateBuilder` - don't clone builtin
- Update: Tests, documentation

**Winner**: Approach 1 (less risk, faster)

#### Conceptual Clarity

**Approach 1**: "Temp functions overwrite builtins in the session registry, but we can bypass it with qualification"
- Confusing: why have both if one overwrites the other?

**Approach 2**: "Builtins and temps are separate; unqualified names check temps first, then builtins"
- Clear: natural shadowing without mutation

**Winner**: Approach 2 (cleaner mental model)

#### Future PATH Feature

For the eventual PATH feature (e.g., `PATH = catalog1.schema1, catalog2.schema2`):

**Approach 2 is better** because each namespace is naturally separate:
- `system.builtin` → FunctionRegistry.builtin
- `system.session` → session.tempFunctionRegistry
- `catalog1.schema1` → separate registry
- `catalog2.schema2` → separate registry

PATH resolution becomes: iterate through PATH and check each registry in order.

**Winner**: Approach 2 (better foundation for PATH)

### Recommendation

Given your excellent observation, I recommend **Approach 2 (Two Separate Registries)** because:

1. **More memory efficient** - significant savings in production
2. **Cleaner architecture** - aligns with PATH goals
3. **Better semantics** - temps and builtins truly separate
4. **Worth the effort** - if we're making changes anyway, do it right

### Implementation Plan for Approach 2

#### Phase 1: Add Temp-Only Registry
```scala
class SessionCatalog(...) {
  // NEW: Separate registry for temp functions only
  private val tempFunctionRegistry: FunctionRegistry = new SimpleFunctionRegistry
  private val tempTableFunctionRegistry: TableFunctionRegistry = new SimpleTableFunctionRegistry

  // Keep reference to builtin (don't clone!)
  private val builtinFunctionRegistry: FunctionRegistry = FunctionRegistry.builtin
  private val builtinTableFunctionRegistry: TableFunctionRegistry = TableFunctionRegistry.builtin
}
```

#### Phase 2: Update Registration
```scala
def registerFunction(name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
  if (name.database.isEmpty) {
    // Temp function - goes in temp registry only
    tempFunctionRegistry.registerFunction(name, info, builder)
  } else {
    // Persistent function - goes to external catalog
    externalCatalog.createFunction(...)
  }
}
```

#### Phase 3: Update Resolution
```scala
def resolveBuiltinOrTempFunction(name: String, args: Seq[Expression]): Option[Expression] = {
  // Check temp first (shadowing behavior)
  tempFunctionRegistry.lookupFunction(FunctionIdentifier(name), args)
    .orElse {
      // Then check builtin
      builtinFunctionRegistry.lookupFunction(FunctionIdentifier(name), args)
    }
}
```

#### Phase 4: Update Qualification Resolution
```scala
// In FunctionResolution:
def resolveBuiltinOrTempFunction(name: Seq[String], ...): Option[Expression] = {
  if (maybeBuiltinFunctionName(name)) {
    // Direct lookup in builtin registry
    FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(name.last), arguments)
  } else if (maybeTempFunctionName(name)) {
    // Direct lookup in temp registry
    v1SessionCatalog.tempFunctionRegistry.lookupFunction(FunctionIdentifier(name.last), arguments)
  } else if (name.size == 1) {
    // Unqualified: check temp first, then builtin (shadowing)
    v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
  } else {
    None
  }
}
```

### My Updated Recommendation

Since you're starting a new project and planning for PATH support, **I recommend Approach 2**. It's more work upfront but provides:
- Better architecture for future PATH feature
- Memory savings
- Clearer separation of concerns
- No conceptual "overwriting" confusion

Would you like me to update the analysis document to focus on Approach 2 instead?

---

## Benefits

1. **Clarity**: Users can explicitly specify builtin vs temp functions
2. **Consistency**: Same pattern as session variables
3. **Disambiguation**: Resolve conflicts when temp function shadows builtin
4. **PATH Preparation**: Foundation for future PATH-based resolution
5. **Documentation**: Self-documenting SQL with explicit qualifications

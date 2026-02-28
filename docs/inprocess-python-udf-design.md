# In-Process Python UDF for Apache Spark — Design & Implementation Plan

## 1. Motivation

Current PySpark Python UDF execution suffers from significant overhead due to its out-of-process
architecture:

```
Current Architecture:
  JVM Executor
    │  serialize (Pickle or Arrow IPC)
    │  Unix socket / pipe
    ▼
  Python Worker Process
    │  deserialize
    │  execute UDF
    │  serialize result
    │  Unix socket / pipe
    ▼
  JVM Executor
    │  deserialize result
```

Even vectorized pandas UDFs — which use Arrow for more efficient batched transfer — still cross a
process boundary. This imposes:

- **Two serialization roundtrips** per batch (JVM → Python, Python → JVM)
- **Socket/pipe IPC overhead** for every batch
- **Process lifecycle cost** for spawning and managing Python worker processes

This design proposes a separate, opt-in **in-process Python UDF framework** that embeds CPython
directly into the Spark executor JVM using [jep (Java Embedded Python)](https://github.com/ninia/jep),
eliminating IPC entirely and achieving zero-copy data passing via the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) and
[PyArrow's `foreign_buffer`](https://arrow.apache.org/docs/python/memory.html#foreign-buffers).

---

## 2. Goals

- **In-process**: CPython runs inside the Spark executor JVM. No separate Python worker process.
- **Zero-copy input**: Spark's off-heap Arrow column buffers are passed to Python as PyArrow arrays
  via native memory address — no `memcpy`.
- **Full Arrow type coverage**: All Spark SQL types (primitives, strings, lists, structs, maps,
  decimals, timestamps) handled uniformly via PyArrow's `Array.from_buffers()`.
- **Separate framework**: No modification to existing Python UDF or pandas UDF paths. New opt-in
  API alongside existing ones.
- **Correct-by-construction concurrency**: Enforce `spark.executor.cores == spark.task.cpus` at
  query planning time so exactly one task runs per executor, eliminating GIL contention.

## 3. Non-Goals

- Replacing existing Python UDF or pandas UDF. Both remain unchanged and available.
- Language-agnostic UDF execution (Wasm, GraalVM, etc.). This design is CPython-specific.
- Zero-copy on output. The result PyArrow array is copied once back into a JVM-managed Arrow
  buffer. This is acceptable — the expensive direction is input (large column data in, small
  result out is common).
- Support for Python < 3.8 or environments without PyArrow installed.

---

## 4. Architecture Overview

```
Spark Executor JVM Process (spark.executor.cores == spark.task.cpus == N)
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  Executor Thread (one task at a time — enforced by config check)    │
│                                                                      │
│  InProcessArrowEvalExec (physical plan node)                         │
│    │                                                                 │
│    │  1. extract native buffer addresses from ArrowColumnVector      │
│    │     (off-heap DirectByteBuffer → long address, no copy)         │
│    │                                                                 │
│    │  2. pass addresses + metadata to CPython via jep                │
│    │     (JNI call, in-process, no socket)                           │
│    │                                                                 │
│    │  ┌──────────────────────────────────────────────────────────┐  │
│    │  │  CPython (embedded via jep SharedInterpreter)            │  │
│    │  │                                                          │  │
│    │  │  pa.foreign_buffer(addr, size)  ← zero-copy wrap        │  │
│    │  │  pa.Array.from_buffers(type, n, [validity, values, ...]) │  │
│    │  │                                                          │  │
│    │  │  result = my_udf(input_array)   ← user UDF executes     │  │
│    │  │                                                          │  │
│    │  │  return result.buffers(), result.type, result.schema     │  │
│    │  └──────────────────────────────────────────────────────────┘  │
│    │                                                                 │
│    │  3. copy result PyArrow buffer → JVM ArrowColumnVector          │
│    │     (one copy on output)                                        │
│    │                                                                 │
│    ▼                                                                 │
│  Result ColumnarBatch                                                │
└──────────────────────────────────────────────────────────────────────┘
```

### Data Flow: Input (Zero-Copy)

```
ArrowColumnVector (JVM off-heap)         CPython
─────────────────────────────            ───────
validityBuffer  @ 0x7f01  ──────────────► pa.foreign_buffer(0x7f01, ...)
offsetsBuffer   @ 0x7f02  ──────────────► pa.foreign_buffer(0x7f02, ...)  ← strings/lists only
valuesBuffer    @ 0x7f03  ──────────────► pa.foreign_buffer(0x7f03, ...)
                                          pa.Array.from_buffers(type, n, [...])
                                               ↓
                                          numpy/pyarrow UDF input
                           same physical memory — no memcpy
```

### Data Flow: Output (One Copy)

```
CPython                                  ArrowColumnVector (JVM)
───────                                  ───────────────────────
result: pa.Array
result.buffers()[1].address  ──copy──►  new off-heap DirectByteBuffer
```

---

## 5. Component Breakdown

### 5.1 jep Bootstrapping — `InProcessPythonPlugin`

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/python/InProcessPythonPlugin.scala`

Leverages Spark's existing `ExecutorPlugin` hook to initialize one embedded CPython interpreter
per executor JVM process at startup.

```scala
class InProcessPythonPlugin extends ExecutorPlugin {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    val pythonExec   = extraConf.getOrDefault("spark.pyspark.python", "python3")
    val sitePackages = extraConf.getOrDefault("spark.inprocess.python.sitePackages", "")
    val config = new JepConfig()
      .addIncludePaths(sitePackages)
      .setRedirectOutputStreams(true)
    SharedInterpreter.initialize(config)
    InProcessPythonRuntime.initialize(pythonExec)
  }

  override def shutdown(): Unit = {
    InProcessPythonRuntime.shutdown()
  }
}
```

**`InProcessPythonRuntime`** (companion object) holds the singleton `SharedInterpreter` reference
and exposes `invoke(serializedUdf: Array[Byte], input: InProcessInput): InProcessResult`.

**Deployment note**: jep requires a native `.so` (`libjep.so` / `libjep.dylib`) on each executor.
This can be distributed via Spark's `--archives` or bundled into a custom executor Docker image.
The plugin is registered via `spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin`.

---

### 5.2 Arrow ↔ PyArrow Zero-Copy Bridge

#### 5.2.1 JVM Side — Buffer Address Extraction

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/python/InProcessArrowBridge.scala`

Recursively extracts native buffer addresses and sizes from `ArrowColumnVector` for any Arrow type:

```scala
case class BufferAddresses(
  typeStr:      String,       // Arrow format string, e.g. "i", "u", "+l"
  numRows:      Int,
  validityAddr: Long,         // 0 if no nulls (all-valid optimization)
  offsetsAddr:  Long,         // 0 for fixed-width types
  valuesAddr:   Long,
  children:     Seq[BufferAddresses]
)

object InProcessArrowBridge {
  def extractAddresses(col: ArrowColumnVector, numRows: Int): BufferAddresses = {
    val vec = col.getValueVector
    BufferAddresses(
      typeStr      = ArrowTypeToFormatString(vec.getField.getType),
      numRows      = numRows,
      validityAddr = addressOf(vec.getValidityBuffer),
      offsetsAddr  = addressOf(vec.getOffsetBuffer),   // null for fixed-width
      valuesAddr   = addressOf(vec.getDataBuffer),
      children     = vec match {
        case c: BaseListVector  => Seq(extractAddresses(c.getDataVector, c.getValueCount))
        case c: NonNullableStructVector =>
          c.getChildrenFromFields.asScala.map(child =>
            extractAddresses(child.asInstanceOf[ArrowColumnVector], numRows))
        case _ => Seq.empty
      }
    )
  }

  private def addressOf(buf: ArrowBuf): Long =
    if (buf == null || buf.capacity() == 0) 0L
    else buf.memoryAddress()
}
```

#### 5.2.2 Python Side — PyArrow Array Reconstruction

**File**: `python/pyspark/inprocess/bridge.py`

Reconstructs a `pyarrow.Array` from native buffer addresses — zero-copy via `pa.foreign_buffer`:

```python
import pyarrow as pa
from typing import Any

def addresses_to_array(addrs: dict) -> pa.Array:
    """
    Reconstruct a pyarrow.Array from native buffer addresses.
    Uses pa.foreign_buffer() — zero-copy, no memcpy.
    """
    type_    = pa.lib.ensure_type(addrs["type_str"])
    num_rows = addrs["num_rows"]

    def make_buf(addr: int, size: int) -> pa.Buffer | None:
        if addr == 0:
            return None
        return pa.foreign_buffer(addr, size)

    validity_buf = make_buf(addrs["validity_addr"], (num_rows + 63) // 64 * 8)
    offsets_buf  = make_buf(addrs["offsets_addr"],  (num_rows + 1) * 4)
    values_buf   = make_buf(addrs["values_addr"],   addrs["values_size"])

    children = [addresses_to_array(c) for c in addrs.get("children", [])]

    return pa.Array.from_buffers(
        type_, num_rows,
        [validity_buf, offsets_buf, values_buf],
        children=children
    )


def array_to_jvm(result: pa.Array) -> dict:
    """
    Extract buffer addresses from a PyArrow result array for JVM to copy back.
    """
    bufs = result.buffers()
    return {
        "type_str":      str(result.type),
        "num_rows":      len(result),
        "null_count":    result.null_count,
        "buffer_addrs":  [b.address if b is not None else 0 for b in bufs],
        "buffer_sizes":  [b.size    if b is not None else 0 for b in bufs],
    }
```

---

### 5.3 UDF Registration API

#### 5.3.1 Python Side — `@inprocess_udf` decorator

**File**: `python/pyspark/inprocess/udf.py`

```python
import cloudpickle
from pyspark.sql.types import DataType
from pyspark.inprocess.bridge import addresses_to_array, array_to_jvm

def inprocess_udf(returnType: DataType):
    """
    Register a Python function as an in-process UDF.
    The function receives pa.Array inputs and must return a pa.Array.

    Example:
        @inprocess_udf(returnType=LongType())
        def double(x: pa.Array) -> pa.Array:
            import pyarrow.compute as pc
            return pc.multiply(x, 2)
    """
    def decorator(func):
        serialized = cloudpickle.dumps(func)
        # Returns an InProcessPythonUDF Column expression when called
        return InProcessPythonUDFWrapper(serialized, returnType)
    return decorator
```

The serialized UDF bytes are broadcast from the driver to all executors via Spark's broadcast
mechanism, avoiding per-task shipping overhead.

#### 5.3.2 Scala Side — `InProcessPythonUDF` Expression

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/python/InProcessPythonUDF.scala`

```scala
// Catalyst leaf expression — carries serialized UDF bytes and output schema
case class InProcessPythonUDF(
  name:           String,
  serializedFunc: Array[Byte],    // cloudpickle bytes, broadcast to executors
  children:       Seq[Expression],
  returnType:     DataType
) extends Expression with NonSQLExpression {
  override def dataType: DataType = returnType
  override def nullable: Boolean  = true
  // Evaluation deferred to InProcessArrowEvalExec (columnar)
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("InProcessPythonUDF must run in columnar mode")
}
```

---

### 5.4 Physical Plan — `InProcessArrowEvalExec`

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/python/InProcessArrowEvalExec.scala`

The physical plan node that drives the full execution pipeline:

```scala
case class InProcessArrowEvalExec(
  udfs:    Seq[InProcessPythonUDF],
  output:  Seq[Attribute],
  child:   SparkPlan
) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { batches =>
      val runtime = InProcessPythonRuntime.get()   // singleton per executor process
      batches.map { batch =>
        val inputCols = udfs.map(udf => udf.children.map { expr =>
          val colIdx = output.indexOf(expr.toAttribute)
          batch.column(colIdx).asInstanceOf[ArrowColumnVector]
        })

        val resultCols = udfs.zip(inputCols).map { case (udf, cols) =>
          // 1. Extract native buffer addresses (zero-copy)
          val addresses = cols.map(c => InProcessArrowBridge.extractAddresses(c, batch.numRows()))

          // 2. Invoke Python UDF via jep (in-process)
          val resultAddrs = runtime.invoke(udf.serializedFunc, addresses, batch.numRows())

          // 3. Copy result back into JVM Arrow buffer (one copy on output)
          InProcessArrowBridge.addressesToColumn(resultAddrs)
        }

        new ColumnarBatch(
          (batch.columns().toSeq ++ resultCols).toArray,
          batch.numRows()
        )
      }
    }
  }
}
```

**`InProcessPythonRuntime.invoke`** calls jep's `SharedInterpreter` to:
1. Deserialize the UDF (cached after first call per executor lifetime)
2. Call `addresses_to_array()` on each input column address set
3. Invoke the UDF function
4. Call `array_to_jvm()` on the result
5. Return the result buffer addresses to Java

---

### 5.5 Config Validation Rule

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/python/InProcessPythonChecks.scala`

Enforces the single-task-per-executor constraint at query planning time:

```scala
object InProcessPythonChecks extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case _: InProcessArrowEvalExec =>
        val executorCores = SQLConf.get.getConf(EXECUTOR_CORES)  // spark.executor.cores
        val taskCpus      = SQLConf.get.getConf(CPUS_PER_TASK)   // spark.task.cpus
        val maxConcurrent = executorCores / taskCpus
        if (maxConcurrent != 1) {
          throw QueryCompilationErrors.inProcessUdfConcurrencyError(executorCores, taskCpus)
        }
      case _ =>
    }
    plan  // no plan transformation — validation only
  }
}
```

Registered in `SparkOptimizer` alongside other pre-execution checks. Provides a clear error:

```
InProcessPythonUDF requires exactly one concurrent task per executor to avoid GIL contention.
Current configuration allows 4 concurrent tasks (spark.executor.cores=4, spark.task.cpus=1).
Set spark.executor.cores == spark.task.cpus, e.g. spark.executor.cores=1.
```

---

### 5.6 Error Handling & Observability

**Python exceptions** are caught in the jep call and re-raised as `SparkException` with the full
Python traceback preserved in the message — same behavior as existing Python UDF errors.

**Interpreter crash** (e.g. segfault in a C extension): treated as executor failure, triggers
Spark's standard task retry mechanism.

**Metrics** exposed via Spark's existing metric system on `InProcessArrowEvalExec`:
- `inProcessUdfTimeMs` — time spent inside CPython per batch
- `zeroCopyInputBytes` — bytes passed to Python without copying
- `outputCopyBytes` — bytes copied on the output path

---

## 6. Implementation Phases

### Phase 1 — Core Foundation (Weeks 1–4)

**Goal**: End-to-end working prototype for primitive types (int, long, double, float).

Steps:
1. Add `jep` as an optional Maven dependency (`provided` scope — not bundled into Spark)
2. Implement `InProcessPythonPlugin` with `SharedInterpreter` initialization
3. Implement `InProcessArrowBridge.extractAddresses` for fixed-width types only
4. Implement Python `bridge.py` with `addresses_to_array` for fixed-width types
5. Implement `InProcessPythonUDF` Catalyst expression
6. Implement `InProcessArrowEvalExec` physical plan (primitive types only)
7. Implement `@inprocess_udf` decorator and manual registration API
8. Implement `InProcessPythonChecks` config validation rule
9. Write end-to-end integration test: `double(longCol)` on a simple DataFrame

**Deliverable**: `SELECT inprocess_double(id) FROM range(1000000)` runs in-process, measurably
faster than equivalent pandas UDF.

---

### Phase 2 — Full Type Coverage (Weeks 5–8)

**Goal**: Support all Spark SQL types via PyArrow's `from_buffers`.

Steps:
1. Extend `InProcessArrowBridge.extractAddresses` for variable-width types (string, binary)
2. Extend for nested types: `ArrayType`, `StructType`, `MapType`
3. Extend for `DecimalType`, `TimestampType`, `DateType`
4. Add null handling: validity bitmap extraction and reconstruction
5. Extend Python `bridge.py` `addresses_to_array` for all types (recursive children)
6. Add type mapping: Spark SQL `DataType` ↔ Arrow format string ↔ PyArrow type
7. Write type coverage tests: one test per Spark SQL type

**Deliverable**: All Spark SQL types work correctly as UDF inputs and outputs.

---

### Phase 3 — UDF Caching & Lifecycle (Weeks 9–10)

**Goal**: Avoid deserializing the UDF on every batch invocation.

Steps:
1. Cache deserialized UDF function in the Python interpreter keyed by hash of serialized bytes
2. Handle UDF eviction if too many distinct UDFs are registered (LRU, configurable size)
3. Ensure correct lifecycle: UDF cache cleared on interpreter shutdown
4. Broadcast serialized UDF bytes from driver to executors (avoid per-task shipping)

**Deliverable**: UDF deserialization happens once per executor lifetime, not once per batch.

---

### Phase 4 — Error Handling & Observability (Weeks 11–12)

**Goal**: Production-quality error reporting and metrics.

Steps:
1. Catch Python exceptions in jep; preserve full Python traceback in `SparkException`
2. Handle interpreter-level failures with clear error messages
3. Implement `inProcessUdfTimeMs`, `zeroCopyInputBytes`, `outputCopyBytes` metrics
4. Expose metrics in Spark UI (extend existing Python UDF metrics panel if present)
5. Add logging for interpreter initialization, UDF registration, and fatal errors

**Deliverable**: Errors are actionable; metrics are visible in Spark UI.

---

### Phase 5 — Testing & Benchmarks (Weeks 13–16)

**Goal**: Validate correctness and quantify performance improvement.

Steps:
1. Unit tests:
   - All Arrow types round-trip correctly (input → Python → output)
   - Null handling (all-null, mixed-null, all-valid columns)
   - Exception propagation from Python to Spark
   - Config validation error messages
2. Integration tests:
   - End-to-end Spark jobs with in-process UDFs on realistic datasets
   - Multi-partition jobs
   - UDFs with multiple input columns
3. Benchmark suite:
   - Compare `inprocess_udf` vs. `pandas_udf` vs. regular Python `udf` on:
     - Primitive column transformation (e.g. `x * 2`)
     - String manipulation (e.g. `upper(s)`)
     - Struct field extraction
   - Measure: throughput (rows/sec), latency per batch, memory overhead
4. Document benchmark results

**Deliverable**: Benchmarks show measurable improvement over `pandas_udf`; test suite passes.

---

## 7. Known Constraints & Limitations

### 7.1 Single Task per Executor

In-process UDFs require `spark.executor.cores == spark.task.cpus`. This prevents GIL contention
between concurrent tasks. This constraint is enforced at query planning time with a clear error.

**Implication**: For a job with `N` total cores, you need `N` executors with 1 core each, rather
than fewer executors with multiple cores. Total parallelism is unchanged; executor count increases.
This is already a common pattern in Python-heavy Spark deployments.

### 7.2 Python 3.8+ Required

jep requires Python 3.8+. This is already required by PySpark as of Spark 3.4.

### 7.3 PyArrow Required

PyArrow must be installed in the Python environment on each executor. PyArrow is already a
required dependency for pandas UDFs, so this is not a new requirement for users of vectorized UDFs.

### 7.4 jep Native Library Deployment

jep requires `libjep.so` (Linux) / `libjep.dylib` (macOS) on each executor. Distribution options:
- Include in a custom executor Docker image (recommended for containerized deployments)
- Distribute via `spark.executorEnv` and `--archives`
- Pre-install on cluster nodes

### 7.5 Output Copy

Result PyArrow arrays are copied once back into JVM-managed Arrow buffers. Zero-copy output
(pre-allocated JVM buffer, Python writes directly) is deferred to a future optimization.

### 7.6 Sub-Interpreter Parallelism

Python 3.12+ per-interpreter GIL (PEP 684) and full C extension support for sub-interpreters
(PEP 489) are not yet widely adopted across the ecosystem (numpy, pandas, scipy). Until packages
fully support PEP 489, sub-interpreters are not used. The single-task-per-executor constraint is
the workaround. This can be revisited as the ecosystem matures.

---

## 8. Open Questions

1. **jep dependency scope**: Should jep be an optional `provided` dependency (user must install)
   or bundled into a Spark distribution artifact? Optional is safer for binary size and licensing.

2. **Multi-UDF batching**: If a query has multiple in-process UDFs in sequence, should they be
   fused into a single Python call (one round-trip for all UDFs) or evaluated separately? Fusion
   reduces JNI call overhead but complicates the execution model.

3. **Output zero-copy (future)**: Can the JVM pre-allocate an output Arrow buffer and pass its
   address to Python for direct write? This would eliminate the one output copy but requires the
   UDF to write into a pre-allocated buffer rather than returning a new array.

4. **REPL / notebook experience**: How should in-process UDFs behave in interactive notebooks
   where the Python environment on the driver differs from executors?

5. **Config flexibility**: Should there be a `spark.inprocess.python.strict` flag to warn instead
   of error on the concurrency check, for users who accept potential GIL contention?

---

## 9. Effort Summary

| Phase | Component | Effort |
|---|---|---|
| 1 | jep bootstrap + primitive type end-to-end | 4 weeks |
| 2 | Full type coverage (strings, nested, nulls) | 4 weeks |
| 3 | UDF caching + broadcast | 2 weeks |
| 4 | Error handling + metrics | 2 weeks |
| 5 | Testing + benchmarks | 4 weeks |
| **Total** | | **~4 months** |

---

## 10. Appendix: Key Dependencies

| Dependency | Version | Role |
|---|---|---|
| [jep](https://github.com/ninia/jep) | 4.x | Embed CPython in JVM via JNI |
| [PyArrow](https://arrow.apache.org/docs/python/) | 12+ | Zero-copy Arrow buffer wrapping via `foreign_buffer` |
| [cloudpickle](https://github.com/cloudpipe/cloudpickle) | 2.x | Serialize Python UDF closure (already a PySpark dep) |
| Apache Arrow Java | (Spark-bundled) | `ArrowColumnVector`, `ArrowBuf` buffer address extraction |

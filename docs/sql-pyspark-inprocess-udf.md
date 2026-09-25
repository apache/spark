---
layout: global
title: In-Process Python UDFs
displayTitle: In-Process Python UDFs
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## Runtime and result contract

Each executor owns a dedicated interpreter thread. The plugin initializes the
interpreter on that thread, and task calls and shutdown are dispatched to the
same thread. The JVM is asked to allocate an 8 MiB stack for this thread; the
actual size is platform-dependent. Calls from concurrent tasks are queued on the
interpreter thread.
One task per executor is recommended for throughput, but is not a correctness requirement.
Application-level Python parallelism comes from multiple executor JVMs.
The plugin configures JEP's process-wide interpreter with hash seed `0`, matching
Spark's default Python worker seed. It must initialize before any other JEP user in
the JVM. The seed cannot change between SparkContexts in the same process; a custom
worker `PYTHONHASHSEED` does not override this embedded-runtime setting.

Task cancellation cannot safely stop arbitrary native Python code. An interrupted
caller waits for the current invocation to finish before freeing the Arrow CDI
structures, then restores its interrupt status. A UDF that never returns can
therefore prevent its task from completing cancellation and block every subsequent
in-process UDF on that executor, including calls from other tasks, jobs, and sessions.
Recovery from a permanently hung invocation requires replacing the executor process.
Plugin shutdown stops accepting new calls and waits up to five seconds for the interpreter thread. If a call is
still running, cleanup stays queued behind it; its memory remains live until the
call returns or the process exits. Shutdown does not forcibly interrupt native
code. A new interpreter cannot start until the previous one has fully stopped.

A scalar UDF must return a `pyarrow.Array` with exactly one element per input row.
The runtime checks the result type against the declared Spark type, including
nested fields, decimal scale, and timestamp unit/timezone. Value types must match
exactly: use an explicit PyArrow cast in the UDF for numeric or other conversions.
Nested field nullability may differ if the actual values satisfy the declared nullability. Sliced results, including nested
child slices, are copied to remove offsets that Arrow Java's CDI importer cannot
read. Compatible results retain zero-copy transfer.

The API produces a regular `PythonUDF` expression with an in-process evaluation
type. Spark's existing `ArrowEvalPython` planning rules handle aggregation,
nested calls, nondeterminism, and filter/limit pushdown. A dedicated
`InProcessArrowEvalPythonExec` extends `EvalPythonExec`, reusing its projection,
row queue, result join, and partition-evaluator path. Ordinary Python UDFs continue
to use Python workers.

`maxRecordsPerBatch <= 0` means no row-count limit. The independent
`spark.sql.execution.arrow.maxBytesPerBatch` limit still applies when positive.
Only UDF arguments are converted to Arrow. Other columns stay in Spark rows,
buffered in a spillable queue until the results are joined back. Duplicate nested
field names in UDF arguments or declared results are rejected before Arrow Java
reads their buffers.

Each batch uses fresh input buffers. A Python function may retain an input array;
later batches do not overwrite it. Retained arrays keep native memory alive, so
functions should release them when no longer needed. JVM input vectors and result
vectors are released on task completion, early termination and failure.

UDF deserialization uses PySpark's bundled cloudpickle. Each task registers its
own function instance once and passes a small handle for subsequent batches.
Task completion queues release of the registered function and its closure state. Imported
Python modules still share executor-wide state. Configured site-packages paths
are supplied to JEP before its first construction, so JEP itself can be found in
an archived venv. They are then processed with `site.addsitedir`, including `.pth`
files. Spark's own PySpark and Py4J distribution paths come first, followed by
configured directories, newly discovered `.pth` paths, and existing system paths.
Already imported modules cannot be replaced by changing the search path.

Spark broadcasts, accumulators, `SparkContext.addPyFile`, and Python `TaskContext`
are not supported by this embedded runtime. Captured broadcast and accumulator
objects are rejected during serialization; functions must not access them through
imported modules either. Install modules on executors before startup, optionally
using `spark.inprocess.python.sitePackages`. Session-scoped `spark.pythonWorkerEnv.*`
settings are rejected: the shared interpreter cannot apply per-session process
environments. Configure environment variables before the executor starts, for example
with `spark.executorEnv.NAME` (or the launching environment in local mode).
SQL registration through `spark.udf.register` is not supported and is rejected at registration time.
Spark Connect does not support this execution mode; both client SQL registration and
server planning reject it. The decorator accepts a `DataType` or a DDL string; DDL
strings are parsed lazily with the active Spark session. It exposes `func`, `returnType`,
`evalType`, `deterministic`, and `asNondeterministic()` along with the function's name
and docstring.
Functions must receive at least one input column (a literal also works) to determine
the batch length. Positional and keyword arguments are supported. Functions are
serialized on first use, so globals can be defined or rebound after decoration
and before that first call. The driver's Python major.minor
version must match the embedded interpreter; registration checks this before
unpickling. Python exceptions, including `SystemExit` during deserialization or
execution, are converted into task failures. Tracebacks honor the query's
`spark.sql.execution.pyspark.udf.hideTraceback.enabled`,
`spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled`, and
`spark.sql.execution.pyspark.udf.tracebackWithLocals.enabled` settings. Native process
termination remains outside this exception handling.

## Overview

In-process Python UDFs embed CPython directly into the Spark executor JVM using
[jep (Java Embedded Python)](https://github.com/ninia/jep), eliminating the IPC overhead of
standard Python UDFs and pandas UDFs. Data is passed to Python as
[PyArrow](https://arrow.apache.org/docs/python/) arrays via the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) — zero-copy
for compatible input and output buffers. Row-to-Arrow conversion and normalization
of sliced results still copy data.

**Use `inprocess_udf` when:**
- You are already using `pandas_udf` for vectorized transformations and want lower latency.
- Your UDF operates on Arrow/PyArrow arrays (e.g. using `pyarrow.compute`).
- You can deploy enough executor JVMs for Python parallelism (see [Requirements](#requirements)).

**Stick with `pandas_udf` or `udf` when:**
- You need pandas Series semantics in your UDF logic.
- You need concurrent Python invocations within a single executor.
- You are not able to install jep on executors.

---

## Quick Start

### 1. Install dependencies

```bash
pip install "jep>=4.3.2" pyarrow cloudpickle
```

JEP and `org.apache.arrow:arrow-c-data` are provided dependencies and are not
bundled with Spark. Supply their JARs on the driver/executor classpaths before
starting Spark, and make the JEP native library available. Use an `arrow-c-data`
version matching Spark's Arrow Java version. Installing the Python packages alone
does not supply the Arrow Java CDI JAR.

Building JEP from source requires a JDK, a C compiler, and development headers for
the Python version being embedded (for example, `python3.12-dev` on Ubuntu with
Python 3.12). These headers are build dependencies; running a prebuilt compatible
JEP installation does not require the development package. The corresponding
Python shared library must remain available at runtime.

### 2. Register the plugin

```python
spark = SparkSession.builder \
    .config("spark.plugins",
            "org.apache.spark.sql.execution.python.InProcessPythonPlugin") \
    .config("spark.executor.cores", "1") \
    .config("spark.task.cpus", "1") \
    .getOrCreate()
```

### 3. Write and call a UDF

```python
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import LongType

@inprocess_udf(return_type=LongType())
def double(x):
    return pc.multiply(x, 2)

df = spark.range(10)
df.select(double(df["id"])).show()
```

The function receives a `pa.Array` for each input column and must return a `pa.Array`.

---

## Examples

### String transformation

```python
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import StringType

@inprocess_udf(return_type=StringType())
def upper(s):
    return pc.utf8_upper(s)

df = spark.createDataFrame([("hello",), ("world",)], ["text"])
df.select(upper(df["text"])).show()
# +------------+
# |upper(text) |
# +------------+
# |HELLO       |
# |WORLD       |
# +------------+
```

### Multi-column UDF

A UDF receives one `pa.Array` argument per input column:

```python
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import DoubleType

@inprocess_udf(return_type=DoubleType())
def weighted_sum(x, y):
    return pc.add(pc.multiply(x, 0.6), pc.multiply(y, 0.4))

df = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0)], ["x", "y"])
df.select(weighted_sum(df["x"], df["y"])).show()
```

### Closure capture

Free variables are captured by cloudpickle and frozen into the serialized UDF. The captured
value is captured at first use and shipped with the function to every executor.
Rebinding a global before the first call is reflected in the serialized function;
subsequent calls reuse the cached serialization:

```python
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import DoubleType

SCALE_FACTOR = 100.0

@inprocess_udf(return_type=DoubleType())
def scale(x):
    return pc.multiply(x, SCALE_FACTOR)
```

### Non-deterministic UDF

Pass `deterministic=False` when the UDF produces different results for the same input (e.g.
random sampling). This prevents the optimizer from deduplicating or reordering calls:

```python
import random
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import DoubleType

@inprocess_udf(return_type=DoubleType(), deterministic=False)
def add_noise(x):
    noise = pa.array([random.gauss(0.0, 0.01) for _ in range(len(x))])
    return pc.add(x, noise)
```

---

## Requirements

| Requirement | Detail |
|---|---|
| Python | 3.11+; driver and embedded major.minor versions must match |
| jep | 4.3.2+ (`pip install jep`) |
| `arrow-c-data` JAR | Provided separately; match Spark's Arrow Java version |
| PyArrow | 18.0.0+ |
| cloudpickle | Bundled with PySpark |
| Python concurrency | One invocation at a time per executor (see below) |

### Executor concurrency

In-process UDFs use one `SharedInterpreter` on a dedicated thread per executor.
Multiple Spark tasks can share an executor, including with fractional
`spark.task.cpus`, but their Python invocations are serialized. `local[*]` therefore
works but does not provide parallel embedded Python execution.

For throughput, consider `spark.executor.cores=1, spark.task.cpus=1` and multiple
executors. More executors also mean more JVM overhead; compare with worker-based
Arrow UDFs under the same total CPU and memory budget.

---

## Deployment and Distribution

### Local development

For local development (e.g. `SparkSession.builder.master("local[*]")`), install jep and the
required Python packages into the virtual environment you run PySpark from. The venv's
site-packages must be supplied explicitly to the embedded interpreter. Use the
PySpark distribution from the same Spark build; a separately pip-installed PySpark
version may not contain this API or match the JVM classes.

```bash
python3 -m venv .venv
.venv/bin/pip install "jep>=4.3.2" pyarrow
source .venv/bin/activate
```

JEP and Arrow CDI must be on the JVM **system classpath** before the JVM starts.
`--jars` alone only configures Spark's user classloader and is insufficient. The CDI
JAR must match the Arrow Java version in the Spark build. For example:

```bash
JEP_DIR="$(python3 -c 'import importlib.util, pathlib; print(pathlib.Path(importlib.util.find_spec("jep").origin).parent)')"
ARROW_C_DATA_JAR=/absolute/path/to/arrow-c-data.jar
spark-submit --master 'local[1]' \
  --driver-class-path "$JEP_DIR/*:$ARROW_C_DATA_JAR" \
  --conf "spark.driver.extraLibraryPath=$JEP_DIR" \
  --conf "spark.inprocess.python.sitePackages=$(dirname "$JEP_DIR")" \
  --conf spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin \
  my_app.py
```

### Cluster deployment — prerequisite: build and zip the venv

Both YARN and Kubernetes support distributing a virtual environment via `--archives`. Build the
venv on a machine that matches the executor OS and Python version:

```bash
python3 -m venv myvenv
myvenv/bin/pip install "jep>=4.3.2" pyarrow cloudpickle my-custom-lib
(cd myvenv && zip -r ../myvenv.zip .)
```

Adjust `python3.11` in the paths below to match the Python version in your venv.

---

### YARN

Spark extracts `--archives` to a relative path (`./myvenv/`) on each YARN container before the executor JVM
starts. The key extra config compared to local development is
`spark.executorEnv.PYSPARK_PYTHON`, which tells PySpark's Python worker to use the venv's
Python executable (ensuring a consistent Python version between the JVM-embedded interpreter
and any out-of-process fallbacks).

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --archives myvenv.zip#myvenv \
  --files /absolute/path/to/arrow-c-data.jar#arrow-c-data.jar \
  --conf 'spark.executor.extraClassPath=./myvenv/lib/python3.11/site-packages/jep/*:./arrow-c-data.jar' \
  --conf spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin \
  --conf spark.executor.cores=1 \
  --conf spark.task.cpus=1 \
  --conf spark.executorEnv.PYSPARK_PYTHON=./myvenv/bin/python3 \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=./myvenv/lib/python3.11/site-packages/jep" \
  --conf spark.inprocess.python.sitePackages=./myvenv/lib/python3.11/site-packages \
  my_app.py
```

For local execution on the driver, use the driver classpath and native-library
settings from the local example. A client-mode driver does not execute executor UDFs.

---

### Kubernetes

#### Option A: Custom Docker image (recommended)

Build an executor image from the same Spark distribution used by the driver. The
image must contain Python 3.11 or newer, a matching Python shared library, and a
JDK compatible with that Spark build. Do not assume `apache/spark:latest` has these
versions or includes JEP's build dependencies.

Install JEP and PyArrow into a known environment, such as `/opt/venv`, while
building the image. Building JEP from its source distribution additionally requires
a C compiler, the matching Python development headers, and a JDK (`JAVA_HOME` set).
Then prepare the image with:

- the JEP JAR and matching Arrow CDI JAR in `/opt/spark/jars`;
- JEP's native library directory on the JVM library path before startup;
- `spark.inprocess.python.sitePackages` pointing to `/opt/venv`'s site-packages;
- Spark's matching `python/lib/pyspark.zip` and Py4J zip in the Spark distribution.

The plugin adds Spark's Python distribution paths itself, ahead of any PySpark
package installed in the venv. The submission example below assumes Python 3.11
and `/opt/venv/lib/python3.11/site-packages/jep` for the native library directory;
adjust both paths to the Python version used to build JEP.

**Submit:**

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:<port> \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-registry/spark-inprocess:tested-build \
  --conf spark.inprocess.python.sitePackages=/opt/venv/lib/python3.11/site-packages \
  --conf spark.executor.extraLibraryPath=/opt/venv/lib/python3.11/site-packages/jep \
  --conf spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin \
  --conf spark.executor.cores=1 \
  --conf spark.task.cpus=1 \
  my_app.py
```

#### Option B: `--archives` with remote file upload

If you cannot build a custom image, Spark on Kubernetes can distribute archives via a remote
staging area (e.g. S3 or GCS). Set `spark.kubernetes.file.upload.path` to an object storage
path that both the driver and executors can access. Provision JEP and Arrow CDI JARs
in `/opt/inprocess/jars` on every executor using an image layer or a mounted volume
before the JVM starts. The image must also have Python 3.11+ and the shared
library matching the archived JEP build. Use the same JEP version as the archived
venv. A classpath wildcard pointing inside the archive is insufficient here: the JVM expands wildcards
before Spark downloads and extracts the archive.

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:<port> \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=my-registry/spark-python311:tested-build \
  --conf spark.kubernetes.file.upload.path=s3a://my-bucket/spark-uploads \
  --archives myvenv.zip#myvenv \
  --conf 'spark.executor.extraClassPath=/opt/inprocess/jars/*' \
  --conf spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin \
  --conf spark.executor.cores=1 \
  --conf spark.task.cpus=1 \
  --conf spark.executorEnv.PYSPARK_PYTHON=./myvenv/bin/python3 \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=./myvenv/lib/python3.11/site-packages/jep" \
  --conf spark.inprocess.python.sitePackages=./myvenv/lib/python3.11/site-packages \
  my_app.py
```

---

## Configuration Reference

### `spark.plugins`

| Default | `(none)` |
|---|---|
| **Required value** | `org.apache.spark.sql.execution.python.InProcessPythonPlugin` |

Registers the in-process Python plugin. This initializes the `SharedInterpreter` on each
executor at startup. Without this plugin, in-process UDF execution fails with an
initialization error. Task calls and cleanup never create or restart an interpreter.

---

### `spark.inprocess.python.sitePackages`

| Default | `(none)` |
|---|---|
| **Type** | Comma-separated list of absolute or relative directory paths |

Site-package directories supplied to JEP before interpreter construction, so the
`jep` package must be directly importable from these directories. After construction,
paths are made absolute and processed with `site.addsitedir`, including `.pth` files.
Spark distribution paths take precedence over these directories; configured paths
then take precedence over system paths for modules not yet imported. `.pth` files
are processed too late to locate JEP itself during construction.

**When you need this:** When you distribute a Python virtual environment via `--archives` and
need packages from that venv to be importable inside UDFs. The problem is that the jep
interpreter starts with the *system* Python's `sys.path`, which does not include the distributed
venv's site-packages. Setting this config tells the plugin where to find the venv's packages.

**Typical usage with `--archives`:**

```
spark.inprocess.python.sitePackages = ./myvenv/lib/python3.11/site-packages
```

The relative path `./myvenv/` resolves to the directory where Spark extracted your archive on
the executor node. Spark unpacks `--archives myvenv.zip#myvenv` to `./myvenv/` at task launch
time.

**Multiple paths** (comma-separated):

```
spark.inprocess.python.sitePackages = ./venv/lib/python3.11/site-packages,/opt/custom/lib
```

**When you do NOT need this:**
- Executors where all required packages are pre-installed on the system Python path.

---

### `spark.executor.extraJavaOptions` — `java.library.path`

jep requires its native library (`libjep.so` on Linux, `libjep.dylib` on macOS) to be on the
JVM's native library path. **This must be set before the JVM starts** — `System.setProperty()`
has no effect after JVM startup, so runtime configuration is not possible.

The reliable approach is to set `-Djava.library.path` via `spark.executor.extraJavaOptions`:

```
spark.executor.extraJavaOptions = -Djava.library.path=./myvenv/lib/python3.11/site-packages/jep
```

When using `--archives`, Spark extracts the archive to a predictable relative path (`./myvenv/`),
so the path above is stable across executor nodes without any per-node configuration.

---

### `spark.executor.cores` and `spark.task.cpus`

Multiple tasks may share an executor, including fractional `spark.task.cpus` values.
Python invocations run one at a time per executor. For throughput, consider multiple
executors with:

```
spark.executor.cores = 1
spark.task.cpus      = 1
```

---

## Supported Types

Common supported Spark SQL input/output types include:

| Category | Types |
|---|---|
| Numeric | `ByteType`, `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType` |
| Boolean | `BooleanType` |
| String / Binary | `StringType`, `BinaryType` |
| Temporal | `DateType`, `TimestampType` |
| Complex | `ArrayType`, `StructType`, `MapType` |

Nested values must satisfy the declared nullability. Map keys cannot be null.
Only types representable by Spark's Arrow conversion and JVM Arrow accessors are
supported; this is not a guarantee for every Spark SQL type. Unsupported return
types are rejected on the driver before the function is serialized or tasks start.

---

## Migrating from `pandas_udf`

`inprocess_udf` and `pandas_udf` have nearly identical call-site syntax. The primary change is
the input/output type: `pandas.Series` becomes `pa.Array`, and pandas operations are replaced
with their `pyarrow.compute` equivalents.

### Side-by-side examples

#### Numeric transform

```python
# pandas_udf
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import LongType

@pandas_udf(LongType())
def double_pandas(x: pd.Series) -> pd.Series:
    return x * 2
```

```python
# inprocess_udf  — replace pd.Series arithmetic with pc.multiply
import pyarrow.compute as pc
from pyspark.inprocess.udf import inprocess_udf
from pyspark.sql.types import LongType

@inprocess_udf(return_type=LongType())
def double_inprocess(x):          # x is pa.Array, not pd.Series
    return pc.multiply(x, 2)
```

#### String transform

```python
# pandas_udf
@pandas_udf(StringType())
def upper_pandas(s: pd.Series) -> pd.Series:
    return s.str.upper()
```

```python
# inprocess_udf  — replace .str.upper() with pc.utf8_upper()
@inprocess_udf(return_type=StringType())
def upper_inprocess(s):
    return pc.utf8_upper(s)
```

#### Multi-column

```python
# pandas_udf
@pandas_udf(DoubleType())
def score_pandas(x: pd.Series, y: pd.Series) -> pd.Series:
    return x * 0.6 + y * 0.4
```

```python
# inprocess_udf  — chain pc.multiply / pc.add instead of pandas arithmetic
@inprocess_udf(return_type=DoubleType())
def score_inprocess(x, y):
    return pc.add(pc.multiply(x, 0.6), pc.multiply(y, 0.4))
```

### Call-site syntax

The call site is identical — `inprocess_udf` returns a standard PySpark Column expression:

```python
df.select(double_inprocess(df["id"])).show()
df.withColumn("score", score_inprocess(df["x"], df["y"])).show()
```

### When migration is beneficial

| Scenario | Recommendation |
|---|---|
| Simple arithmetic or comparisons | **Migrate** — `pyarrow.compute` is 1:1 with pandas ops |
| String transforms (`upper`, `lower`, `trim`) | **Migrate** — `pc.utf8_*` covers the common cases |
| Wide schemas or large batches (high IPC cost) | **Migrate** — speedup is greatest when IPC serialization dominates |
| Heavy pandas regex (`.str.extract`, `.str.replace`) | **Consider** — PyArrow has equivalents (`pc.extract_regex`, `pc.replace_substring_regex`) but the code change is larger |
| `pandas.groupby`, `rolling`, `resample` | **Stay on `pandas_udf`** — no Arrow compute equivalents |
| UDF calls an external library expecting `pd.Series` | **Stay on `pandas_udf`**, or wrap with `pd.Series(array.to_pylist())` at the boundary |

### Executor sizing

Unlike worker-based UDFs, in-process UDF invocations share one interpreter thread
per executor. Use multiple executors for Python parallelism and include the added
JVM memory in comparisons (see [Requirements](#requirements)).

---

## Choosing Between UDF Types

| | `udf` | `pandas_udf` | `inprocess_udf` |
|---|---|---|---|
| Input type | Python scalar | `pandas.Series` | `pa.Array` |
| Output type | Python scalar | `pandas.Series` | `pa.Array` |
| Data transfer | Pickle, row-by-row | Arrow IPC (process boundary) | Arrow CDI (zero-copy, in-process) |
| Requires jep | No | No | Yes |
| Python concurrency per executor | Multiple workers | Multiple workers | One invocation |
| Best for | Simple row transforms | pandas-heavy logic | High-throughput Arrow transforms |

### Related pages

* [Scalar User Defined Functions (UDFs)](sql-ref-functions-udf-scalar.html)
* [PySpark Usage Guide for Pandas with Apache Arrow](sql-pyspark-pandas-with-arrow.html)

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

## Overview

In-process Python UDFs embed CPython directly into the Spark executor JVM using
[jep (Java Embedded Python)](https://github.com/ninia/jep), eliminating the IPC overhead of
standard Python UDFs and pandas UDFs. Data is passed to Python as
[PyArrow](https://arrow.apache.org/docs/python/) arrays via the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) — zero-copy
for inputs, one copy for outputs.

**Use `inprocess_udf` when:**
- You are already using `pandas_udf` for vectorized transformations and want lower latency.
- Your UDF operates on Arrow/PyArrow arrays (e.g. using `pyarrow.compute`).
- You can deploy executors with one task per core (see [Requirements](#requirements)).

**Stick with `pandas_udf` or `udf` when:**
- You need pandas Series semantics in your UDF logic.
- You cannot control executor sizing (multi-task-per-executor clusters).
- You are not able to install jep on executors.

---

## Quick Start

### 1. Install dependencies

```bash
pip install "jep>=4.2" pyarrow cloudpickle
```

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

## Requirements

| Requirement | Detail |
|---|---|
| Python | 3.8+ |
| jep | 4.x (`pip install jep`) |
| PyArrow | 12+ |
| cloudpickle | 2.x (already a PySpark dependency) |
| `spark.executor.cores == spark.task.cpus` | Enforced at query planning time (see below) |

### Single task per executor

In-process UDFs use a single `SharedInterpreter` per executor JVM process. Because CPython's
GIL is not re-entrant, **only one task may run at a time per executor**. Spark enforces this
by requiring `spark.executor.cores == spark.task.cpus` whenever an in-process UDF appears in
a query. If this condition is violated, query planning raises:

```
InProcessPythonUDF requires exactly one concurrent task per executor to avoid GIL contention.
Current configuration allows 4 concurrent tasks (spark.executor.cores=4, spark.task.cpus=1).
Set spark.executor.cores == spark.task.cpus, e.g. spark.executor.cores=1.
```

The simplest fix is `spark.executor.cores=1, spark.task.cpus=1`. Total cluster parallelism is
unchanged — you just use more, smaller executors.

---

## Deployment and Distribution

### Local development

For local development (e.g. `SparkSession.builder.master("local[*]")`), install jep and the
required Python packages into the virtual environment you run PySpark from. The venv's
site-packages are already on `sys.path`, so no extra configuration is needed.

```bash
python3 -m venv .venv
.venv/bin/pip install "jep>=4.2" pyarrow cloudpickle pyspark
source .venv/bin/activate
```

You must also make the jep native library discoverable by the JVM:

```bash
# macOS
export DYLD_LIBRARY_PATH="$(python3 -c 'import jep; import os; print(os.path.dirname(jep.__file__))')"

# Linux
export LD_LIBRARY_PATH="$(python3 -c 'import jep; import os; print(os.path.dirname(jep.__file__))')"
```

### Cluster deployment with `--archives`

On a cluster, you typically distribute a pre-built virtual environment to each executor using
Spark's `--archives` feature. This is the recommended approach for YARN and Kubernetes.

**Step 1: Build and zip the venv**

```bash
python3 -m venv myvenv
myvenv/bin/pip install "jep>=4.2" pyarrow cloudpickle my-custom-lib
zip -r myvenv.zip myvenv/
```

**Step 2: Submit with `--archives`**

Spark extracts the zip to a relative path (here `./myvenv/`) on each executor node.

```bash
spark-submit \
  --archives myvenv.zip#myvenv \
  --conf spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin \
  --conf spark.executor.cores=1 \
  --conf spark.task.cpus=1 \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=./myvenv/lib/python3.11/site-packages/jep" \
  --conf spark.inprocess.python.sitePackages=./myvenv/lib/python3.11/site-packages \
  my_app.py
```

Adjust the Python version in the path (`python3.11`) to match the version in your venv.

---

## Configuration Reference

### `spark.plugins`

| Default | `(none)` |
|---|---|
| **Required value** | `org.apache.spark.sql.execution.python.InProcessPythonPlugin` |

Registers the in-process Python plugin. This initializes the `SharedInterpreter` on each
executor at startup. Without this, the interpreter is initialized lazily on the first UDF call
(with no extra `sys.path` configuration applied).

---

### `spark.inprocess.python.sitePackages`

| Default | `(none)` |
|---|---|
| **Type** | Comma-separated list of absolute or relative directory paths |

Paths to append to `sys.path` inside the jep interpreter at executor startup.

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
- Local development: running PySpark from inside the venv already puts site-packages on
  `sys.path` via `PYTHONPATH`.
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

Must satisfy `spark.executor.cores == spark.task.cpus`. The recommended setting for in-process
UDF workloads is:

```
spark.executor.cores = 1
spark.task.cpus      = 1
```

---

## Supported Types

All Spark SQL types are supported as UDF inputs and outputs:

| Category | Types |
|---|---|
| Numeric | `ByteType`, `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType` |
| Boolean | `BooleanType` |
| String / Binary | `StringType`, `BinaryType` |
| Temporal | `DateType`, `TimestampType` |
| Complex | `ArrayType`, `StructType` |

`MapType` is not currently supported.

---

## Choosing Between UDF Types

| | `udf` | `pandas_udf` | `inprocess_udf` |
|---|---|---|---|
| Input type | Python scalar | `pandas.Series` | `pa.Array` |
| Output type | Python scalar | `pandas.Series` | `pa.Array` |
| Data transfer | Pickle, row-by-row | Arrow IPC (process boundary) | Arrow CDI (zero-copy, in-process) |
| Requires jep | No | No | Yes |
| Executor sizing constraint | None | None | 1 task per executor |
| Best for | Simple row transforms | pandas-heavy logic | High-throughput Arrow transforms |

### Related pages

* [Scalar User Defined Functions (UDFs)](sql-ref-functions-udf-scalar.html)
* [PySpark Usage Guide for Pandas with Apache Arrow](sql-pyspark-pandas-with-arrow.html)

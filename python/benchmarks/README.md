# PySpark Benchmarks

This directory contains microbenchmarks for PySpark using [ASV (Airspeed Velocity)](https://asv.readthedocs.io/).

## Prerequisites

Install ASV:

```bash
pip install asv
```

For running benchmarks with isolated environments (without `--python=same`), you need an environment manager.
The default configuration uses `virtualenv`, but ASV also supports `conda`, `mamba`, `uv`, and some others. See the official docs for details.

## Running Benchmarks

All commands below can be run from the Spark root directory using `./python/asv`,
which is a wrapper that forwards arguments to `asv` in the benchmarks directory.

### Quick run (current environment)

Run benchmarks using your current Python environment (fastest for development):

```bash
./python/asv run --python=same --quick
```

You can also specify the test class to run:

```bash
./python/asv run --python=same --quick -b 'bench_arrow.LongArrowToPandasBenchmark'
```

### Full run against a commit

Run benchmarks in an isolated virtualenv (builds pyspark from source):

```bash
./python/asv run master^!          # Run on latest master commit
./python/asv run v3.5.0^!          # Run on a specific tag
./python/asv run abc123^!          # Run on a specific commit
```

### Compare two commits

Compare current branch against upstream/master with 10% threshold:

```bash
./python/asv continuous -f 1.1 upstream/master HEAD
```

### Other useful commands

```bash
./python/asv check          # Validate benchmark syntax
```

## Writing Benchmarks

Benchmarks are Python classes with methods prefixed by:
- `time_*` - Measure execution time
- `peakmem_*` - Measure peak memory usage
- `mem_*` - Measure memory usage of returned object

Example:

```python
class MyBenchmark:
    params = [[1000, 10000], ["option1", "option2"]]
    param_names = ["n_rows", "option"]

    def setup(self, n_rows, option):
        # Called before each benchmark method
        self.data = create_test_data(n_rows, option)

    def time_my_operation(self, n_rows, option):
        # Benchmark timing
        process(self.data)

    def peakmem_my_operation(self, n_rows, option):
        # Benchmark peak memory
        process(self.data)
```

See [ASV documentation](https://asv.readthedocs.io/en/stable/writing_benchmarks.html) for more details.

## In-process Python UDF benchmark

`bench_inprocess_udf.InProcessUDFTimeBench` runs real Spark queries comparing
in-process Arrow UDFs with worker Arrow UDFs (the primary baseline) and pandas
UDFs (a supplementary baseline). Both Arrow modes accept and return PyArrow arrays
and execute the same Arrow operations. It covers narrow/wide integer inputs,
short string uppercase, and 1000-character string identity. Input construction,
cache materialization and two warmup queries are outside timing. Each sample
executes one query to a noop sink; normal runs request five samples.

Use a Spark assembly built from the same checkout as the Python source. For example,
`build/sbt -Phive package` builds Spark with Hive support. This benchmark requires
the in-process UDF implementation in the JVM as well as Python; ASV's wheel build
alone does not ensure that the Spark JARs match a selected commit. Start with
`--python=same` and rebuild Spark whenever switching source versions.

Activate a Python environment containing `asv`, `jep>=4.3.1`, `pyarrow`, `pandas` and
`cloudpickle`. JEP must be built for that Python installation and the selected JDK.
Install these dependencies in a venv (for example, `python -m venv .venv` and
`source .venv/bin/activate`). From the Spark checkout root, configure the driver
before ASV launches any JVM. Set `ARROW_C_DATA_JAR` to an external `arrow-c-data`
JAR matching the Arrow Java version in the Spark build; it is a provided dependency
and is not included in the assembly:

```bash
export SPARK_HOME="$PWD"
export ARROW_C_DATA_JAR=/absolute/path/to/arrow-c-data.jar
test -f "$ARROW_C_DATA_JAR" || exit 1
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
JEP_DIR="$(python -c 'import importlib.util; print(next(iter(importlib.util.find_spec("jep").submodule_search_locations)))')"
JEP_JARS=("$JEP_DIR"/jep-*.jar)
export PYTHONPATH="$(dirname "$JEP_DIR")${PYTHONPATH:+:$PYTHONPATH}"
PY4J_ZIPS=("$SPARK_HOME"/python/lib/py4j-*-src.zip)
export ASV_PYTHONPATH="$SPARK_HOME/python:${PY4J_ZIPS[0]}:$PYTHONPATH"
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g --driver-class-path ${JEP_JARS[0]}:$ARROW_C_DATA_JAR --driver-java-options \"-Djava.library.path=$JEP_DIR -XX:MaxDirectMemorySize=8g\" pyspark-shell"
./python/asv run --python=same --launch-method=spawn --quick --dry-run --show-stderr \
  -b 'bench_inprocess_udf.InProcessUDFTimeBench'
```

For recorded measurements, first commit the benchmark and build the matching
Spark revision. Remove `--quick --dry-run` and add
`--set-commit-hash "$(git rev-parse HEAD)" --record-samples` to label and save
results from the existing environment. The quick run is a smoke check, not a
performance result. An absent JEP package skips in-process cases;
JEP loading errors fail the benchmark rather than silently falling back.

All three modes use `local[1]`, one input partition, worker reuse, and a 128 MiB Arrow
byte limit. Row limits are 10K for narrow integers, 1M for wide integers, and 100K
for strings. Thus the long-string workload permits about 95 MiB of string payload
per full batch instead of splitting it at the default 64 MiB limit. Budget for
8 GiB heap, 8 GiB direct memory, and additional Python/native allocations.

Report dependency versions and the Spark commit with results. Compute the primary
speedup as worker Arrow UDF median divided by in-process UDF median. This removes
the pandas conversion difference, but still measures the complete execution paths,
including serialization and framework overhead, rather than IPC alone. The pandas baseline
includes pandas/Arrow conversion costs, so the ratio is not an isolated measurement
of IPC savings. ASV's process/setup lifecycle differs from the historical script;
its results establish a new baseline. The original standalone scripts were removed;
their earlier versions remain available in Git history for historical comparisons.
No relative-speed pass/fail threshold is imposed by this benchmark.

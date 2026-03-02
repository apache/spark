# Implementation Plan: In-Process Python UDFs

---

## Phase 1: Core API & Execution Path

Most of this phase is already implemented. Two known gaps remain.

**Working items:**

- [x] Add non-deterministic UDF flag support: expose `deterministic=False` parameter in `@inprocess_udf` decorator, propagate it through `InProcessPythonUDF` Catalyst expression so the optimizer does not reorder or deduplicate non-deterministic UDF calls.
- [x] Extend integration test coverage to non-primitive Arrow types: add test cases for `StringType`, `BinaryType`, `TimestampType`, `DateType`, `ArrayType`, `StructType` (CDI is type-agnostic so these should work, but no tests confirm correctness and null handling today). `MapType` deferred — less commonly used with transformational UDFs.

---

## Phase 2: Deployment & Distribution

**Working items:**

- [x] Implement `spark.inprocess.python.sitePackages` config in `InProcessPythonRuntime`: after the `SharedInterpreter` is created, read this config and append the specified path(s) to `sys.path` inside the interpreter so the distributed venv's packages are importable without requiring users to set `PYTHONPATH` manually.

  **When to use:** This config is needed when you distribute a Python virtual environment to executors via `--archives` and need packages from that venv to be importable inside UDFs. Typical flow:

  1. Build a venv with your dependencies (`jep`, `pyarrow`, `cloudpickle`, and any custom libraries) and zip it.
  2. Ship it to each executor via `spark-submit --archives myvenv.zip#myvenv`.
  3. Set the config to point to the unpacked site-packages directory:
     ```
     spark.inprocess.python.sitePackages = ./myvenv/lib/python3.11/site-packages
     ```
     At executor startup, `InProcessPythonPlugin` appends this path to `sys.path` inside the jep interpreter, so `import my_custom_lib` works in every UDF.

  **When you do NOT need it:** Local development (running from within the venv already puts site-packages on `sys.path` via `PYTHONPATH`), or when executors already have all required packages on the system Python path.
- [x] Document `java.library.path` setup: `java.library.path` is read by the JVM at startup and **cannot be changed at runtime** (`System.setProperty()` has no effect after startup; `System.load(absolutePath)` exists but is fragile with jep's static initializer). The practical solution is `spark.executor.extraJavaOptions=-Djava.library.path=./venv/lib/...` which is stable when using `--archives` because the venv is extracted to a predictable relative path. Documented in the `spark.executor.extraJavaOptions` section of `docs/sql-pyspark-inprocess-udf.md`.
- [x] Write cluster manager integration guides for YARN and Kubernetes: end-to-end setup showing how to build and distribute the Python venv containing jep + pyarrow + cloudpickle using `--archives`, and how to configure `spark.executorEnv.PYSPARK_PYTHON` and `spark.executor.extraJavaOptions`. Added YARN and Kubernetes (Docker image + `--archives`) sections to `docs/sql-pyspark-inprocess-udf.md`.

---

## Phase 3: Error Handling

**Working items:**

- [x] Extract full Python traceback from `JepException`: `JepException` has no `getPythonErrorMessage()` API. Instead, the Python bridge (`runtime.py`) now catches exceptions from user UDF code, formats the full traceback with `traceback.format_exc()`, and re-raises as `RuntimeError` with a sentinel prefix (`__INPROCESS_UDF_TRACEBACK__:`). The full traceback (file, line, function name, exception type+message) is embedded in the `JepException.getMessage()` string that the Scala side receives.
- [x] Throw `PythonException` instead of `RuntimeException` in `InProcessPythonRuntime.invoke`: the catch block now detects the sentinel in `JepException.getMessage()`. If found, it extracts the traceback string and throws `PythonException(errorClass="PYTHON_EXCEPTION", ...)` — the same error class used by `pandas_udf` failures, which renders in the Spark UI Task Error tab. If not found (no sentinel = infrastructure error), it throws `RuntimeException` with "In-process Python infrastructure error: ..." message.
- [x] Categorize errors: UDF logic errors (user code raises `TypeError`, `ValueError`, etc.) carry the sentinel and are thrown as `PythonException`. Infrastructure errors (CDI import failure, cloudpickle deserialization failure, interpreter not initialized) propagate as plain `JepException` without the sentinel and are thrown as `RuntimeException`.
- [x] Add unit tests: `test_buggy_udf_exposes_python_traceback` in `test_inprocess_udf.py` verifies that a UDF raising `ValueError("intentional test error")` produces a task failure whose message includes `ValueError`, the error text, and the UDF function name.

---

## Phase 4: Testing & CI

**Working items:**

- [ ] Unit tests for plan shape: verify that `ExtractInProcessPythonUDFs` produces `InProcessEvalPython` logical nodes and that `InProcessArrowEvalExec` appears in the physical plan.
- [ ] Unit tests for config validation: verify that `InProcessPythonChecks` throws a clear `IllegalArgumentException` when `spark.executor.cores != spark.task.cpus`, and passes when they are equal.
- [ ] Integration tests across all Arrow types: correctness and null-handling tests for each type group (numeric primitives, string/binary, temporal, nested).
- [ ] Performance regression benchmark suite: formalize `benchmark_inprocess_udf.py` as a CI benchmark with noop sink (Scenarios A, B, D, E). Alert if in-process UDF time exceeds 1.2× baseline on any scenario.
- [ ] CI pipeline integration: add the integration test suite to Spark's GitHub Actions / Jenkins CI so in-process UDF tests run on every PR that touches the relevant paths.

---

## Phase 5: Documentation

**Working items:**

- [ ] User guide: explain the `@inprocess_udf` decorator, show examples for numeric, string, multi-column, and closure-capture UDFs, and describe when to choose `inprocess_udf` vs `pandas_udf` vs `udf`.
- [ ] Configuration reference: document all `spark.inprocess.*` configs, required executor sizing (`spark.executor.cores=1`), `spark.plugins` registration, `--archives` venv distribution, and `java.library.path` setup.
- [ ] Migration guide from `pandas_udf`: side-by-side examples showing how to rewrite a `pandas_udf` as an `inprocess_udf`, with notes on input type change (pandas Series → `pa.Array`) and when migration is beneficial.

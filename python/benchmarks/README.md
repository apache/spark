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

### Quick run (current environment)

Run benchmarks using your current Python environment (fastest for development):

```bash
cd python/benchmarks
asv run --python=same --quick
```

### Full run against a commit

Run benchmarks in an isolated virtualenv (builds pyspark from source):

```bash
cd python/benchmarks
asv run master^!          # Run on latest master commit
asv run v3.5.0^!          # Run on a specific tag
asv run abc123^!          # Run on a specific commit
```

### Compare two commits

Compare current branch against upstream/main with 10% threshold:

```bash
asv continuous -f 1.1 upstream/main HEAD
```

### Other useful commands

```bash
asv check          # Validate benchmark syntax
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

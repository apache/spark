# Spark CLI

This is the unified entry point for all of Spark's command line functionality. It's available under `bin/spark` (for Unix-like systems) and `bin/spark.cmd` (for Windows). Installers should place these executables on the user's `PATH`.

The CLI offers various commands that currently dispatch to the scripts under `bin/` and `sbin/`. In the future these scripts may be folded directly into this CLI. The CLI itself is implemented in Python and sticks to the standard library. It supports the same versions of Python as rest of Spark.

Note that although the CLI is written in Python, it's not part of PySpark or specific to the Python distributions of Spark available on PyPI. It's a common CLI intended for all users of the full distribution of Spark.

## Testing

Tests are instrumented as part of `dev/run-tests`. You can run them as follows:

```
SKIP_SCALA_BUILD=true SKIP_MIMA=true dev/run-tests -m cli
```

You can also run them directly with:

```
python3 -m unittest cli.tests.test_spark_cli
```

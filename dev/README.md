# Spark Developer Scripts

This directory contains scripts useful to developers when packaging,
testing, or committing to Spark.

Many of these scripts require Apache credentials to work correctly.

## Testing `sparktestsupport`

The tests in `sparktestsupport/tests/`, which include the doctests of every `sparktestsupport`
module, run automatically in CI and in `dev/run-tests`. To run them directly:

```sh
python3 -m unittest discover -s dev/sparktestsupport/tests -t dev
```

Keep short usage examples as doctests. Put regression cases, module-graph invariants, and tests
requiring setup or mocks in the test files.

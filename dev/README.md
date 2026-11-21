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

## Testing the merge script

`dev/merge_spark_pr.py` runs its tests before a merge and stops if they fail: its doctests,
then the unit tests in `prmerge/tests/`. To run the unit tests directly:

```sh
python3 -m unittest discover -s dev/prmerge/tests -t dev
```

Keep short examples as doctests on the functions. Put cases that need mocks or setup
in `prmerge/tests/`.

README
======

Please do not add any class in this place unless it is used by `sql/console` or Python tests.
If you need to create any classes or traits that will be used by tests from both `sql/core` and
`sql/hive`, you can add them in the `src/test` of `sql/core` (tests of `sql/hive`
depend on the test jar of `sql/core`).

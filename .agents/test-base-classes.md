# Test Base Classes

## Scala Test Base Classes

When writing a new Scala test suite, pick the lowest base class that provides what the test actually needs. Spark uses the `AnyFunSuite` ScalaTest style throughout, so the bases below are the chain to choose from. Each adds capability on top of the previous:

    SparkFunSuite                                                           (core)
      <- PlanTest                                                           (sql/catalyst)
        <- QueryTest                                                        (sql/core)

| Test scope | Base | Notes |
|------------|------|-------|
| Plain JVM/Scala — no Spark SQL | `SparkFunSuite` | `core` utilities, RDD, network, util classes, etc. Adds per-test timeout, `testRetry`, `gridTest`, thread audit, fixed timezone/locale, `withTempDir`, `withLogAppender`, `checkError`. |
| Catalyst plan tests — no `SparkSession` | `PlanTest` | Adds `comparePlans`, `normalizePlan`, `normalizeExprIds`. For analyzer / optimizer / planner rule tests. |
| SQL/DataFrame tests — needs a `SparkSession` | `QueryTest` | Adds `checkAnswer`, codegen-on/off helpers. `spark: SparkSession` is abstract and must be supplied by a session-providing trait (see below). |

### Providing a `SparkSession` for `QueryTest`

`QueryTest` declares `spark: SparkSession` abstractly via `SparkSessionProvider`, so it cannot be instantiated on its own. A concrete suite mixes in one of the session-providing traits below:

    QueryTest                                                               (abstract `spark`)
      + SharedSparkSession (sql/core)        -> classic in-process `TestSparkSession`
      + TestHiveSingleton  (sql/hive)        -> Hive-backed `TestHive` session

| Session provider | Module / location | Typical usage |
|---|---|---|
| `SharedSparkSession` | `sql/core` | Already extends `QueryTest` for historical reasons, but still mix in `QueryTest` explicitly, e.g. `class X extends QueryTest with SharedSparkSession`. Default for tests under `sql/core`. |
| `TestHiveSingleton` | `sql/hive` | Mixed in alongside `QueryTest`, e.g. `class X extends QueryTest with TestHiveSingleton`. Used by tests under `sql/hive`. |

## Python Test Base Classes

PySpark tests use the stdlib `unittest` framework: every suite subclasses `unittest.TestCase` (run via `python/run-tests`, see `.agents/build-and-test.md`). As with Scala, pick the lowest base that provides what the test actually needs. The bases live under `python/pyspark/testing/` and each adds capability on top of the previous:

    unittest.TestCase                                     (stdlib)
      <- PySparkBaseTestCase                              (pyspark.testing.utils)
        <- ReusedPySparkTestCase                          (pyspark.testing.utils)
          <- ReusedSQLTestCase                            (pyspark.testing.sqlutils)
            <- PandasOnSparkTestCase                      (pyspark.testing.pandasutils)

| Test scope | Base | Notes |
|------------|------|-------|
| Plain Python — no Spark | `unittest.TestCase` | Use the stdlib base directly. `PySparkBaseTestCase` is the same thing plus a SIGTERM fault-handler dump enabled when `PYSPARK_TEST_TIMEOUT` is set; subclass it only when you want that. |
| RDD / `SparkContext` — no `SparkSession` | `PySparkTestCase` (fresh) or `ReusedPySparkTestCase` (shared) | Both create a `SparkContext("local[4]")`. `PySparkTestCase` makes a new one per test (isolation); `ReusedPySparkTestCase` shares one per class (faster, the usual choice) and adds `quiet()` and an overridable `conf()` / `master()`. |
| SQL / DataFrame — needs a `SparkSession` | `ReusedSQLTestCase` | The workhorse for classic tests under `python/pyspark/sql`. Adds a shared `cls.spark`, sample `cls.df` / `cls.testData`, and mixes in `SQLTestUtils` + `PySparkErrorTestUtils`. Classic (non-Connect) mode. |
| pandas API on Spark | `PandasOnSparkTestCase` | Extends `ReusedSQLTestCase` with Arrow enabled and pandas-on-Spark assertions (`PandasOnSparkTestUtils`); `ComparisonTestBase` builds on it. |

### Spark Connect test bases

Spark Connect suites live in `pyspark.testing.connectutils` and are auto-skipped (via `should_test_connect`) when Connect dependencies are missing:

    PySparkBaseTestCase                                   (pyspark.testing.utils)
      <- PlanOnlyTestFixture                              (pyspark.testing.connectutils)
      <- ReusedConnectTestCase                            (pyspark.testing.connectutils)
           <- ReusedMixedTestCase                         (pyspark.testing.connectutils)

| Test scope | Base | Notes |
|------------|------|-------|
| Plan / proto construction — no server | `PlanOnlyTestFixture` | Uses a `MockRemoteSession`; builds and inspects plans without a running Connect server. For proto / plan-shape assertions. |
| Connect DataFrame — real session | `ReusedConnectTestCase` | The Connect analog of `ReusedSQLTestCase`; starts a session via `.remote(...)` (honoring `SPARK_CONNECT_TESTING_REMOTE`, default `local[4]`). Mixes in `SQLTestUtils` + `PySparkErrorTestUtils`. |
| Classic + Connect side by side | `ReusedMixedTestCase` | Extends `ReusedConnectTestCase`. For directly comparing classic vs Connect: it exposes a classic `self.spark` and a Connect `self.connect` so a test can run the same operation on each and assert they agree (`compare_by_show`, `both_conf`). Requires JVM access. |

### Mixins and helpers

These are combined with a base above rather than used on their own:

- `SQLTestUtils` — context managers `sql_conf`, `table`, `temp_view`, `view`, `database`, `function`, `temp_func`, `temp_env`; assumes `self.spark`. Already mixed into `ReusedSQLTestCase` and `ReusedConnectTestCase`.
- `PySparkErrorTestUtils` — `check_error(...)` to assert on a `PySparkException`'s error class and message parameters. The Python counterpart of Scala's `checkError`.
- `assertDataFrameEqual` / `assertSchemaEqual` (public API, from `pyspark.testing`) — standalone assertion functions that work in any test, no particular base class required.
- Domain bases outside the main ladder: `SparkSessionTestCase` (`pyspark.testing.mlutils`, for `ml`), `MLlibTestCase` (`pyspark.testing.mllibutils`), and `PySparkStreamingTestCase` (`pyspark.testing.streamingutils`, DStreams).

package catalyst
package execution

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveQueryTests extends HiveComparisonTest {
  import TestShark._

  createQueryTest("Simple Average",
    "SELECT AVG(key) FROM src")

  createQueryTest("Simple Average + 1",
    "SELECT AVG(key) + 1.0 FROM src")

  createQueryTest("Simple Average + 1 with group",
    "SELECT AVG(key) + 1.0, value FROM src group by value")

  createQueryTest("string literal",
    "SELECT 'test' FROM src")

  createQueryTest("Escape sequences",
    """SELECT key, '\\\t\\' FROM src WHERE key = 86""")

  createQueryTest("IgnoreExplain",
    """EXPLAIN SELECT key FROM src""")

  createQueryTest("trival join where clause",
    "SELECT * FROM src a JOIN src b WHERE a.key = b.key")

  createQueryTest("trival join ON clause",
    "SELECT * FROM src a JOIN src b ON a.key = b.key")

  createQueryTest("small.cartesian",
    "SELECT a.key, b.key FROM (SELECT key FROM src WHERE key < 1) a JOIN (SELECT key FROM src WHERE key = 2) b")

  createQueryTest("length.udf",
    "SELECT length(\"test\") FROM src LIMIT 1")


  ignore("partitioned table scan") {
    createQueryTest("partitioned table scan",
      "SELECT ds, hr, key, value FROM srcpart")
  }
  createQueryTest("hash",
    "SELECT hash('test') FROM src LIMIT 1")

  createQueryTest("create table as",
    """
      |CREATE TABLE createdtable AS SELECT * FROM src;
      |SELECT * FROM createdtable
    """.stripMargin)

  createQueryTest("transform",
    "SELECT TRANSFORM (key) USING 'cat' AS (tKey) FROM src")

  createQueryTest("LIKE",
    "SELECT * FROM src WHERE value LIKE '%1%'")

  createQueryTest("DISTINCT",
    "SELECT DISTINCT key, value FROM src")

  ignore("empty aggregate input") {
    createQueryTest("empty aggregate input",
      "SELECT SUM(key) FROM (SELECT * FROM src LIMIT 0) a")
  }
}
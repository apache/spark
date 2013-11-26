package catalyst
package shark2

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveQueryTests extends HiveComaparisionTest {
  import TestShark._

  createQueryTest("Simple Average",
    "SELECT AVG(key) FROM src")

  createQueryTest("string literal",
    "SELECT 'test' FROM src")

  test("Run random sample") { // Since this is non-deterministic we just check to make sure it runs for now.
    "SELECT key, value FROM src WHERE RAND() > 0.5 ORDER BY RAND()".q.execute().get.collect()
  }

  createQueryTest("Escape sequences",
    """SELECT key, '\\\t\\' FROM src WHERE key = 86""")
}
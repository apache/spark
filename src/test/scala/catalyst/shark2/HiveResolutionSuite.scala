package catalyst
package shark2

/**
 * A set of test cases expressed in Hive QL that are not covered by the tests included in the hive distribution.
 */
class HiveResolutionSuite extends HiveComaparisionTest {
  import TestShark._

  createQueryTest("table.attr",
    "SELECT src.key FROM src ORDER BY key LIMIT 1")

  createQueryTest("database.table",
    "SELECT key FROM default.src ORDER BY key LIMIT 1")

  createQueryTest("database.table table.attr",
    "SELECT src.key FROM default.src ORDER BY key LIMIT 1")

  createQueryTest("alias.attr",
    "SELECT a.key FROM src a ORDER BY key LIMIT 1")

  createQueryTest("subquery-alias.attr",
    "SELECT a.key FROM (SELECT * FROM src ORDER BY key LIMIT 1) a")

  createQueryTest("quoted alias.attr",
    "SELECT `a`.`key` FROM src a ORDER BY key LIMIT 1")

  createQueryTest("attr",
    "SELECT key FROM src a ORDER BY key LIMIT 1")

  createQueryTest("alias.*",
    "SELECT a.* FROM src a ORDER BY key LIMIT 1")

  /**
   * Negative examples.  Currently only left here for documentation purposes.
   * TODO(marmbrus): Test that catalyst fails on these queries.
   */

  /* SemanticException [Error 10009]: Line 1:7 Invalid table alias 'src'
  createQueryTest("table.*",
    "SELECT src.* FROM src a ORDER BY key LIMIT 1") */

  /* Invalid table alias or column reference 'src': (possible column names are: key, value)
  createQueryTest("tableName.attr from aliased subquery",
    "SELECT src.key FROM (SELECT * FROM src ORDER BY key LIMIT 1) a") */

}
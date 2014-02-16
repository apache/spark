package org.apache.spark.sql
package shark
package execution

/**
 * A set of tests that validates support for Hive SerDe.
 */
class HiveSerDeSuite extends HiveComparisonTest {
  createQueryTest(
    "Read and write with LazySimpleSerDe (tab separated)",
    "SELECT * from serdeins")

  createQueryTest("Read with RegexSerDe", "SELECT * FROM sales")

  createQueryTest("Read with AvroSerDe", "SELECT * FROM episodes")
}

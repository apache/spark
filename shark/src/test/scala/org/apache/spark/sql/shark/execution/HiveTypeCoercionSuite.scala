package org.apache.spark.sql.shark.execution

/**
 * A set of tests that validate type promotion rules.
 */
class HiveTypeCoercionSuite extends HiveComparisonTest {

  val baseTypes = Seq("1", "1.0", "1L", "1S", "1Y", "'1'")

  baseTypes.foreach { i =>
    baseTypes.foreach { j =>
      createQueryTest(s"$i + $j", s"SELECT $i + $j FROM src LIMIT 1")
    }
  }
}
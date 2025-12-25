// Simplified test case for view function resolution issue
import org.apache.spark.sql.SparkSession

object TestViewFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestViewFunction")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    println("=== Step 1: Create permanent function in default database ===")
    spark.sql("USE DEFAULT")
    spark.sql("CREATE FUNCTION test_udf AS 'test.org.apache.spark.sql.MyDoubleAvg'")
    println("Created permanent function default.test_udf (MyDoubleAvg)")

    println("\n=== Step 2: Create view using test_udf ===")
    spark.sql("CREATE VIEW v1 AS SELECT test_udf(col1) AS func FROM VALUES (1), (2), (3) t(col1)")
    println("Created view v1")

    println("\n=== Step 3: Query view (should use MyDoubleAvg) ===")
    val result1 = spark.sql("SELECT * FROM v1").collect()
    println(s"Result before temp function: ${result1.mkString(", ")}")

    println("\n=== Step 4: Create temporary function test_udf ===")
    spark.sql("CREATE TEMPORARY FUNCTION test_udf AS 'test.org.apache.spark.sql.MyDoubleSum'")
    println("Created temporary function test_udf (MyDoubleSum)")

    println("\n=== Step 5: Query view again (should STILL use MyDoubleAvg, not MyDoubleSum) ===")
    val result2 = spark.sql("SELECT * FROM v1").collect()
    println(s"Result after temp function: ${result2.mkString(", ")}")

    // Clean up
    spark.sql("DROP VIEW v1")
    spark.sql("DROP TEMPORARY FUNCTION test_udf")
    spark.sql("DROP FUNCTION test_udf")

    spark.stop()
  }
}

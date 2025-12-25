// Simple test to check if persistent function is created and found
val spark = org.apache.spark.sql.SparkSession.builder().master("local[1]").getOrCreate()

println("Step 1: Create permanent function")
try {
  spark.sql("CREATE FUNCTION test_simple AS 'test.org.apache.spark.sql.MyDoubleAvg'")
  println("  SUCCESS: Function created")
} catch {
  case e: Exception => println(s"  FAILED: ${e.getMessage}")
}

println("\nStep 2: Check if function exists in metastore")
val catalog = spark.sessionState.catalog
val funcIdent = org.apache.spark.sql.catalyst.FunctionIdentifier("test_simple", Some("default"))
val exists = catalog.externalCatalog.functionExists("default", "test_simple")
println(s"  Exists in metastore: $exists")

println("\nStep 3: Try to use function")
try {
  spark.sql("SELECT test_simple(1.0)").collect()
  println("  SUCCESS: Function works")
} catch {
  case e: Exception => println(s"  FAILED: ${e.getMessage}")
}

println("\nStep 4: Clean up")
spark.sql("DROP FUNCTION IF EXISTS test_simple")

spark.stop()

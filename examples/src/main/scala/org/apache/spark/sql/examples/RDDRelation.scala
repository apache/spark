package org.apache.spark.sql.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SqlContext

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "RDDRelation")
    val sqlContext = new SqlContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._

    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerAsTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sql("SELECT COUNT(*) FROM records").collect().head.getInt(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect.foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    rdd.where('key === 1).orderBy('value.asc).select('key).toRdd.collect().foreach(println)

    // Write out an RDD as a parquet file.
    rdd.writeToFile("pair.parquet")

    /**
     * Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
     *
     * WIP: The following fails.
     * [error] Exception in thread "main" parquet.io.ParquetEncodingException: pair.parquet invalid: all the files must be contained in the root file:/Users/marmbrus/workspace/spark/examples/pair.parquet
     * [error] 	at parquet.hadoop.ParquetFileWriter.mergeFooters(ParquetFileWriter.java:354)
     * [error] 	at parquet.hadoop.ParquetFileWriter.writeMetadataFile(ParquetFileWriter.java:342)
     * [error] 	at org.apache.spark.sql.parquet.ParquetTypesConverter$.writeMetaData(ParquetRelation.scala:265)
     * [error] 	at org.apache.spark.sql.parquet.ParquetRelation$.create(ParquetRelation.scala:149)
     * [error] 	at org.apache.spark.sql.execution.SparkStrategies$BasicOperators$.apply(SparkStrategies.scala:213)
     * [error] 	at org.apache.spark.sql.catalyst.planning.QueryPlanner$$anonfun$1.apply(QueryPlanner.scala:60)
     * [error] 	at org.apache.spark.sql.catalyst.planning.QueryPlanner$$anonfun$1.apply(QueryPlanner.scala:60)
     * [error] 	at scala.collection.Iterator$$anon$13.hasNext(Iterator.scala:371)
     * [error] 	at org.apache.spark.sql.catalyst.planning.QueryPlanner.apply(QueryPlanner.scala:61)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.sparkPlan$lzycompute(SparkSqlContext.scala:155)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.sparkPlan(SparkSqlContext.scala:155)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.executedPlan$lzycompute(SparkSqlContext.scala:156)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.executedPlan(SparkSqlContext.scala:156)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.toRdd$lzycompute(SparkSqlContext.scala:159)
     * [error] 	at org.apache.spark.sql.SqlContext$QueryExecution.toRdd(SparkSqlContext.scala:159)
     * [error] 	at org.apache.spark.sql.SqlContext$TableRdd.writeToFile(SparkSqlContext.scala:96)
     * [error] 	at org.apache.spark.sql.examples.RDDRelation$.main(RDDRelation.scala:41)
     * [error] 	at org.apache.spark.sql.examples.RDDRelation.main(RDDRelation.scala)
     */
    val parquetFile = sqlContext.loadFile("pair.parquet")

    // Queries can be run using the DSL on parequet files just like the original RDD.
    parquetFile.where('key === 1).select('value).toRdd.collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerAsTable("parquetFile")
    sql("SELECT * FROM parquetFile").collect().foreach(println)


  }
}
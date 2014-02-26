
package org.apache.spark.sql.execution
package shark

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.shark.TestShark

class ParquetQuerySuite extends FunSuite with BeforeAndAfterAll {

  def runQuery(querystr: String): Array[Row] = {
    TestShark
      .sql(querystr)
      .rdd
      .collect()
  }

  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    ParquetTestData.writeFile

    // Override initial Parquet test table
    TestShark.catalog.overrideTable(Some[String]("parquet"), "testsource", ParquetTestData.testData)
  }

  override def afterAll() {
    ParquetTestData.testFile.delete()
  }

  test("SELECT on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM parquet.testsource")
    assert(rdd != null)
    assert(rdd.forall(_.size == 2))
  }

  test("Simple column projection on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM parquet.testsource WHERE myboolean=true")
    assert(rdd.size === 5)
    assert(rdd.forall(_.getBoolean(0)))
  }

  // TODO: It seems that "CREATE TABLE" is passed directly to Hive as a NativeCommand, which
  // makes this test fail. One should come up with a more permanent solution first.
  /*test("CREATE Parquet table") {
    val result = runQuery("CREATE TABLE IF NOT EXISTS parquet.tmptable (key INT, value STRING)")
    assert(result != null)
  }*/

  test("CREATE TABLE AS Parquet table") {
    runQuery("CREATE TABLE parquet.testdest AS SELECT * FROM src")
    val rddCopy = runQuery("SELECT * FROM parquet.testdest").sortBy(_.getInt(0))
    val rddOrig = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    val allsame = (rddCopy, rddOrig).zipped.forall { (a,b) => (a,b).zipped.forall { (x,y) => x==y}}
    assert(allsame)
  }

  test("INSERT OVERWRITE to Parquet table") {
    runQuery("CREATE TABLE parquet.testdest AS SELECT * FROM src")
    runQuery("INSERT OVERWRITE TABLE parquet.testdest SELECT * FROM src")
    val rddCopy = runQuery("SELECT * FROM parquet.testdest").sortBy(_.getInt(0))
    val rddOrig = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    val allsame = (rddCopy, rddOrig).zipped.forall { (a,b) => (a,b).zipped.forall { (x,y) => x==y } }
    assert(allsame)
  }
}



package org.apache.spark.sql
package shark

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.ParquetTestData

class ParquetQuerySuite extends FunSuite with BeforeAndAfterAll {

  def runQuery(querystr: String): Array[Row] = {
    TestShark.sql(querystr).rdd.collect()
  }

  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    org.apache.spark.sql.ParquetTestData.testFile.delete()
    ParquetTestData.writeFile
    val testRel = ParquetTestData.testData
    TestShark.catalog.overrideTable(Some[String]("default"), "testtable", testRel)
  }

  override def afterAll() {
    ParquetTestData.testFile.delete()
  }

  test("SELECT on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM default.testtable")
    assert(rdd != null)
    assert(rdd.forall(_.size == 2))
  }

  test("Filter on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM default.testtable WHERE myboolean=true")
    assert(rdd.size === 5)
    assert(rdd.forall(_.getBoolean(0)))
  }

  // TODO: fix insert into table
  /*test("INSERT OVERWRITE Parquet table") {
    val rdd = runQuery("INSERT OVERWRITE TABLE default.testtable SELECT * FROM default.testtable")
    assert(rdd != null)
  }*/
}


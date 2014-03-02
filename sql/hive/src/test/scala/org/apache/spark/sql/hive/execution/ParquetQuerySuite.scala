
package org.apache.spark.sql
package hive
package execution

import java.io.File

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.WriteToFile
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoCreatedTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.{ParquetTestData, ParquetRelation}

class ParquetQuerySuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val filename = "file:///tmp/parquettest"

  // runs a SQL and optionally resolves one Parquet table
  def runQuery(querystr: String, tableName: Option[String] = None, filename: Option[String] = None): Array[Row] = {
    // call to resolve references in order to get CREATE TABLE AS to work
    val query = TestHive
      .parseSql(querystr)
    val finalQuery =
      if(tableName.nonEmpty && filename.nonEmpty)
        resolveParquetTable(tableName.get, filename.get, query)
      else
        query
    TestHive.executePlan(finalQuery)
      .toRdd
      .collect()
  }

  // stores a query output to a Parquet file
  def storeQuery(querystr: String, filename: String): Unit = {
    val query = WriteToFile(
      filename,
      TestHive.parseSql(querystr),
      Some("testtable"))
    TestHive
      .executePlan(query)
      .stringResult()
  }

  /**
   * TODO: This function is necessary as long as there is no notion of a Catalog for
   * Parquet tables. Once such a thing exists this functionality should be moved there.
   */
  def resolveParquetTable(tableName: String, filename: String, plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case relation @ UnresolvedRelation(databaseName, name, alias) =>
        if(name == tableName)
          ParquetRelation(tableName, filename)
        else
          relation
      case op @ InsertIntoCreatedTable(databaseName, name, child) =>
        if(name == tableName) {
          // note: at this stage the plan is not yet analyzed but Parquet needs to know the schema
          // and for that we need the child to be resolved
          TestHive.loadTestTable("src") // may not be loaded now
          val relation = ParquetRelation.create(
              filename,
              TestHive.analyzer(child),
              TestHive.sparkContext.hadoopConfiguration,
              Some(tableName))
          InsertIntoTable(
            relation.asInstanceOf[BaseRelation],
            Map.empty,
            child,
            overwrite = false)
        } else
          op
    }
  }

  override def beforeAll() {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    // write test data
    ParquetTestData.writeFile
    // Override initial Parquet test table
    TestHive.catalog.overrideTable(Some[String]("parquet"), "testsource", ParquetTestData.testData)
  }

  override def afterAll() {
    ParquetTestData.testFile.delete()
  }

  override def beforeEach() {
    (new File(filename)).getAbsoluteFile.delete()
  }

  override def afterEach() {
    (new File(filename)).getAbsoluteFile.delete()
  }

  test("SELECT on Parquet table") {
    val rdd = runQuery("SELECT * FROM parquet.testsource")
    assert(rdd != null)
    assert(rdd.forall(_.size == 6))
  }

  test("Simple column projection + filter on Parquet table") {
    val rdd = runQuery("SELECT myboolean, mylong FROM parquet.testsource WHERE myboolean=true")
    assert(rdd.size === 5)
    assert(rdd.forall(_.getBoolean(0)))
  }

  test("Converting Hive to Parquet Table via WriteToFile") {
    storeQuery("SELECT * FROM src", filename)
    val rddOne = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    val rddTwo = runQuery("SELECT * from ptable", Some("ptable"), Some(filename)).sortBy(_.getInt(0))
    val allsame = (rddOne, rddTwo).zipped.forall { (a,b) => (a,b).zipped.forall { (x,y) => x==y}}
    assert(allsame)
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    storeQuery("SELECT * FROM parquet.testsource", filename)
    runQuery("INSERT OVERWRITE TABLE ptable SELECT * FROM parquet.testsource", Some("ptable"), Some(filename))
    runQuery("INSERT OVERWRITE TABLE ptable SELECT * FROM parquet.testsource", Some("ptable"), Some(filename))
    val rddCopy = runQuery("SELECT * FROM ptable", Some("ptable"), Some(filename))
    val rddOrig = runQuery("SELECT * FROM parquet.testsource")
    val allsame = (rddCopy, rddOrig).zipped.forall { (a,b) => (a,b).zipped.forall { (x,y) => x==y } }
    assert(allsame)
  }

  test("CREATE TABLE AS Parquet table") {
    runQuery("CREATE TABLE ptable AS SELECT * FROM src", Some("ptable"), Some(filename))
    val rddCopy = runQuery("SELECT * FROM ptable", Some("ptable"), Some(filename)).sortBy(_.getInt(0))
    val rddOrig = runQuery("SELECT * FROM src").sortBy(_.getInt(0))
    val allsame = (rddCopy, rddOrig).zipped.forall { (a,b) => (a,b).zipped.forall { (x,y) => x==y } }
    assert(allsame)
  }
}

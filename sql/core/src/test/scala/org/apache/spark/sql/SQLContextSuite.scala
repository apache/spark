/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

@deprecated("This suite is deprecated to silent compiler deprecation warnings", "2.0.0")
class SQLContextSuite extends SparkFunSuite with SharedSparkContext {

  object DummyRule extends Rule[LogicalPlan] {
    def apply(p: LogicalPlan): LogicalPlan = p
  }

  test("getOrCreate instantiates SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(sc).eq(sqlContext),
      "SQLContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate return the original SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val newSession = sqlContext.newSession()
    assert(SQLContext.getOrCreate(sc).eq(sqlContext),
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
    SparkSession.setActiveSession(newSession.sparkSession)
    assert(SQLContext.getOrCreate(sc).eq(newSession),
      "SQLContext.getOrCreate after explicitly setActive() did not return the active context")
  }

  test("Bug SPARK-22192 Nested POJO object not handled when creating DataFrame from RDD") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val personsCollection = for (k <- 1 until 100) yield {
      new Person(k, "name_" + k, k.toLong, k.toShort,
        k.toByte, k.toDouble *86.7543d, k.toFloat *7.31f,
        true, Array.fill[Byte](k)(k.toByte),
        new java.sql.Date(7836*k*1000), new Timestamp(7896*k*1000),
        new Address("12320 sw horizon", 97007))
    }

    // create a pair RDD from the collection
    val personsRDD = sc.parallelize(personsCollection)
    val df = sqlContext.createDataFrame(personsRDD, classOf[Person])
    df.printSchema()
    df.collect()
  }

  test("Sessions of SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val session1 = sqlContext.newSession()
    val session2 = sqlContext.newSession()

    // all have the default configurations
    val key = SQLConf.SHUFFLE_PARTITIONS.key
    assert(session1.getConf(key) === session2.getConf(key))
    session1.setConf(key, "1")
    session2.setConf(key, "2")
    assert(session1.getConf(key) === "1")
    assert(session2.getConf(key) === "2")

    // temporary table should not be shared
    val df = session1.range(10)
    df.createOrReplaceTempView("test1")
    assert(session1.tableNames().contains("test1"))
    assert(!session2.tableNames().contains("test1"))

    // UDF should not be shared
    def myadd(a: Int, b: Int): Int = a + b
    session1.udf.register[Int, Int, Int]("myadd", myadd)
    session1.sql("select myadd(1, 2)").explain()
    intercept[AnalysisException] {
      session2.sql("select myadd(1, 2)").explain()
    }
  }

  test("Catalyst optimization passes are modifiable at runtime") {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.experimental.extraOptimizations = Seq(DummyRule)
    assert(sqlContext.sessionState.optimizer.batches.flatMap(_.rules).contains(DummyRule))
  }

  test("get all tables") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")
    assert(
      sqlContext.tables().filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    assert(
      sqlContext.sql("SHOW tables").filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    sqlContext.sessionState.catalog.dropTable(
      TableIdentifier("listtablessuitetable"), ignoreIfNotExists = true, purge = false)
    assert(sqlContext.tables().filter("tableName = 'listtablessuitetable'").count() === 0)
  }

  test("getting all tables with a database name has no impact on returned table names") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")
    assert(
      sqlContext.tables("default").filter("tableName = 'listtablessuitetable'").collect().toSeq ==
      Row("", "listtablessuitetable", true) :: Nil)

    assert(
      sqlContext.sql("show TABLES in default").filter("tableName = 'listtablessuitetable'")
        .collect().toSeq == Row("", "listtablessuitetable", true) :: Nil)

    sqlContext.sessionState.catalog.dropTable(
      TableIdentifier("listtablessuitetable"), ignoreIfNotExists = true, purge = false)
    assert(sqlContext.tables().filter("tableName = 'listtablessuitetable'").count() === 0)
  }

  test("query the returned DataFrame of tables") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.range(10)
    df.createOrReplaceTempView("listtablessuitetable")

    val expectedSchema = StructType(
      StructField("database", StringType, false) ::
        StructField("tableName", StringType, false) ::
        StructField("isTemporary", BooleanType, false) :: Nil)

    Seq(sqlContext.tables(), sqlContext.sql("SHOW TABLes")).foreach {
      case tableDF =>
        assert(expectedSchema === tableDF.schema)

        tableDF.createOrReplaceTempView("tables")
        assert(
          sqlContext.sql(
            "SELECT isTemporary, tableName from tables WHERE tableName = 'listtablessuitetable'")
            .collect().toSeq == Row(true, "listtablessuitetable") :: Nil)
        assert(
          sqlContext.tables().filter("tableName = 'tables'").select("tableName", "isTemporary")
            .collect().toSeq == Row("tables", true) :: Nil)
        sqlContext.dropTempTable("tables")
    }
  }

}


class Person(var id: Int, var name: String, var longField: Long, var shortField: Short,
             var byteField: Byte, var doubleField: Double, var floatField: Float,
             var booleanField: Boolean, var binaryField: Array[Byte],
             var datee: Date, var timeeStamp: Timestamp,
             var address: Address  ) extends java.io.Serializable{
  def this() = this(0, null, 0, 0, 0, 0d, 0f, false, null, null, null, null)
  def getName: String = name
  def getId: Int = id
  def getLongField: Long = longField
  def getShortField: Short = shortField
  def getByteField: Byte = byteField
  def getDoubleField: Double = doubleField
  def getFloatField: Float = floatField
  def getBooleanField: Boolean = booleanField
  def getBinaryField: Array[Byte] = binaryField
  def getDatee: Date = datee
  def getTimeeStamp: Timestamp = timeeStamp
  def getAddress: Address = address

  def setName(name: String): Unit = {this.name = name}
  def setId(id: Int): Unit = {this.id = id}
  def setLongField(longField: Long): Unit = {this.longField = longField}
  def setShortField(shortField: Short): Unit = {this.shortField = shortField}
  def setByteField(byteField: Byte): Unit = {this.byteField = byteField}
  def setDoubleField(doubleField: Double): Unit = {this.doubleField = doubleField}
  def setFloatField(floatField: Float): Unit = {this.floatField = floatField}
  def setBooleanField(booleanField: Boolean): Unit = {this.booleanField = booleanField}
  def setBinaryField(binaryField: Array[Byte]): Unit = {this.binaryField = binaryField}
  def setDatee(datee: Date): Unit = {this.datee = datee}
  def setTimeeStamp(ts: Timestamp): Unit = {this.timeeStamp = ts}
  def setAddress(address: Address): Unit = {this.address = address}
}



class Address(var street: String, var zip: Int) extends java.io.Serializable {
  def this() = this(null, -1)
  def getStreet: String = this.street
  def getZip: Int = this.zip
  def setStreet(street: String): Unit = {this.street = street}
  def setZip(zip: Int): Unit = {this.zip = zip}
}

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
package org.apache.spark.sql.hive.execution

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.MetaStoreEventListener
import org.apache.hadoop.hive.metastore.events.{AddPartitionEvent, CreateTableEvent, DropTableEvent}

import org.apache.spark.sql.{QueryTest, SaveMode, SparkSession}
import org.apache.spark.sql.hive.execution.InsertEventListener.{disableListener, enableListener}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils


case class BugTestData(key: Int, value: String)

case class BugTestDataBothInt(key: Int, value: Int)

class BugTests extends QueryTest with SQLTestUtils with TestHiveSingleton {

  private var scratchDir: String = _
  private var path: File = _

  import testImplicits._

  lazy private val testDataFrame = spark.sparkContext.parallelize(
    (1 to 100).map(i => BugTestData(i, i.toString))).toDF()

  lazy private val testDataFrameBothInt = spark.sparkContext.parallelize(
    (1 to 100).map(i => BugTestDataBothInt(1, i))).toDF()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // scalastyle:off hadoopconfiguration
    scratchDir = spark.sparkContext.hadoopConfiguration.get(
      ConfVars.SCRATCHDIR.varname).substring("file:".length)
    // scalastyle:on hadoopconfiguration
    withTable(InsertEventListener.enableListener) {
      sql(s"CREATE TABLE ${InsertEventListener.enableListener} (key int, value string)" +
        s" using hive")
    }
  }

  override protected def afterAll(): Unit = {
    // reinstate props:
    // disable InsertEventListener
    withTable(InsertEventListener.disableListener) {
      sql(s"CREATE TABLE ${InsertEventListener.disableListener} (key int, value string)" +
        s" using hive")
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    path = Utils.createTempDir()
    path.delete()
  }

  test("SPARK-43112 insert using dataframe api on partitioned table") {
    withTable("createAndInsertTest") {
      sql(s"CREATE TABLE createAndInsertTest (key int, value string) using" +
        s" hive OPTIONS(fileFormat='parquet') partitioned by (key)")
      val x = spark.table("createAndInsertTest")
      val tableSchema = x.schema
      // Add some data.
      val dataFrameSchema = testDataFrame.schema
      assert(tableSchema.zip(dataFrameSchema).forall {
        case(sf1, sf2) => sf1.name.equalsIgnoreCase(sf2.name) && sf1.dataType == sf2.dataType
      })
      testDataFrame.write.mode(SaveMode.Append).
        insertInto("createAndInsertTest")
    }
  }

  test("SPARK-43112 wrong partitions being created") {
    withTable("createAndInsertTest") {
      sql(s"CREATE TABLE createAndInsertTest (key int, value int) using" +
        s" hive OPTIONS(fileFormat='parquet') partitioned by (key)")
      val tableSchema = spark.table("createAndInsertTest").schema
      // use dataframe with key as 1 and values ranging from 1 to 100
      testDataFrameBothInt.write.mode(SaveMode.Append).insertInto("createAndInsertTest")
      // count the number of partitions created.
      // it should be 1 and not 100
      assertFromResultsFile(1)

    }
  }

  test("SPARK-43112 check schema fields order correctness") {
    withTable("createAndInsertTest") {
      sql(s"CREATE TABLE createAndInsertTest (key int, value string) using" +
        s" hive OPTIONS(fileFormat='parquet') partitioned by (key)")
      val tableSchema = spark.table("createAndInsertTest").schema
      assert(tableSchema.fieldNames(0) == "key")
      assert(tableSchema.fieldNames(1) == "value")
    }
  }

  private def assertFromResultsFile(expectedPartitionCount: Int): Unit = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(
      new File(scratchDir, InsertEventListener.resultFileName))))
    val actualCount = Integer.parseInt(reader.readLine().trim())
    assert(actualCount === expectedPartitionCount)
  }
}

class InsertEventListener(config: Configuration) extends MetaStoreEventListener(config) {
  private var listenerEnabled: Boolean = false


  private def resultFile: File = {
    val scratchDir = new File(SparkSession.getActiveSession.get.sparkContext.
      hadoopConfiguration.get(ConfVars.SCRATCHDIR.varname).substring("file:".length))
    scratchDir.mkdir()
    val resultFile = config.get(InsertEventListener.keyForResultFileName)
    new java.io.File(scratchDir, resultFile)
  }

  override def onCreateTable(tableEvent: CreateTableEvent): Unit = {
    if (tableEvent.getTable.getTableName.equalsIgnoreCase(InsertEventListener.disableListener)) {
      listenerEnabled = false
    } else if (tableEvent.getTable.getTableName.equalsIgnoreCase(enableListener)) {
      listenerEnabled = true
    }
    if (listenerEnabled) {
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
      writer.write("")
      writer.close()
    }
  }


  override def onAddPartition(partitionEvent: AddPartitionEvent): Unit = if (listenerEnabled) {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(resultFile)))
    val previousPartsStr = reader.readLine()
    val previousParts = if ((previousPartsStr eq null) || previousPartsStr.isEmpty) {
      0
    } else {
      previousPartsStr.toInt
    }
    import scala.collection.JavaConverters._
    val newParts = partitionEvent.getPartitionIterator.asScala.length
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))

    writer.write(String.valueOf(newParts + previousParts))
    writer.close()
  }

  override def onDropTable(tableEvent: DropTableEvent): Unit = {
    val isListenerTablesDrop = tableEvent.getTable.getTableName.equalsIgnoreCase(enableListener) ||
      tableEvent.getTable.getTableName.equalsIgnoreCase(disableListener)

    if (listenerEnabled && !isListenerTablesDrop) {
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
      writer.write("")
      writer.close()
    }
  }
}

object InsertEventListener {
  val keyForResultFileName = "hive.metastore.dml.events.resultfile"
  val resultFileName = "result"
  val disableListener = "disableListener"
  val enableListener = "enableListener"
}
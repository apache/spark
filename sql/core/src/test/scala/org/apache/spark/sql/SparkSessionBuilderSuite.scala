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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.catalog._

/**
 * Test cases for the builder pattern of [[SparkSession]].
 */
class SparkSessionBuilderSuite extends SparkFunSuite {

  private var initialSession: SparkSession = _

  private lazy val sparkContext: SparkContext = {
    initialSession = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", value = false)
      .config("some-config", "v2")
      .getOrCreate()
    initialSession.sparkContext
  }

  test("create with config options and propagate them to SparkContext and SparkSession") {
    // Creating a new session with config - this works by just calling the lazy val
    sparkContext
    assert(initialSession.sparkContext.conf.get("some-config") == "v2")
    assert(initialSession.conf.get("some-config") == "v2")
    SparkSession.clearDefaultSession()
  }

  test("use global default session") {
    val session = SparkSession.builder().getOrCreate()
    assert(SparkSession.builder().getOrCreate() == session)
    SparkSession.clearDefaultSession()
  }

  test("config options are propagated to existing SparkSession") {
    val session1 = SparkSession.builder().config("spark-config1", "a").getOrCreate()
    assert(session1.conf.get("spark-config1") == "a")
    val session2 = SparkSession.builder().config("spark-config1", "b").getOrCreate()
    assert(session1 == session2)
    assert(session1.conf.get("spark-config1") == "b")
    SparkSession.clearDefaultSession()
  }

  test("use session from active thread session and propagate config options") {
    val defaultSession = SparkSession.builder().getOrCreate()
    val activeSession = defaultSession.newSession()
    SparkSession.setActiveSession(activeSession)
    val session = SparkSession.builder().config("spark-config2", "a").getOrCreate()

    assert(activeSession != defaultSession)
    assert(session == activeSession)
    assert(session.conf.get("spark-config2") == "a")
    SparkSession.clearActiveSession()

    assert(SparkSession.builder().getOrCreate() == defaultSession)
    SparkSession.clearDefaultSession()
  }

  test("create a new session if the default session has been stopped") {
    val defaultSession = SparkSession.builder().getOrCreate()
    SparkSession.setDefaultSession(defaultSession)
    defaultSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != defaultSession)
    newSession.stop()
  }

  test("create a new session if the active thread session has been stopped") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setActiveSession(activeSession)
    activeSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != activeSession)
    newSession.stop()
  }

  test("create SparkContext first then SparkSession") {
    sparkContext.stop()
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val sparkContext2 = new SparkContext(conf)
    val session = SparkSession.builder().config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext.conf.get("key1") == "value1")
    assert(session.sparkContext.conf.get("key2") == "value2")
    assert(session.sparkContext.conf.get("spark.app.name") == "test")
    session.stop()
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(session.sessionState.newHadoopConf().get("hive.in.test") == "true")
    assert(session.sparkContext.hadoopConfiguration.get("hive.in.test") == "true")
    session.stop()
  }

  test("SPARK-15991: Set global Hadoop conf") {
    val session = SparkSession.builder().master("local").getOrCreate()
    val mySpecialKey = "my.special.key.15991"
    val mySpecialValue = "msv"
    try {
      session.sparkContext.hadoopConfiguration.set(mySpecialKey, mySpecialValue)
      assert(session.sessionState.newHadoopConf().get(mySpecialKey) == mySpecialValue)
    } finally {
      session.sparkContext.hadoopConfiguration.unset(mySpecialKey)
      session.stop()
    }
  }

  test("SPARK-17767 Spark SQL ExternalCatalog API custom implementation support") {
    val session = SparkSession.builder()
      .master("local")
      .config("spark.sql.externalCatalog", "org.apache.spark.sql.MyExternalCatalog")
      .config("spark.sql.externalSessionState", "org.apache.spark.sql.MySessionState")
      .enableProvidedCatalog()
      .getOrCreate()
    assert(session.sharedState.externalCatalog.isInstanceOf[MyExternalCatalog])
    assert(session.sessionState.isInstanceOf[MySessionState])
    session.stop()
  }
}

class MyExternalCatalog(conf: SparkConf, hadoopConf: Configuration) extends ExternalCatalog {
  import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {}

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {}

  def getDatabase(db: String): CatalogDatabase = null

  def databaseExists(db: String): Boolean = true

  def listDatabases(): Seq[String] = Seq.empty

  def listDatabases(pattern: String): Seq[String] = Seq.empty

  def setCurrentDatabase(db: String): Unit = {}

  def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {}

  def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {}

  def renameTable(db: String, oldName: String, newName: String): Unit = {}

  def alterTable(tableDefinition: CatalogTable): Unit = {}

  def getTable(db: String, table: String): CatalogTable = null

  def getTableOption(db: String, table: String): Option[CatalogTable] = None

  def tableExists(db: String, table: String): Boolean = true

  def listTables(db: String): Seq[String] = Seq.empty

  def listTables(db: String, pattern: String): Seq[String] = Seq.empty

  def loadTable(
    db: String,
    table: String,
    loadPath: String,
    isOverwrite: Boolean,
    holdDDLTime: Boolean): Unit = {}

  def loadPartition(
    db: String,
    table: String,
    loadPath: String,
    partition: TablePartitionSpec,
    isOverwrite: Boolean,
    holdDDLTime: Boolean,
    inheritTableSpecs: Boolean): Unit = {}

  def loadDynamicPartitions(
    db: String,
    table: String,
    loadPath: String,
    partition: TablePartitionSpec,
    replace: Boolean,
    numDP: Int,
    holdDDLTime: Boolean): Unit = {}

  def createPartitions(
    db: String,
    table: String,
    parts: Seq[CatalogTablePartition],
    ignoreIfExists: Boolean): Unit = {}

  def dropPartitions(
    db: String,
    table: String,
    parts: Seq[TablePartitionSpec],
    ignoreIfNotExists: Boolean,
    purge: Boolean): Unit = {}

  def renamePartitions(
    db: String,
    table: String,
    specs: Seq[TablePartitionSpec],
    newSpecs: Seq[TablePartitionSpec]): Unit = {}

  def alterPartitions(
    db: String,
    table: String,
    parts: Seq[CatalogTablePartition]): Unit = {}

  def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition =
    null

  def getPartitionOption(
    db: String,
    table: String,
    spec: TablePartitionSpec): Option[CatalogTablePartition] = None

  def listPartitions(
    db: String,
    table: String,
    partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = Seq.empty

  def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {}

  def dropFunction(db: String, funcName: String): Unit = {}

  def renameFunction(db: String, oldName: String, newName: String): Unit = {}

  def getFunction(db: String, funcName: String): CatalogFunction = null

  def functionExists(db: String, funcName: String): Boolean = true

  def listFunctions(db: String, pattern: String): Seq[String] = Seq.empty
}

class MySessionState(sparkSession: SparkSession)
  extends org.apache.spark.sql.internal.SessionState(sparkSession) {
}

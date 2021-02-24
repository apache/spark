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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.internal.config.EXECUTOR_ALLOW_SPARK_CONTEXT
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.ThreadUtils

/**
 * Test cases for the builder pattern of [[SparkSession]].
 */
class SparkSessionBuilderSuite extends SparkFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // This suite should not interfere with the other test suites.
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
  }

  test("create with config options and propagate them to SparkContext and SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "v2")
      .getOrCreate()
    assert(session.sparkContext.conf.get("some-config") == "v2")
    assert(session.conf.get("some-config") == "v2")
  }

  test("use global default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.builder().getOrCreate() == session)
  }

  test("sets default and active session") {
    assert(SparkSession.getDefaultSession == None)
    assert(SparkSession.getActiveSession == None)
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.getDefaultSession == Some(session))
    assert(SparkSession.getActiveSession == Some(session))
  }

  test("get active or default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.active == session)
    SparkSession.clearActiveSession()
    assert(SparkSession.active == session)
    SparkSession.clearDefaultSession()
    intercept[IllegalStateException](SparkSession.active)
    session.stop()
  }

  test("config options are propagated to existing SparkSession") {
    val session1 = SparkSession.builder().master("local").config("spark-config1", "a").getOrCreate()
    assert(session1.conf.get("spark-config1") == "a")
    val session2 = SparkSession.builder().config("spark-config1", "b").getOrCreate()
    assert(session1 == session2)
    assert(session1.conf.get("spark-config1") == "b")
  }

  test("use session from active thread session and propagate config options") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    val activeSession = defaultSession.newSession()
    SparkSession.setActiveSession(activeSession)
    val session = SparkSession.builder().config("spark-config2", "a").getOrCreate()

    assert(activeSession != defaultSession)
    assert(session == activeSession)
    assert(session.conf.get("spark-config2") == "a")
    assert(session.sessionState.conf == SQLConf.get)
    assert(SQLConf.get.getConfString("spark-config2") == "a")
    SparkSession.clearActiveSession()

    assert(SparkSession.builder().getOrCreate() == defaultSession)
  }

  test("create a new session if the default session has been stopped") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setDefaultSession(defaultSession)
    defaultSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != defaultSession)
  }

  test("create a new session if the active thread session has been stopped") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setActiveSession(activeSession)
    activeSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != activeSession)
  }

  test("create SparkContext first then SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val sparkContext2 = new SparkContext(conf)
    val session = SparkSession.builder().config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == sparkContext2)
    // We won't update conf for existing `SparkContext`
    assert(!sparkContext2.conf.contains("key2"))
    assert(sparkContext2.conf.get("key1") == "value1")
  }

  test("create SparkContext first then pass context to SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val newSC = new SparkContext(conf)
    val session = SparkSession.builder().sparkContext(newSC).config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == newSC)
    assert(session.sparkContext.conf.get("key1") == "value1")
    // If the created sparkContext is passed through the Builder's API sparkContext,
    // the conf of this sparkContext will not contain the conf set through the API config.
    assert(!session.sparkContext.conf.contains("key2"))
    assert(session.sparkContext.conf.get("spark.app.name") == "test")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(session.sessionState.newHadoopConf().get("hive.in.test") == "true")
    assert(session.sparkContext.hadoopConfiguration.get("hive.in.test") == "true")
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
    }
  }

  test("SPARK-31234: RESET command will not change static sql configs and " +
    "spark context conf values in SessionState") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31234")
      .config("spark.app.name", "test-app-SPARK-31234")
      .getOrCreate()

    assert(session.sessionState.conf.getConfString("spark.app.name") === "test-app-SPARK-31234")
    assert(session.sessionState.conf.getConf(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
    session.sql("RESET")
    assert(session.sessionState.conf.getConfString("spark.app.name") === "test-app-SPARK-31234")
    assert(session.sessionState.conf.getConf(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
  }

  test("SPARK-31354: SparkContext only register one SparkSession ApplicationEnd listener") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31354-1")
    val context = new SparkContext(conf)
    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postFirstCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postSecondCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    assert(postFirstCreation == postSecondCreation)
  }

  test("SPARK-31532: should not propagate static sql configs to the existing" +
    " active/default SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532")
      .config("spark.app.name", "test-app-SPARK-31532")
      .getOrCreate()
    // do not propagate static sql configs to the existing active session
    val session1 = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-1")
      .getOrCreate()
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
    assert(session1.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")

    // do not propagate static sql configs to the existing default session
    SparkSession.clearActiveSession()
    val session2 = SparkSession
      .builder()
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532-2")
      .getOrCreate()

    assert(!session.conf.get(WAREHOUSE_PATH).contains("SPARK-31532-db"))
    assert(session.conf.get(WAREHOUSE_PATH) === session2.conf.get(WAREHOUSE_PATH))
    assert(session2.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
  }

  test("SPARK-31532: propagate static sql configs if no existing SparkSession") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31532-2")
      .set(GLOBAL_TEMP_DATABASE.key, "globaltempdb-spark-31532")
      .set(WAREHOUSE_PATH.key, "SPARK-31532-db")
    SparkContext.getOrCreate(conf)

    // propagate static sql configs if no existing session
    val session = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-2")
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db-2")
      .getOrCreate()
    assert(session.conf.get("spark.app.name") === "test-app-SPARK-31532-2")
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532-2")
    assert(session.conf.get(WAREHOUSE_PATH) === "SPARK-31532-db-2")
  }

  test("SPARK-32062: reset listenerRegistered in SparkSession") {
    (1 to 2).foreach { i =>
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName(s"test-SPARK-32062-$i")
      val context = new SparkContext(conf)
      val beforeListenerSize = context.listenerBus.listeners.size()
      SparkSession
        .builder()
        .sparkContext(context)
        .getOrCreate()
      val afterListenerSize = context.listenerBus.listeners.size()
      assert(beforeListenerSize + 1 == afterListenerSize)
      context.stop()
    }
  }

  test("SPARK-32160: Disallow to create SparkSession in executors") {
    val session = SparkSession.builder().master("local-cluster[3, 1, 1024]").getOrCreate()

    val error = intercept[SparkException] {
      session.range(1).foreach { v =>
        SparkSession.builder.master("local").getOrCreate()
        ()
      }
    }.getMessage()

    assert(error.contains("SparkSession should only be created and accessed on the driver."))
  }

  test("SPARK-32160: Allow to create SparkSession in executors if the config is set") {
    val session = SparkSession.builder().master("local-cluster[3, 1, 1024]").getOrCreate()

    session.range(1).foreach { v =>
      SparkSession.builder.master("local")
        .config(EXECUTOR_ALLOW_SPARK_CONTEXT.key, true).getOrCreate().stop()
      ()
    }
  }

  test("SPARK-32991: Use conf in shared state as the original configuration for RESET") {
    val wh = "spark.sql.warehouse.dir"
    val td = "spark.sql.globalTempDatabase"
    val custom = "spark.sql.custom"

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SPARK-32991")
      .set(wh, "./data1")
      .set(td, "bob")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .config(wh, "./data2")
      .config(td, "alice")
      .config(custom, "kyao")
      .getOrCreate()

    // When creating the first session like above, we will update the shared spark conf to the
    // newly specified values
    val sharedWH = spark.sharedState.conf.get(wh)
    val sharedTD = spark.sharedState.conf.get(td)
    assert(sharedWH === "./data2",
      "The warehouse dir in shared state should be determined by the 1st created spark session")
    assert(sharedTD === "alice",
      "Static sql configs in shared state should be determined by the 1st created spark session")
    assert(spark.sharedState.conf.getOption(custom).isEmpty,
      "Dynamic sql configs is session specific")

    assert(spark.conf.get(wh) === sharedWH,
      "The warehouse dir in session conf and shared state conf should be consistent")
    assert(spark.conf.get(td) === sharedTD,
      "Static sql configs in session conf and shared state conf should be consistent")
    assert(spark.conf.get(custom) === "kyao", "Dynamic sql configs is session specific")

    spark.sql("RESET")

    assert(spark.conf.get(wh) === sharedWH,
      "The warehouse dir in shared state should be respect after RESET")
    assert(spark.conf.get(td) === sharedTD,
      "Static sql configs in shared state should be respect after RESET")
    assert(spark.conf.get(custom) === "kyao",
      "Dynamic sql configs in session initial map should be respect after RESET")

    val spark2 = SparkSession.builder()
      .config(wh, "./data3")
      .config(custom, "kyaoo").getOrCreate()
    assert(spark2.conf.get(wh) === sharedWH)
    assert(spark2.conf.get(td) === sharedTD)
    assert(spark2.conf.get(custom) === "kyaoo")
  }

  test("SPARK-32991: RESET should work properly with multi threads") {
    val wh = "spark.sql.warehouse.dir"
    val td = "spark.sql.globalTempDatabase"
    val custom = "spark.sql.custom"
    val spark = ThreadUtils.runInNewThread("new session 0", false) {
      SparkSession.builder()
        .master("local")
        .config(wh, "./data0")
        .config(td, "bob")
        .config(custom, "c0")
        .getOrCreate()
    }

    spark.sql(s"SET $custom=c1")
    assert(spark.conf.get(custom) === "c1")
    spark.sql("RESET")
    assert(spark.conf.get(wh) === "./data0",
      "The warehouse dir in shared state should be respect after RESET")
    assert(spark.conf.get(td) === "bob",
      "Static sql configs in shared state should be respect after RESET")
    assert(spark.conf.get(custom) === "c0",
      "Dynamic sql configs in shared state should be respect after RESET")

    val spark1 = ThreadUtils.runInNewThread("new session 1", false) {
      SparkSession.builder().getOrCreate()
    }

    assert(spark === spark1)

    // TODO: SPARK-33718: After clear sessions, the SharedState will be unreachable, then all
    // the new static will take effect.
    SparkSession.clearDefaultSession()
    val spark2 = ThreadUtils.runInNewThread("new session 2", false) {
      SparkSession.builder()
        .master("local")
        .config(wh, "./data1")
        .config(td, "alice")
        .config(custom, "c2")
        .getOrCreate()
    }

    assert(spark2 !== spark)
    spark2.sql(s"SET $custom=c1")
    assert(spark2.conf.get(custom) === "c1")
    spark2.sql("RESET")
    assert(spark2.conf.get(wh) === "./data1")
    assert(spark2.conf.get(td) === "alice")
    assert(spark2.conf.get(custom) === "c2")

  }

  test("SPARK-33944: warning setting hive.metastore.warehouse.dir using session options") {
    val msg = "Not allowing to set hive.metastore.warehouse.dir in SparkSession's options"
    val logAppender = new LogAppender(msg)
    withLogAppender(logAppender) {
      SparkSession.builder()
        .master("local")
        .config("hive.metastore.warehouse.dir", "any")
        .getOrCreate()
        .sharedState
    }
    assert(logAppender.loggingEvents.exists(_.getRenderedMessage.contains(msg)))
  }

  test("SPARK-33944: no warning setting spark.sql.warehouse.dir using session options") {
    val msg = "Not allowing to set hive.metastore.warehouse.dir in SparkSession's options"
    val logAppender = new LogAppender(msg)
    withLogAppender(logAppender) {
      SparkSession.builder()
        .master("local")
        .config("spark.sql.warehouse.dir", "any")
        .getOrCreate()
        .sharedState
    }
    assert(!logAppender.loggingEvents.exists(_.getRenderedMessage.contains(msg)))
  }
}

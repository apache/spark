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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.tags.ExtendedSQLTest

/**
 * Tests native functions in local-cluster mode, where the executors run in JVMs of their own:
 * Spark distributes the packages to them, and they find, check and load the libraries
 * themselves. [[NativeUDFSuite]] tests the rest in local mode.
 */
@ExtendedSQLTest
class NativeUDFClusterSuite extends QueryTest {
  import NativeDataSourceTestUtils.checkingOrderOfCalls
  import NativeUDFTestUtils._

  protected var spark: SparkSession = _

  // Jobs outside of SQL executions get the files of all the sessions, so the sessions are kept
  // until the end of the suite, as in NativeUDFSuite.
  private val sessions = ArrayBuffer.empty[SparkSession]

  private lazy val library = compile("test_native_udf")
  // Another build, so that its package has another checksum, which the executors did not load.
  private lazy val otherLibrary = compile("test_native_udf_cluster")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("NativeUDFClusterSuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      sessions.clear()
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  // The executors append the calls that the order of the calls rules out to the same file.
  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
    try checkingOrderOfCalls(f) finally SparkSession.setActiveSession(spark)
  }

  private def newSession(): SparkSession = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    sessions += session
    session
  }

  private def checkRows(df: => DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expected, checkToRDD = false)
  }

  nativeTest("evaluate the functions of a package added as an artifact") {
    val session = newSession()
    val pkg = createPackage(library)
    session.addArtifact(pkg.getPath)
    session.sql(s"CREATE TEMPORARY FUNCTION add_one AS 'add_one' USING PACKAGE '${pkg.getName}'")
    // 4 partitions, so that both executors load the library.
    checkRows(session.range(0, 20, 1, 4).selectExpr("add_one(id)"), (1L to 20L).map(Row(_)))
  }

  nativeTest("evaluate the functions that a clone inherits from a session that is released") {
    val session = newSession()
    val pkg = createPackage(library)
    session.addArtifact(pkg.getPath)
    session.sql(s"CREATE TEMPORARY FUNCTION add_one AS 'add_one' USING PACKAGE '${pkg.getName}'")
    val clone = session.cloneSession()
    sessions += clone
    session.artifactManager.close()
    SparkSession.setActiveSession(clone)
    checkRows(clone.range(0, 8, 1, 4).selectExpr("add_one(id)"), (1L to 8L).map(Row(_)))
  }

  nativeTest("executors load the distributed copy of the package at a path") {
    val session = newSession()
    val pkg = createPackage(otherLibrary)
    session.sql(s"CREATE TEMPORARY FUNCTION shout AS 'shout' USING PACKAGE '${pkg.getPath}'")
    // Copies the package to the artifacts of the session, as a query does.
    NativeDataSourceRegistry.distribute(NativeDataSourcePackage.read(pkg))
    // Then the file at the path of the driver is no longer a valid package, while the driver
    // keeps the package it read, since the file is written in place and keeps its length and
    // modification time. So the query only succeeds if the executors find the distributed copy.
    val lastModified = pkg.lastModified()
    Files.write(pkg.toPath, new Array[Byte](pkg.length().toInt))
    assert(pkg.setLastModified(lastModified))
    checkRows(session.range(0, 8, 1, 4).selectExpr("shout(CAST(id AS STRING))"),
      (0 until 8).map(i => Row(s"$i!")))
  }

  nativeTest("distribute the package at a path through the session of a streaming query") {
    val session = newSession()
    import session.implicits._
    val pkg = createPackage(library)
    session.sql(s"CREATE TEMPORARY FUNCTION add_one AS 'add_one' USING PACKAGE '${pkg.getPath}'")
    val input = MemoryStream[Long](session, 4)
    val query = input.toDF().selectExpr("add_one(value)").writeStream
      .format("memory").queryName("native_stream").start()
    try {
      input.addData(0L until 20L)
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    checkRows(session.table("native_stream"), (1L to 20L).map(Row(_)))
    val stream = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sparkSessionForStream
    assert(stream.artifactManager.hasFileArtifact(
      s"${pkg.getName}.${NativeDataSourcePackage.read(pkg).sha256}"))
  }
}

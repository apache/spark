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

import java.io.File
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

/**
 * Tests native data sources in local-cluster mode, where the executors run in JVMs of their own:
 * Spark distributes the packages to them, and they find, check and load the libraries
 * themselves. [[NativeDataSourceSuite]] tests the rest in local mode, where the executors use the
 * packages of the driver.
 */
@ExtendedSQLTest
class NativeDataSourceClusterSuite extends QueryTest {
  import NativeDataSourceTestUtils._

  protected var spark: SparkSession = _

  // Jobs outside of SQL executions get the files of all the sessions, so the sessions are kept
  // until the end of the suite, as in NativeDataSourceSuite.
  private val sessions = ArrayBuffer.empty[SparkSession]

  private lazy val library = compile("test_native_datasource")
  // Another build, so that its package has another checksum, which the executors did not load.
  private lazy val otherLibrary =
    compile("test_native_datasource_cluster", "-DNAME_PREFIX=\"cluster_\"")

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("NativeDataSourceClusterSuite")
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

  // Like Python data sources, native data sources are found in the active session.
  private def newSession(): SparkSession = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    sessions += session
    session
  }

  private def checkRows(df: => DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expected, checkToRDD = false)
  }

  private def written(dir: File): Seq[String] = {
    dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala).toSeq.sorted
  }

  nativeTest("read and write with a package added as an artifact") {
    val session = newSession()
    session.addArtifact(createPackage(library, dataSources("")).getPath)
    // 4 partitions, so that both executors load the library.
    val df = session.read.format("native_range")
      .option("end", "20").option("partitions", "4").load().select("id")
    checkRows(df, (0L until 20L).map(Row(_)))

    val dir = Utils.createTempDir()
    session.range(10).repartition(4).selectExpr("id", "concat('v', id) AS name")
      .write.format("native_sink").option("path", dir.getPath).mode("append").save()
    assert(written(dir) == (0 until 10).map(i => s"$i,v$i").sorted)

    val streamDir = Utils.createTempDir()
    val query = session.readStream.format("native_counter")
      .option("max_offset", "12").option("step", "5").load()
      .writeStream.format("native_sink")
      .option("path", streamDir.getPath)
      .option("checkpointLocation", Utils.createTempDir().getPath)
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    assert(written(streamDir) == (0 until 12).map(_.toString).sorted)
  }

  nativeTest("executors load the distributed copy of a configured package") {
    val session = newSession()
    val pkg = createPackage(otherLibrary, dataSources("cluster_"))
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, pkg.getPath)
    // Prepares the package on the driver as a query would: Spark reads its manifest, loads its
    // library and copies it to the artifacts of the session under a name with its checksum.
    val found = NativeDataSourceRegistry.lookup("cluster_native_range", session.sessionState.conf)
      .map(_._2).get.asInstanceOf[NativeDataSourcePackage]
    NativeLibraries.get(found)
    val distributed =
      NativeDataSourceRegistry.distribute(found).asInstanceOf[NativeDataSourcePackage]
    assert(distributed.artifactUUID.contains(session.sessionUUID))
    assert(distributed.distributedFileName.contains(s"${pkg.getName}.${found.sha256}"))

    // Then the file at the path of the driver is no longer a valid package, so the executors
    // cannot use it, while the driver keeps the package it read, since the file is written in
    // place and keeps its length and modification time. So the query only succeeds if the
    // executors find the distributed copy. NativeDataSourceSuite checks that the executors only
    // load a valid package with the checksum of the driver.
    val lastModified = pkg.lastModified()
    Files.write(pkg.toPath, new Array[Byte](pkg.length().toInt))
    assert(pkg.setLastModified(lastModified))
    val df = session.read.format("cluster_native_range")
      .option("end", "20").option("partitions", "4").load().select("id")
    checkRows(df, (0L until 20L).map(Row(_)))
  }
}

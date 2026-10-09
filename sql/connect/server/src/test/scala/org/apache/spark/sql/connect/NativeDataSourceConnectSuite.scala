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
package org.apache.spark.sql.connect

import java.nio.file.Files
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeDataSourceTestUtils._
import org.apache.spark.util.Utils

/**
 * Tests native data sources with Spark Connect: a client adds packages to its session with
 * `spark.addArtifact`, and the server finds the data sources in the packages of the session or in
 * the installed libraries. The tests are skipped when there is no C++ compiler or no JNI headers.
 */
class NativeDataSourceConnectSuite extends SparkConnectServerTest {

  private lazy val library = compile("test_native_datasource")
  private lazy val installedLibrary =
    compile("test_native_datasource_installed", "-DNAME_PREFIX=\"installed_\"")

  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and the JNI headers are required.")
    f
  }

  private def withNewSession(f: SparkSession => Unit): Unit = {
    withSession(sessionId = UUID.randomUUID().toString)(f)
  }

  nativeTest("add packages with spark.addArtifact") {
    val pkg = createPackage(library, dataSources(""))
    withNewSession { session =>
      session.addArtifact(pkg.getPath)
      val df = session.read.format("native_range").option("end", "3").load()
      assert(df.select("id").collect().map(_.getLong(0)).sorted.toSeq == Seq(0L, 1L, 2L))

      val dir = Utils.createTempDir()
      session.range(2).selectExpr("id", "concat('v', id) AS name")
        .write.format("native_sink").option("path", dir.getPath).mode("append").save()
      assert(dir.listFiles().filter(_.getName.startsWith("part-"))
        .flatMap(file => Files.readAllLines(file.toPath).asScala).toSet == Set("0,v0", "1,v1"))

      try {
        session.sql("CREATE TABLE native_connect_table USING native_range OPTIONS (end 2)")
        assert(session.sql("SELECT id FROM native_connect_table").collect()
          .map(_.getLong(0)).sorted.toSeq == Seq(0L, 1L))
      } finally {
        session.sql("DROP TABLE IF EXISTS native_connect_table")
      }
    }

    // The packages of a session are not visible to the other sessions.
    withNewSession { other =>
      val e = intercept[SparkException] {
        other.read.format("native_range").load().collect()
      }
      assert(e.getCondition == "DATA_SOURCE_NOT_FOUND")
    }
  }

  nativeTest("find installed libraries automatically") {
    val dir = Utils.createTempDir()
    installLibrary(installedLibrary, "installed_native_range", dir)
    withLibraryPath(dir) {
      withNewSession { session =>
        val df = session.read.format("installed_native_range").option("end", "3").load()
        assert(df.select("id").collect().map(_.getLong(0)).sorted.toSeq == Seq(0L, 1L, 2L))
      }
    }
  }
}

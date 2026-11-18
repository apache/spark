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

package org.apache.spark.sql.execution.python

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Python.PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonTableFunction, shouldTestPythonUDFs}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Tests that the Python workers of a session with artifact isolation, as in Spark Connect, can
 * import the Python files that the application was started with (`spark.submit.pyFiles`).
 */
class PythonGlobalPyFilesSuite extends QueryTest with SharedSparkSession {

  // The application's Python files: a module, a package in a zip file, and a module whose name
  // a session can also use.
  private lazy val globalPyFiles: Seq[String] = {
    val dir = Utils.createTempDir()
    val module = new File(dir, "global_py_files_module.py")
    Files.writeString(module.toPath, "value = 'global module'")
    val clash = new File(dir, "global_py_files_clash.py")
    Files.writeString(clash.toPath, "value = 'global clash'")
    val pkg = new File(dir, "global_py_files_pkg.zip")
    val out = new ZipOutputStream(Files.newOutputStream(pkg.toPath))
    try {
      out.putNextEntry(new ZipEntry("global_py_files_pkg/__init__.py"))
      out.write("value = 'global package'".getBytes("UTF-8"))
      out.closeEntry()
    } finally {
      out.close()
    }
    Seq(module, clash, pkg).map(_.getAbsolutePath)
  }

  // As spark-submit does with --py-files outside YARN.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.files", globalPyFiles.mkString(","))
    .set("spark.submit.pyFiles", globalPyFiles.mkString(","))

  private val udtfName = "global_py_files_udtf"

  // `analyze` runs in a Python worker on the driver, and `eval` in one on an executor.
  private val udtfScript =
    s"""
       |from pyspark.sql.functions import AnalyzeResult
       |from pyspark.sql.types import StringType, StructType
       |
       |class $udtfName:
       |    @staticmethod
       |    def analyze():
       |        import global_py_files_module
       |        import global_py_files_pkg
       |        return AnalyzeResult(
       |            schema=StructType()
       |                .add("module", StringType())
       |                .add("package", StringType())
       |                .add("clash", StringType()))
       |
       |    def eval(self):
       |        import global_py_files_clash
       |        import global_py_files_module
       |        import global_py_files_pkg
       |        yield (
       |            global_py_files_module.value,
       |            global_py_files_pkg.value,
       |            global_py_files_clash.value)
       |""".stripMargin

  /** Runs the UDTF in `session`, under its artifact state, as Spark Connect does. */
  private def runUDTF(session: SparkSession): Seq[Row] = {
    session.udtf.registerPython(
      udtfName, createUserDefinedPythonTableFunction(udtfName, udtfScript, returnType = None))
    session.artifactManager.withResources {
      session.sql(s"SELECT * FROM $udtfName()").collect().toSeq
    }
  }

  test("SPARK-62000: Isolated session imports the Python files of the application") {
    assume(shouldTestPythonUDFs)
    val session = spark.newSession()
    assert(session.artifactManager.state !== null)
    assert(runUDTF(session) === Seq(Row("global module", "global package", "global clash")))
  }

  test("SPARK-62000: Python files of the session take precedence over those of the application") {
    assume(shouldTestPythonUDFs)
    val session = spark.newSession()
    withTempDir { dir =>
      val clash = new File(dir, "global_py_files_clash.py")
      Files.writeString(clash.toPath, "value = 'session clash'")
      session.artifactManager.addArtifact(
        Paths.get("pyfiles/global_py_files_clash.py"), clash.toPath, None)
      assert(runUDTF(session) === Seq(Row("global module", "global package", "session clash")))
    }
  }

  test("SPARK-62000: Isolated session misses the Python files of the application if disabled") {
    assume(shouldTestPythonUDFs)
    val conf = spark.sparkContext.conf
    conf.set(PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED, false)
    try {
      // A new session has new Python workers, which read the config when they start.
      val e = intercept[Exception](runUDTF(spark.newSession()))
      assert(e.getMessage.contains("No module named 'global_py_files_module'"), e.getMessage)
    } finally {
      conf.remove(PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED)
    }
  }
}

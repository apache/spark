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

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.internal.config.Python.{PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED, PYTHON_USE_DAEMON}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonTableFunction, pysparkPythonPath, pythonPath, shouldTestPythonUDFs}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Tests that the Python workers of a session with artifact isolation, as in Spark Connect, can
 * import the Python files that the application was started with (`spark.submit.pyFiles`).
 */
class PythonGlobalPyFilesSuite extends QueryTest with SharedSparkSession {

  private lazy val filesDir = Utils.createTempDir()

  /** Writes an archive that holds the package `name` whose `value` is `value`. */
  private def writePackage(archive: File, name: String, value: String): File = {
    val out = new ZipOutputStream(Files.newOutputStream(archive.toPath))
    try {
      out.putNextEntry(new ZipEntry(s"$name/__init__.py"))
      out.write(s"value = '$value'".getBytes("UTF-8"))
      out.closeEntry()
    } finally {
      out.close()
    }
    archive
  }

  // The application's Python files: a module, a package in a zip file, and a module whose name
  // a session can also use.
  private lazy val globalPyFiles: Seq[String] = {
    val dir = filesDir
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
    // Named like modules of the standard library that a Python worker imports when it starts:
    // the workers must not have them on their path until they have started.
    val token = new File(dir, "token.py")
    Files.writeString(token.toPath, "raise ImportError('shadowed the standard library')")
    val shadow = new File(dir, "global_py_files_shadow.zip")
    val shadowOut = new ZipOutputStream(Files.newOutputStream(shadow.toPath))
    try {
      shadowOut.putNextEntry(new ZipEntry("signal/__init__.py"))
      shadowOut.write("raise ImportError('shadowed the standard library')".getBytes("UTF-8"))
      shadowOut.closeEntry()
    } finally {
      shadowOut.close()
    }
    // Packages in archives of each kind that PySpark adds, one with an upper-case extension.
    val egg = writePackage(new File(dir, "global_py_files_egg.egg"), "global_py_files_egg", "egg")
    val jar = writePackage(new File(dir, "global_py_files_jar.jar"), "global_py_files_jar", "jar")
    val upper = writePackage(
      new File(dir, "GLOBAL_PY_FILES_UPPER.ZIP"), "global_py_files_upper", "upper")
    Seq(module, clash, pkg, token, shadow, egg, jar, upper).map(_.getAbsolutePath)
  }

  // Another file of the application: a module that is not one of its Python files.
  private lazy val otherFiles: Seq[String] = {
    val unlisted = new File(filesDir, "global_py_files_unlisted.py")
    Files.writeString(unlisted.toPath, "value = 'unlisted'")
    Seq(unlisted).map(_.getAbsolutePath)
  }

  // As spark-submit does with --py-files and --files outside YARN, except that the Python files
  // are named by paths that do not exist: the workers must use the copies in the default files
  // directory, as on the executors of other hosts. The list also names a module twice, and a
  // module that has no copy in that directory, as on YARN.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.files", (globalPyFiles ++ otherFiles).mkString(","))
    .set("spark.submit.pyFiles",
      (globalPyFiles :+ globalPyFiles.head :+ "global_py_files_without_copy.py")
        .map(f => new File("/nonexistent", new File(f).getName).getPath).mkString(","))

  private val udtfName = "global_py_files_udtf"
  private val countUDTFName = "global_py_files_count_udtf"
  private def workerPythonPath: Seq[String] = Seq(pysparkPythonPath, pythonPath).filter(_.nonEmpty)

  // `analyze` runs in a Python worker on the driver, and `eval` in one on an executor.
  private val udtfScript =
    s"""
       |from pyspark.sql.functions import AnalyzeResult
       |from pyspark.sql.types import StringType, StructType
       |
       |def check_archives():
       |    import global_py_files_egg
       |    import global_py_files_jar
       |    import global_py_files_upper
       |    values = [m.value for m in [
       |        global_py_files_egg, global_py_files_jar, global_py_files_upper]]
       |    if values != ["egg", "jar", "upper"]:
       |        raise Exception(f"Unexpected archive values {values}")
       |
       |def check_start_path():
       |    # The application's Python files are added after the worker has started.
       |    import os
       |    added = [p for p in os.environ["SPARK_GLOBAL_PYTHON_PATH"].split(os.pathsep) if p]
       |    start = os.environ.get("PYTHONPATH", "").split(os.pathsep)
       |    if not added or any(p in start for p in added):
       |        raise Exception(f"Unexpected start path {start} for {added}")
       |
       |class $udtfName:
       |    @staticmethod
       |    def analyze():
       |        import global_py_files_module
       |        import global_py_files_pkg
       |        try:
       |            import global_py_files_unlisted
       |            raise Exception("Imported a file that is not a Python file of the application")
       |        except ModuleNotFoundError:
       |            pass
       |        check_start_path()
       |        check_archives()
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
       |        try:
       |            import global_py_files_unlisted
       |            raise Exception("Imported a file that is not a Python file of the application")
       |        except ModuleNotFoundError:
       |            pass
       |        check_start_path()
       |        check_archives()
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

  test("SPARK-62000: Isolated session imports the Python files of the application without daemon") {
    assume(shouldTestPythonUDFs)
    val conf = spark.sparkContext.conf
    conf.set(PYTHON_USE_DAEMON, false)
    try {
      // A new session gets a new Python worker factory, which starts simple workers.
      assert(runUDTF(spark.newSession()) ===
        Seq(Row("global module", "global package", "global clash")))
    } finally {
      conf.remove(PYTHON_USE_DAEMON)
    }
  }

  test("SPARK-62000: Archive already on the start path of the workers is added once") {
    assume(shouldTestPythonUDFs)
    val countScript =
      s"""
         |from pyspark.sql.functions import AnalyzeResult
         |from pyspark.sql.types import IntegerType, StructType
         |
         |class $countUDTFName:
         |    @staticmethod
         |    def analyze():
         |        return AnalyzeResult(schema=StructType().add("entries", IntegerType()))
         |
         |    def eval(self):
         |        import os
         |        import sys
         |        import global_py_files_pkg
         |        archive = os.path.realpath(global_py_files_pkg.__file__.rsplit(".zip", 1)[0])
         |        yield (sum(os.path.realpath(p) == archive + ".zip" for p in sys.path),)
         |""".stripMargin
    withTempDir { dir =>
      // The workers start with a PYTHONPATH that names the archive's copy through another
      // spelling of its path, as YARN's does on its executors.
      val link = new File(dir, "link").toPath
      Files.createSymbolicLink(link, Paths.get(SparkFiles.getRootDirectory()).toAbsolutePath)
      val session = spark.newSession()
      // The session's PYTHONPATH replaces the one of the test's Python functions, which finds
      // pyspark, so it names that path too.
      session.conf.set("spark.pythonWorkerEnv.PYTHONPATH",
        (link.resolve("global_py_files_pkg.zip").toString +: workerPythonPath).mkString(":"))
      session.udtf.registerPython(countUDTFName,
        createUserDefinedPythonTableFunction(countUDTFName, countScript, returnType = None))
      val rows = session.artifactManager.withResources {
        session.sql(s"SELECT * FROM $countUDTFName()").collect().toSeq
      }
      assert(rows === Seq(Row(1)))
    }
  }

  test("SPARK-62000: Isolated session misses the Python files of the application if disabled") {
    assume(shouldTestPythonUDFs)
    val conf = spark.sparkContext.conf
    conf.set(PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED, false)
    try {
      // A new session gets a new Python worker factory, which reads the config when it is created.
      val e = intercept[Exception](runUDTF(spark.newSession()))
      assert(e.getMessage.contains("No module named 'global_py_files_module'"), e.getMessage)
    } finally {
      conf.remove(PYTHON_ISOLATED_SESSION_GLOBAL_PY_FILES_ENABLED)
    }
  }
}

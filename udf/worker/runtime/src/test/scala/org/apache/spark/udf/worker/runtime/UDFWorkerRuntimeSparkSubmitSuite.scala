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

package org.apache.spark.udf.worker.runtime

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkEnv, TestUtils}
import org.apache.spark.deploy.SparkSubmitTestUtils
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExternalUserDefinedFunction}
import org.apache.spark.sql.catalyst.plans.logical.{ExecuteExternalUDF, Range}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.externalUDF.ExecuteExternalUDFExec
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.udf.worker.{DirectWorker, ProcessCallable, UDFProtoCommunicationPattern,
  UDFWorkerDataFormat, UDFWorkerProperties, UDFWorkerSpecification, UnixDomainSocket,
  WorkerCapabilities, WorkerConnectionSpec, WorkerEnvironment}
import org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain
import org.apache.spark.util.Utils

/**
 * Runs an external UDF through spark-submit, so the engine can only reach the direct
 * dispatcher through the shaded spark-udf-worker-runtime jar in the jars directory.
 *
 * The sbt-built jars directory also holds unshaded gRPC jars that a Maven-built
 * distribution does not ship. The test submits against a copy of that directory
 * without them, so a missed relocation fails here instead of being satisfied by a
 * leaked jar.
 */
class UDFWorkerRuntimeSparkSubmitSuite extends SparkSubmitTestUtils {

  import UDFWorkerRuntimeTestUtils._

  // Jars that only the sbt build leaks into the jars directory.
  private val unshadedGrpcJarPrefixes = Seq("grpc-", "perfmark-api-", "proto-google-common-protos-")

  test("external UDF runs through the shaded runtime jar under spark-submit") {
    assume(EchoGrpcWorkerMain.udsTransportAvailable,
      "Netty UDS native transport (epoll on Linux or kqueue on macOS) is required")
    assumeAssemblyBuilt()
    val runtimeJarName = runtimeJar().getName

    val tempDir = Utils.createTempDir()
    try {
      val sparkHome = new File(tempDir, "spark-home")
      val jars = new File(sparkHome, "jars")
      assert(jars.mkdirs())
      Files.createSymbolicLink(
        new File(sparkHome, "bin").toPath, new File(UDFWorkerRuntimeTestUtils.sparkHome, "bin")
          .getCanonicalFile.toPath)
      jarsDir.listFiles().filter(_.getName.endsWith(".jar")).foreach { jar =>
        if (!unshadedGrpcJarPrefixes.exists(jar.getName.startsWith)) {
          Files.createSymbolicLink(new File(jars, jar.getName).toPath, jar.toPath)
        }
      }

      val appJar = new File(tempDir, "udf-worker-runtime-app.jar")
      TestUtils.createJar(appClassFiles(), appJar, Some(appPackageDir))

      runSparkSubmit(
        Seq(
          "--class", UDFWorkerRuntimeApp.getClass.getName.stripSuffix("$"),
          "--name", "UDFWorkerRuntimeApp",
          "--master", "local-cluster[1,1,1024]",
          "--conf", s"${StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key}=true",
          "--conf", "spark.ui.enabled=false",
          appJar.toString,
          Paths.get(System.getProperty("java.home"), "bin", "java").toString,
          System.getProperty("java.class.path"),
          EchoGrpcWorkerMain.getClass.getName.stripSuffix("$"),
          runtimeJarName),
        sparkHomeOpt = Some(sparkHome.getCanonicalPath),
        timeout = 3.minutes,
        isSparkTesting = false)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  private val appPackageDir = UDFWorkerRuntimeApp.getClass.getPackage.getName.replace('.', '/')

  /** The compiled classes of [[UDFWorkerRuntimeApp]], to ship as the application jar. */
  private def appClassFiles(): Seq[File] = {
    val appClass = UDFWorkerRuntimeApp.getClass
    val classesDir = new File(appClass.getProtectionDomain.getCodeSource.getLocation.toURI)
    assert(classesDir.isDirectory, s"Expected test classes in a directory, got $classesDir")
    val prefix = appClass.getSimpleName.stripSuffix("$")
    new File(classesDir, appPackageDir).listFiles().filter { f =>
      f.getName.endsWith(".class") &&
        (f.getName == s"$prefix.class" || f.getName.startsWith(s"$prefix$$"))
    }.toSeq
  }
}

/**
 * The application [[UDFWorkerRuntimeSparkSubmitSuite]] submits. It checks that the
 * driver and an executor load the direct dispatcher factory from the runtime jar and
 * cannot load unshaded gRPC, then runs an identity UDF through an echo worker process.
 *
 * Arguments: the java binary, classpath and main class of the echo worker, and the file
 * name of the runtime jar. The echo worker is test code, so the application only names it.
 */
object UDFWorkerRuntimeApp {

  private val unshadedGrpcClass = "io.grpc.ManagedChannel"

  def main(args: Array[String]): Unit = {
    val Array(javaBin, workerClasspath, workerMainClass, runtimeJarName) = args
    val spark = SparkSession.builder().getOrCreate()
    try {
      val driver = classSources()
      val executor = spark.sparkContext.parallelize(Seq(0), 1).map(_ => classSources()).collect()
      (driver +: executor.toSeq).foreach { case (factorySource, grpcLoadable) =>
        check(factorySource.endsWith(runtimeJarName),
          s"${SparkEnv.DIRECT_DISPATCHER_FACTORY_CLASS} was loaded from $factorySource, " +
            s"not from $runtimeJarName")
        check(!grpcLoadable, s"$unshadedGrpcClass is on the Spark classpath")
      }

      val rows = runIdentityUDF(spark, javaBin, workerClasspath, workerMainClass)
      val expected = (0L until 6L).map(i => (i, i))
      check(rows == expected, s"Expected $expected, got $rows")
    } finally {
      spark.stop()
    }
  }

  /** Where the dispatcher factory comes from, and whether unshaded gRPC is loadable. */
  private def classSources(): (String, Boolean) = {
    val loader = classOf[SparkEnv].getClassLoader
    val factory = loader.loadClass(SparkEnv.DIRECT_DISPATCHER_FACTORY_CLASS)
    val grpcLoadable = try {
      loader.loadClass(unshadedGrpcClass)
      true
    } catch {
      case _: ClassNotFoundException => false
    }
    (factory.getProtectionDomain.getCodeSource.getLocation.getPath, grpcLoadable)
  }

  private def runIdentityUDF(
      spark: SparkSession,
      javaBin: String,
      workerClasspath: String,
      workerMainClass: String): Seq[(Long, Long)] = {
    val runner = ProcessCallable.newBuilder()
      .addCommand(javaBin)
      .addCommand("-cp")
      .addCommand(workerClasspath)
      .addCommand(workerMainClass)
    val spec = UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(WorkerCapabilities.newBuilder()
        .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
        .addSupportedCommunicationPatterns(UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING))
      .setDirect(DirectWorker.newBuilder()
        .setRunner(runner)
        .setProperties(UDFWorkerProperties.newBuilder()
          .setConnection(WorkerConnectionSpec.newBuilder()
            .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance))
          .setInitializationTimeoutMs(30000)))
      .build()
    val range = Range(0, 6, 1, 2)
    val function = ExternalUserDefinedFunction(
      name = Some("identity"),
      workerSpec = spec,
      payload = "identity".getBytes(StandardCharsets.UTF_8),
      dataType = LongType,
      children = range.output,
      udfDeterministic = true,
      udfNullable = false)
    val resultAttr = AttributeReference("externalUDF", LongType, nullable = true)()
    val plan = ExecuteExternalUDF(function, resultAttr, range)
    spark.sessionState.planner.plan(plan).toSeq match {
      case Seq(exec: ExecuteExternalUDFExec) =>
        exec.executeCollect().map(row => (row.getLong(0), row.getLong(1))).toSeq
      case other =>
        throw new IllegalStateException(
          s"Expected one ExecuteExternalUDFExec node, found:\n${other.mkString("\n")}")
    }
  }

  private def check(condition: Boolean, message: => String): Unit = {
    if (!condition) {
      throw new IllegalStateException(message)
    }
  }
}

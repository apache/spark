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

package org.apache.spark.sql.execution.externalUDF

import java.io.File
import java.net.{JarURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

import org.scalatest.Assertions
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkEnv, TestUtils}
import org.apache.spark.deploy.SparkSubmitTestUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExternalUserDefinedFunction}
import org.apache.spark.sql.catalyst.plans.logical.{ExecuteExternalUDF, Range}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.udf.worker.{DirectWorker, ProcessCallable, UDFProtoCommunicationPattern,
  UDFWorkerDataFormat, UDFWorkerProperties, UDFWorkerSpecification, UnixDomainSocket,
  WorkerCapabilities, WorkerConnectionSpec, WorkerEnvironment}

// Spark-submit is required so the driver loads assembly jars plus test-classes.
@ExtendedSQLTest
class UdfWorkerRuntimeSparkSubmitSuite extends SparkSubmitTestUtils {

  import org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain

  test("packaged UDF worker runtime executes Range(0, 6) with the echo worker") {
    assume(EchoGrpcWorkerMain.udsTransportAvailable,
      "Netty UDS native transport (epoll on Linux or kqueue on macOS) is required")
    val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val classPath = System.getProperty("java.class.path")
    val workerMain = classOf[EchoGrpcWorkerMain.type].getName.stripSuffix("$")
    // spark-internal is SparkLauncher.NO_RESOURCE. A real application jar is added
    // to the driver and then fetched from the driver, which deadlocks local mode.
    // The spark-submit driver is a new JVM, so it does not inherit the test
    // process's module opens. Arrow needs java.nio. The UDS client needs sun.nio.ch.
    val driverJavaOptions = Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true").mkString(" ")
    runSparkSubmit(Seq(
      "--class", UdfWorkerRuntimeSparkSubmitSuite.getClass.getName.stripSuffix("$"),
      "--master", "local[2]",
      "--driver-memory", "1g",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--conf", s"spark.driver.extraJavaOptions=$driverJavaOptions",
      "spark-internal",
      javaBin,
      classPath,
      workerMain),
      timeout = 3.minutes,
      environmentOverrides = packagedDriverEnvironment())
  }

  // SPARK_PREPEND_CLASSES puts unshaded module classes ahead of the assembly.
  // The dist classpath also carries unshaded udf-worker jars, including a second
  // Init.class. Drop those entries and keep the third-party test jars.
  private def packagedDriverEnvironment(): Map[String, Option[String]] = {
    val dist = Option(new ProcessBuilder().environment().get("SPARK_DIST_CLASSPATH"))
    val filtered = dist.map { classpath =>
      classpath.split(java.io.File.pathSeparator).filterNot(isUnshadedUdfWorker)
        .mkString(java.io.File.pathSeparator)
    }.filter(_.nonEmpty)
    Map(
      "SPARK_PREPEND_CLASSES" -> None,
      "SPARK_DIST_CLASSPATH" -> filtered)
  }

  private def isUnshadedUdfWorker(entry: String): Boolean = {
    val path = entry.replace('\\', '/')
    path.contains("/udf/worker/") || path.contains("spark-udf-worker-")
  }
}

object UdfWorkerRuntimeSparkSubmitSuite extends Assertions {

  private val InitResource = "org/apache/spark/udf/worker/Init.class"
  private val DispatcherFactory = "org.apache.spark.udf.worker.grpc.DirectDispatcherFactory"
  private val UdfGrpcProvider = "org.sparkproject.spark_udf_worker.grpc.LoadBalancerProvider"
  private val ConnectGrpcProvider = "org.sparkproject.connect.grpc.LoadBalancerProvider"
  private val RelocatedGrpcPackage = "org.sparkproject.spark_udf_worker.grpc."
  private val RelocatedService =
    "META-INF/services/org.sparkproject.spark_udf_worker.grpc.LoadBalancerProvider"
  private val UnrelocatedService = "META-INF/services/io.grpc.LoadBalancerProvider"
  private val Forbidden =
    Seq("io/grpc/", "com/google/protobuf/", "com/google/common/", "org/sparkproject/guava/")
  private val GuavaLiterals = Set("com/google/common/", "org/sparkproject/guava/")
  private val RequiredRefs = Seq("io/netty/", "org/sparkproject/spark_udf_worker/guava/")

  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j2("INFO")
    assert(args.length == 3, s"Expected java, classpath, and worker main. Got ${args.length}.")
    assertPackagedRuntime()
    assertEchoPlan(args(0), args(1), args(2))
  }

  private def assertPackagedRuntime(): Unit = {
    val loader = classOf[SparkEnv].getClassLoader
    val initUrls = loader.getResources(InitResource).asScala.toSeq
    assert(initUrls.length == 1, initUrls.mkString("\n"))
    val runtimeJar = jarFile(initUrls.head)
    assert(runtimeJar.getName.startsWith("spark-udf-worker-runtime"), runtimeJar.getPath)

    val factoryClass = loader.loadClass(DispatcherFactory)
    assert(codeSource(factoryClass) == runtimeJar, codeSource(factoryClass).getPath)
    assert(factoryClass.getConstructor().newInstance().getClass == factoryClass)

    val grpcClass = loader.loadClass(UdfGrpcProvider)
    assert(codeSource(grpcClass) == runtimeJar, codeSource(grpcClass).getPath)
    assertDistinctConnectSource(loader, runtimeJar)
    assertJarLayout(runtimeJar)
  }

  // A Connect server is not started. When the shaded Connect class is already
  // on the assembly classpath, its jar must be a different file from the UDF runtime.
  private def assertDistinctConnectSource(loader: ClassLoader, runtimeJar: File): Unit = {
    val resource = ConnectGrpcProvider.replace('.', '/') + ".class"
    if (loader.getResources(resource).asScala.nonEmpty) {
      val connectClass = loader.loadClass(ConnectGrpcProvider)
      val connectJar = codeSource(connectClass)
      assert(connectJar != runtimeJar, connectJar.getPath)
      assert(connectJar.getName.contains("spark-connect"), connectJar.getPath)
    }
  }

  private def assertJarLayout(jar: File): Unit = {
    val file = new JarFile(jar)
    try {
      val entries = file.entries().asScala.toSeq
      val names = entries.map(_.getName)
      val badNames = names.filter(name => Forbidden.exists(name.startsWith))
      assert(badNames.isEmpty, badNames.mkString("\n"))
      assert(names.contains(RelocatedService), s"missing $RelocatedService")
      assert(!names.contains(UnrelocatedService), s"found $UnrelocatedService")

      val service = readEntry(file, RelocatedService)
      val providers = service.split("\\s+").filter(_.nonEmpty)
      assert(providers.nonEmpty, service)
      assert(providers.forall(_.startsWith(RelocatedGrpcPackage)), service)
      assert(!service.contains("io.grpc."), service)

      val bodies = entries.filter(_.getName.endsWith(".class")).map { entry =>
        val in = file.getInputStream(entry)
        try {
          entry.getName -> in.readAllBytes()
        } finally {
          in.close()
        }
      }
      val badRefs = bodies.flatMap { case (name, bytes) =>
        // Relocated Guava keeps its original package in string literals, such as
        // the caller-check regex in PatternCompiler. Those literals are not links.
        val tokens = if (name.startsWith("org/sparkproject/spark_udf_worker/guava/")) {
          Forbidden.filterNot(GuavaLiterals.contains)
        } else {
          Forbidden
        }
        tokens.filter(token => contains(bytes, token)).map(token => s"$name $token")
      }
      assert(badRefs.isEmpty, badRefs.mkString("\n"))
      val missingRefs = RequiredRefs.filterNot { token =>
        bodies.exists { case (_, bytes) => contains(bytes, token) }
      }
      assert(missingRefs.isEmpty, missingRefs.mkString("\n"))
    } finally {
      file.close()
    }
  }

  private def assertEchoPlan(javaBin: String, classPath: String, workerMain: String): Unit = {
    val spark = SparkSession.builder()
      .config(StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED.key, "true")
      .getOrCreate()
    try {
      val range = Range(0, 6, 1, 2)
      val function = ExternalUserDefinedFunction(
        name = Some("identity"),
        workerSpec = workerSpecification(javaBin, classPath, workerMain),
        payload = "identity".getBytes(StandardCharsets.UTF_8),
        dataType = LongType,
        children = Seq(range.output.head),
        udfDeterministic = true,
        udfNullable = false)
      val resultAttr = AttributeReference("externalUDF", LongType, nullable = true)()
      val logicalPlan = ExecuteExternalUDF(function, resultAttr, range)
      val execution = spark.sessionState.planner.plan(logicalPlan).toSeq match {
        case Seq(node: ExecuteExternalUDFExec) => node
        case other =>
          fail(s"Expected one ExecuteExternalUDFExec node, found:\n${other.mkString("\n")}")
      }
      val rows = execution.executeCollect().map(row => (row.getLong(0), row.getLong(1)))
      assert(rows.toSeq == Seq(
        (0L, 0L), (1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L), (5L, 5L)))
    } finally {
      spark.stop()
    }
  }

  private def workerSpecification(
      javaBin: String,
      classPath: String,
      workerMain: String): UDFWorkerSpecification = {
    val runner = ProcessCallable.newBuilder()
      .addCommand(javaBin)
      .addCommand("-cp")
      .addCommand(classPath)
      .addCommand(workerMain)
    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(WorkerCapabilities.newBuilder()
        .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
        .addSupportedCommunicationPatterns(
          UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING))
      .setDirect(DirectWorker.newBuilder()
        .setRunner(runner)
        .setProperties(UDFWorkerProperties.newBuilder()
          .setConnection(WorkerConnectionSpec.newBuilder()
            .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance))
          .setInitializationTimeoutMs(30000)))
      .build()
  }

  private def jarFile(url: URL): File = {
    val fileUrl = url.getProtocol match {
      case "jar" =>
        url.openConnection().asInstanceOf[JarURLConnection].getJarFileURL
      case "file" =>
        url
      case _ =>
        fail(s"Unexpected resource URL $url")
    }
    new File(fileUrl.toURI).getCanonicalFile
  }

  private def codeSource(cls: Class[_]): File = {
    val location = cls.getProtectionDomain.getCodeSource.getLocation
    new File(location.toURI).getCanonicalFile
  }

  private def readEntry(file: JarFile, name: String): String = {
    val entry = file.getJarEntry(name)
    assert(entry != null, name)
    val in = file.getInputStream(entry)
    try {
      new String(in.readAllBytes(), StandardCharsets.UTF_8)
    } finally {
      in.close()
    }
  }

  private def contains(haystack: Array[Byte], needle: String): Boolean = {
    new String(haystack, StandardCharsets.ISO_8859_1).contains(needle)
  }
}

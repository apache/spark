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

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.StandardCharsets
import java.util.jar.JarFile

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.spark.SparkFunSuite

/**
 * Checks the shaded spark-udf-worker-runtime jar that the assembly ships: it carries
 * the direct dispatcher with gRPC and Guava relocated, and leaves Netty, protobuf-java
 * and the UDF worker proto and core classes to the rest of the Spark classpath.
 */
class UDFWorkerRuntimeJarSuite extends SparkFunSuite {

  import UDFWorkerRuntimeTestUtils._

  private val relocatedPrefix = "org/sparkproject/spark_udf_worker/"

  private val protoPackage = "org/apache/spark/udf/worker/"

  // Packages that must appear in the jar only in relocated form, or not at all.
  private val unrelocatedPackages = Seq(
    "io/grpc/",
    "io/perfmark/",
    "com/google/common/",
    "com/google/thirdparty/",
    "com/google/gson/",
    "android/annotation/",
    "org/codehaus/mojo/animal_sniffer/",
    // Spark's shared relocated Guava, which the Maven parent shade rewrites into the
    // udf-worker-grpc jar.
    "org/sparkproject/guava/")

  // Classes this jar must leave to other jars on the Spark classpath.
  private val excludedPackages = Seq(
    "io/netty/",
    "META-INF/native/",
    "META-INF/versions/",
    "com/google/protobuf/",
    "org/apache/spark/udf/worker/core/")

  /**
   * The constant pool strings of a class file that can name a type: class names and
   * descriptors, as opposed to string literals such as URLs, which keep the original
   * package text.
   */
  private def typeNames(classBytes: Array[Byte]): Seq[String] = {
    val in = new DataInputStream(new ByteArrayInputStream(classBytes))
    in.readInt() // magic
    in.readUnsignedShort() // minor_version
    in.readUnsignedShort() // major_version
    val count = in.readUnsignedShort()
    val utf8 = mutable.Map.empty[Int, String]
    val classNames = mutable.Set.empty[Int]
    val literals = mutable.Set.empty[Int]
    var index = 1
    while (index < count) {
      in.readUnsignedByte() match {
        case 1 => utf8(index) = in.readUTF()
        case 7 => classNames += in.readUnsignedShort()
        case 8 => literals += in.readUnsignedShort()
        case 16 | 19 | 20 => in.readUnsignedShort()
        case 15 =>
          in.readUnsignedByte()
          in.readUnsignedShort()
        case 3 | 4 | 9 | 10 | 11 | 12 | 17 | 18 => in.readInt()
        // Long and Double take two constant pool slots.
        case 5 | 6 => in.readLong(); index += 1
        case tag => fail(s"Unknown constant pool tag $tag")
      }
      index += 1
    }
    utf8.collect {
      case (i, value) if classNames.contains(i) || !literals.contains(i) => value
    }.toSeq
  }

  private def withEntries[T](f: (JarFile, Seq[String]) => T): T = {
    assumeAssemblyBuilt()
    Using.resource(new JarFile(runtimeJar())) { jar =>
      f(jar, jar.entries().asScala.map(_.getName).toSeq)
    }
  }

  test("runtime jar carries the direct dispatcher and relocated gRPC and Guava") {
    withEntries { (_, entries) =>
      Seq(
        "org/apache/spark/udf/worker/grpc/DirectDispatcherFactory.class",
        "org/apache/spark/udf/worker/UdfWorkerGrpc.class",
        s"${relocatedPrefix}grpc/ManagedChannel.class",
        s"${relocatedPrefix}grpc/netty/NettyChannelBuilder.class",
        s"${relocatedPrefix}grpc/protobuf/ProtoUtils.class",
        s"${relocatedPrefix}grpc/stub/StreamObserver.class",
        s"${relocatedPrefix}guava/util/concurrent/ListenableFuture.class",
        s"${relocatedPrefix}io_perfmark/PerfMark.class"
      ).foreach { name =>
        assert(entries.contains(name), s"$name is missing from the runtime jar")
      }
    }
  }

  test("no other jar in the jars directory carries the direct dispatcher") {
    assumeAssemblyBuilt()
    val runtime = runtimeJar()
    val dispatcherClass = "org/apache/spark/udf/worker/grpc/DirectDispatcherFactory.class"
    val others = jarsDir.listFiles().filter { f =>
      f.getName.endsWith(".jar") && f != runtime &&
        Using.resource(new JarFile(f))(_.getEntry(dispatcherClass) != null)
    }
    assert(others.isEmpty,
      s"$dispatcherClass is also in: ${others.map(_.getName).mkString(", ")}")
  }

  test("runtime jar has no unrelocated gRPC or Guava, Netty, protobuf or worker core") {
    withEntries { (_, entries) =>
      val unexpected = entries.filterNot(_.endsWith("/")).filter { name =>
        (unrelocatedPackages ++ excludedPackages).exists(name.startsWith) ||
          // The proto module's message classes, such as UDFWorkerSpecification. The
          // generated gRPC stubs share their package but belong to this jar.
          (name.startsWith(protoPackage) && name.endsWith(".class") &&
            name.indexOf('/', protoPackage.length) < 0 &&
            !name.startsWith(s"${protoPackage}UdfWorkerGrpc"))
      }
      assert(unexpected.isEmpty, s"Unexpected runtime jar entries:\n${unexpected.mkString("\n")}")
    }
  }

  test("no runtime jar class links to an unrelocated gRPC or Guava class") {
    withEntries { (jar, entries) =>
      val offenders = entries.filter(_.endsWith(".class")).flatMap { name =>
        val bytes = Using.resource(jar.getInputStream(jar.getEntry(name)))(_.readAllBytes())
        typeNames(bytes).flatMap { typeName =>
          unrelocatedPackages
            .filter(pkg => typeName.startsWith(pkg) || typeName.contains(s"L$pkg"))
            .map(pkg => s"$name -> $typeName")
        }
      }
      assert(offenders.isEmpty, s"Unrelocated references:\n${offenders.mkString("\n")}")
    }
  }

  test("gRPC service provider files are relocated with the classes they name") {
    withEntries { (jar, entries) =>
      val services = entries.filter(_.startsWith("META-INF/services/"))
      val grpcServices = services.filter(_.contains("grpc"))
      assert(grpcServices.nonEmpty, s"No gRPC service files in: ${services.mkString(", ")}")
      grpcServices.foreach { service =>
        val serviceName = service.stripPrefix("META-INF/services/")
        assert(serviceName.startsWith("org.sparkproject.spark_udf_worker.grpc."),
          s"$service names an unrelocated service")
        val providers = Using.resource(jar.getInputStream(jar.getEntry(service))) { in =>
          new String(in.readAllBytes(), StandardCharsets.UTF_8)
        }.linesIterator.map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#")).toSeq
        assert(providers.nonEmpty, s"$service lists no providers")
        providers.foreach { provider =>
          assert(provider.startsWith("org.sparkproject.spark_udf_worker.grpc."),
            s"$service lists the unrelocated provider $provider")
          val classFile = provider.replace('.', '/') + ".class"
          assert(entries.contains(classFile), s"$service lists $provider, which is missing")
        }
      }
      assert(grpcServices.exists(_.endsWith(".grpc.LoadBalancerProvider")),
        s"Missing the LoadBalancerProvider service file: ${grpcServices.mkString(", ")}")
    }
  }
}

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

import java.io.File
import java.nio.file.Files

import scala.util.Properties

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.ProtoDataTypes
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}
import org.apache.spark.util.SparkTestUtils

class UDFClassLoadingE2ESuite extends ConnectFunSuite with RemoteSparkSession {

  private val scalaVersion = Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  // Compile StubClassDummyUdf.scala and StubClassDummyUdfPacker.scala into a JAR,
  // then run the Packer to generate the serialized UdfPacket binary.
  private lazy val (udfByteArray: Array[Byte], udfJarFile: File) = {
    val resDir = new File("src/test/resources")
    val sources = Seq(
      new File(resDir, "StubClassDummyUdf.scala"),
      new File(resDir, "StubClassDummyUdfPacker.scala"))
    val tmpDir = Files.createTempDirectory("spark-udf-test").toFile
    val jarFile = new File(tmpDir, s"udf$scalaVersion.jar")
    val udfFile = new File(tmpDir, s"udf$scalaVersion")
    val cp = System
      .getProperty("java.class.path")
      .split(File.pathSeparator)
      .map(p => new File(p).toURI.toURL)
      .toSeq
    SparkTestUtils.createJarWithScalaSources(sources, jarFile, cp)
    // Run the Packer to generate the serialized UdfPacket binary.
    val loader = new java.net.URLClassLoader(Array(jarFile.toURI.toURL), getClass.getClassLoader)
    val packerClass = loader
      .loadClass("org.apache.spark.sql.connect.client.StubClassDummyUdfPacker")
    packerClass
      .getMethod("main", classOf[Array[String]])
      .invoke(null, Array(udfFile.getAbsolutePath))
    (Files.readAllBytes(udfFile.toPath), jarFile)
  }
  private lazy val udfJar = udfJarFile.toURI.toURL

  private def registerUdf(session: SparkSession): Unit = {
    val builder = proto.CommonInlineUserDefinedFunction
      .newBuilder()
      .setDeterministic(true)
      .setFunctionName("dummyUdf")
    builder.getScalarScalaUdfBuilder
      .setPayload(ByteString.copyFrom(udfByteArray))
      .addInputTypes(ProtoDataTypes.IntegerType)
      .setOutputType(ProtoDataTypes.IntegerType)
      .setNullable(true)
      .setAggregate(false)
    session.registerUdf(builder.build())
  }

  test("update class loader after stubbing: new session") {
    // Session1 should stub the missing class, but fail to call methods on it
    val session1 = spark.newSession()

    assert(
      intercept[Exception] {
        registerUdf(session1)
      }.getMessage.contains(
        "java.lang.NoSuchMethodException: org.apache.spark.sql.connect.client.StubClassDummyUdf"))

    // Session2 uses the real class
    val session2 = spark.newSession()
    session2.addArtifact(udfJar.toURI)
    registerUdf(session2)
  }

  test("update class loader after stubbing: same session") {
    // Session should stub the missing class, but fail to call methods on it
    val session = spark.newSession()

    assert(
      intercept[Exception] {
        registerUdf(session)
      }.getMessage.contains(
        "java.lang.NoSuchMethodException: org.apache.spark.sql.connect.client.StubClassDummyUdf"))

    // Session uses the real class
    session.addArtifact(udfJar.toURI)
    registerUdf(session)
  }
}

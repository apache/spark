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

package org.apache.spark.sql.protobuf

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Files

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.ProtobufDescriptorFileReader
import org.apache.spark.sql.protobuf.utils.{ProtobufUtils => ConnectorProtobufUtils}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Regression tests for reading the SQL from_protobuf/to_protobuf descriptor-file argument via the
 * Hadoop FileSystem (SPARK-58109). A `file:`-scheme path exercises the Hadoop path; a scheme-less
 * path exercises the local fallback. Distributed filesystems (hdfs:/s3:/abfss:) can't run in a
 * hermetic test, but all schemes flow through the same `path.getFileSystem(conf).open` call.
 */
class ProtobufDescriptorFileReadSuite extends SharedSparkSession with ProtobufTestBase {

  // Register a `mockfs://` FileSystem whose open() always throws, so tests can exercise the
  // reader's failure handling for an explicit-scheme path. Registered via the environment
  // SparkConf because the reader resolves its Hadoop conf from
  // `SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf)`, which does not observe post-startup
  // mutations of the runtime Hadoop configuration.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.hadoop.fs.mockfs.impl", classOf[ThrowingMockFileSystem].getName)

  private val descPath: String = protobufDescriptorFile("functions_suite.desc")
  private val messageName = "SimpleMessageJavaTypes"

  private def runFromProtobuf(pathArg: String): Boolean = {
    spark.range(1).selectExpr("CAST(NULL AS BINARY) AS value").createOrReplaceTempView("t_pb")
    spark.sql(
      s"SELECT from_protobuf(value, '$messageName', '$pathArg', map()) IS NULL AS r FROM t_pb")
      .head().getBoolean(0)
  }

  test("from_protobuf reads a file: scheme descriptor path") {
    val fileUri = new File(descPath).getCanonicalFile.toURI.toString
    assert(fileUri.startsWith("file:"))
    assert(runFromProtobuf(fileUri))
  }

  test("from_protobuf reads a scheme-less local descriptor path") {
    val local = new File(descPath).getCanonicalPath
    assert(new File(local).exists())
    assert(runFromProtobuf(local))
  }

  test("to_protobuf reads a file: scheme descriptor path") {
    val fileUri = new File(descPath).getCanonicalFile.toURI.toString
    spark.range(1)
      .selectExpr("named_struct('id', id) AS value")
      .createOrReplaceTempView("t_pb_struct")
    // Proves to_protobuf reads the descriptor via a file: URI and returns non-null encoded bytes.
    val r = spark.sql(
      s"SELECT to_protobuf(value, '$messageName', '$fileUri', map()) IS NOT NULL AS r " +
      "FROM t_pb_struct").head().getBoolean(0)
    assert(r)
  }

  test("missing descriptor file raises PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND") {
    val missing = new File(Files.createTempDirectory("pb").toFile, "does-not-exist.desc")
      .getCanonicalFile.toURI.toString
    checkError(
      exception = intercept[AnalysisException](runFromProtobuf(missing)),
      condition = "PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND",
      parameters = Map("filePath" -> missing))
  }

  test("to_protobuf with an empty descriptor path raises CANNOT_PARSE_PROTOBUF_DESCRIPTOR") {
    // An empty path (new Path("") throws) must surface as CANNOT_PARSE_PROTOBUF_DESCRIPTOR,
    // not a raw internal error. to_protobuf, unlike from_protobuf, has no empty-path guard.
    spark.range(1)
      .selectExpr("named_struct('id', id) AS value")
      .createOrReplaceTempView("t_pb_struct_empty")
    checkError(
      exception = intercept[org.apache.spark.sql.AnalysisException] {
        spark.sql(
          s"SELECT to_protobuf(value, '$messageName', '', map()) FROM t_pb_struct_empty"
        ).head()
      },
      condition = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR")
  }

  test("invalid descriptor bytes raise CANNOT_PARSE_PROTOBUF_DESCRIPTOR") {
    // A .proto source file is not a serialized FileDescriptorSet, so buildDescriptor rejects it.
    // Asserted at the buildDescriptor layer (not via SQL) because the SQL path defers the parse to
    // execution, where it surfaces as SparkException rather than AnalysisException.
    val bogusBytes =
      Files.readAllBytes(new File("src/test/resources/protobuf/catalyst_types.proto").toPath)
    checkError(
      exception = intercept[AnalysisException](
        ConnectorProtobufUtils.buildDescriptor("SomeMessage", Some(bogusBytes))),
      condition = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR")
  }

  test("invalid descriptor bytes read via a file: path raise CANNOT_PARSE through SQL") {
    // Complements the buildDescriptor-layer test above: this drives the full SQL path with a real
    // file: descriptor path, so the bytes are actually read by the reader and then handed to
    // buildDescriptor -- which rejects the non-FileDescriptorSet bytes with
    // CANNOT_PARSE_PROTOBUF_DESCRIPTOR. This proves the Hadoop-read bytes reach buildDescriptor.
    // A non-NULL input value is used so the descriptor is actually evaluated (with NULL input the
    // descriptor-building lazy val is never forced and the error would not surface).
    val bogus = new File("src/test/resources/protobuf/catalyst_types.proto")
      .getCanonicalFile.toURI.toString
    spark.range(1).selectExpr("CAST(X'00' AS BINARY) AS value").createOrReplaceTempView("t_pb_bad")
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(
          s"SELECT from_protobuf(value, '$messageName', '$bogus', map()) FROM t_pb_bad").collect()
      },
      condition = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR")
  }

  test("reader routes a missing file: path to PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND") {
    // Direct unit test of the reader's FileNotFoundException routing for an explicit-scheme path
    // (no fallback: an explicit scheme is not eligible for the local read).
    val missing = new File(Files.createTempDirectory("pb").toFile, "nope.desc")
      .getCanonicalFile.toURI.toString
    checkError(
      exception = intercept[AnalysisException](
        ProtobufDescriptorFileReader.readDescriptorFileContent(missing)),
      condition = "PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND",
      parameters = Map("filePath" -> missing))
  }

  test("reader does NOT fall back for an explicit-scheme path; the real error is chained") {
    // The mockfs FileSystem (registered via sparkConf) throws on open. A mockfs:// path is
    // explicit-scheme, so the reader must NOT retry a local NIO read of the bogus path; it surfaces
    // CANNOT_PARSE_PROTOBUF_DESCRIPTOR and preserves the underlying IOException as the cause.
    val ex = intercept[AnalysisException] {
      ProtobufDescriptorFileReader.readDescriptorFileContent(s"mockfs://$descPath")
    }
    checkError(exception = ex, condition = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR")
    // The underlying FileSystem exception must be preserved as the cause (not swallowed).
    assert(ex.getCause != null)
  }
}

/** A FileSystem for the `mockfs` scheme whose `open` always throws, to exercise read failures. */
class ThrowingMockFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "mockfs"
  override def getUri: URI = URI.create("mockfs:///")
  override def open(f: Path, bufferSize: Int): org.apache.hadoop.fs.FSDataInputStream =
    throw new IOException("mockfs open failure")
}

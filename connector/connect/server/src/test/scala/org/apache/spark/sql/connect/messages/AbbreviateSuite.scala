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
package org.apache.spark.sql.connect.messages

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.{ProtoDataTypes, ProtoUtils}

class AbbreviateSuite extends SparkFunSuite {

  test("truncate string: simple SQL text") {
    val message = proto.SQL.newBuilder().setQuery("x" * 1024).build()

    Seq(1, 16, 256, 512, 1024, 2048).foreach { threshold =>
      val truncated = ProtoUtils.abbreviate(message, threshold)
      assert(truncated.isInstanceOf[proto.SQL])
      val truncatedSQL = truncated.asInstanceOf[proto.SQL]

      if (threshold < 1024) {
        assert(truncatedSQL.getQuery.indexOf("[truncated") === threshold)
      } else {
        assert(truncatedSQL.getQuery.indexOf("[truncated") === -1)
        assert(truncatedSQL.getQuery.length === 1024)
      }
    }
  }

  test("truncate string: nested message") {
    val sql = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("x" * 1024)
          .build())
      .build()
    val drop = proto.Relation
      .newBuilder()
      .setDrop(
        proto.Drop
          .newBuilder()
          .setInput(sql)
          .addAllColumnNames(Seq("a", "b").asJava)
          .build())
      .build()
    val limit = proto.Relation
      .newBuilder()
      .setLimit(
        proto.Limit
          .newBuilder()
          .setInput(drop)
          .setLimit(100)
          .build())
      .build()

    Seq(1, 16, 256, 512, 1024, 2048).foreach { threshold =>
      val truncated = ProtoUtils.abbreviate(limit, threshold)
      assert(truncated.isInstanceOf[proto.Relation])

      val truncatedLimit = truncated.asInstanceOf[proto.Relation].getLimit
      assert(truncatedLimit.getLimit === 100)

      val truncatedDrop = truncatedLimit.getInput.getDrop
      assert(truncatedDrop.getColumnNamesList.asScala.toSeq === Seq("a", "b"))

      val truncatedSQL = truncatedDrop.getInput.getSql

      if (threshold < 1024) {
        assert(truncatedSQL.getQuery.indexOf("[truncated") === threshold)
      } else {
        assert(truncatedSQL.getQuery.indexOf("[truncated") === -1)
        assert(truncatedSQL.getQuery.length === 1024)
      }
    }
  }

  test("truncate bytes: simple python udf") {
    Seq(1, 8, 16, 64, 256).foreach { numBytes =>
      val bytes = Array.ofDim[Byte](numBytes)
      val message = proto.PythonUDF
        .newBuilder()
        .setEvalType(1)
        .setOutputType(ProtoDataTypes.BinaryType)
        .setCommand(ByteString.copyFrom(bytes))
        .setPythonVer("3.12")
        .build()

      val truncated = ProtoUtils.abbreviate(message)
      assert(truncated.isInstanceOf[proto.PythonUDF])

      val truncatedUDF = truncated.asInstanceOf[proto.PythonUDF]
      assert(truncatedUDF.getEvalType === 1)
      assert(truncatedUDF.getOutputType === ProtoDataTypes.BinaryType)
      assert(truncatedUDF.getPythonVer === "3.12")

      if (numBytes <= 8) {
        assert(truncatedUDF.getCommand.size() === numBytes)
      } else {
        assert(truncatedUDF.getCommand.size() != numBytes)
      }
    }
  }

  test("truncate bytes with threshold: simple python udf") {
    val bytes = Array.ofDim[Byte](1024)
    val message = proto.PythonUDF
      .newBuilder()
      .setEvalType(1)
      .setOutputType(ProtoDataTypes.BinaryType)
      .setCommand(ByteString.copyFrom(bytes))
      .setPythonVer("3.12")
      .build()

    Seq(1, 16, 256, 512, 1024, 2048).foreach { threshold =>
      val truncated = ProtoUtils.abbreviate(message, Map("BYTES" -> threshold))
      assert(truncated.isInstanceOf[proto.PythonUDF])

      val truncatedUDF = truncated.asInstanceOf[proto.PythonUDF]
      assert(truncatedUDF.getEvalType === 1)
      assert(truncatedUDF.getOutputType === ProtoDataTypes.BinaryType)
      assert(truncatedUDF.getPythonVer === "3.12")

      if (threshold < 1024) {
        // with suffix: [truncated(size=...)]
        assert(
          threshold < truncatedUDF.getCommand.size() &&
            truncatedUDF.getCommand.size() < threshold + 64)
      } else {
        assert(truncatedUDF.getCommand.size() === 1024)
      }
    }
  }
}

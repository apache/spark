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

import scala.collection.JavaConverters._

import com.google.protobuf.DescriptorProtos.FileDescriptorSet

import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{DataType, StructType}

trait ProtobufTestBase extends SQLTestUtils {

  /**
   * Returns full path to the given file in the resource folder,
   * if the first choice throw NPE, try to return the full path of alternative.
   * The result path doesn't contain the `file:/` protocol part.
   */
  protected def testFile(fileName: String, alternateFileName: String): String = {
    s"target/generated-test-sources/$fileName"
    // XXX Remove alternateFileName arg?
  }

  protected def structFromDDL(ddl: String): StructType =
    DataType.fromDDL(ddl).asInstanceOf[StructType]

  /**
   * Returns a new binary descriptor set that contains single FileDescriptor that has
   * Protobuf message with the name `messageName`. It does not include any of its dependencies.
   * This roughly simulates a case where `--include_imports` is missing for `protoc` command that
   * generated the descriptor file. E.g.
   * {{ protoc --descriptor_set_out=my_protos.desc my_protos.proto }}
   */
  protected def descriptorSetWithoutImports(
    binaryDescriptorSet: Array[Byte],
    messageName: String): Array[Byte] = {

    val fdSet = FileDescriptorSet.parseFrom(binaryDescriptorSet)
    val fdForMessage = fdSet.getFileList.asScala.find { fd =>
      fd.getMessageTypeList.asScala.exists(_.getName == messageName)
    }

    fdForMessage match {
      case Some(fd) =>
        // Create a file descriptor with single FileDescriptor, no dependencies are included.
        FileDescriptorSet.newBuilder().addFile(fd).build().toByteArray()
      case None =>
        throw new RuntimeException(s"Could not find FileDescriptor for '$messageName'")
    }
  }
}

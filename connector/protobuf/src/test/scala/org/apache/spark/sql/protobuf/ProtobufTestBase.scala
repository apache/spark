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

import java.io.File

import scala.jdk.CollectionConverters._

import com.google.protobuf.DescriptorProtos.FileDescriptorSet

import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{DataType, StructType}

trait ProtobufTestBase extends SQLTestUtils {

  private val descriptorDir = getWorkspaceFilePath(
    "connector", "protobuf", "target", "generated-test-sources")

  /**
   * Returns path for a Protobuf descriptor file used in the tests. These files are generated
   * during the build. Maven and SBT create the descriptor files differently. Maven creates one
   * descriptor file for each Protobuf file, where as SBT create one descriptor file that includes
   * all the Protobuf files. As a result actual file path returned in each case is different.
   */
  protected def protobufDescriptorFile(fileName: String): String = {
    val dir = descriptorDir.toFile.getCanonicalPath
    if (new File(s"$dir/$fileName").exists) {
      s"$dir/$fileName"
    } else { // sbt test
      s"$dir/descriptor-set-sbt.desc"  // Single file contains all the proto files in sbt.
    }
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

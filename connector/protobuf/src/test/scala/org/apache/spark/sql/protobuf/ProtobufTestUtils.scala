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

import org.apache.spark.sql.test.SQLTestUtils

/**
 * A trait used by all the Protobuf function suites.
 */
private[sql] trait ProtobufTestUtils extends SQLTestUtils {

  val javaPackageV2 = "org.apache.spark.sql.protobuf.protos.v2"
  val javaPackageV3 = "org.apache.spark.sql.protobuf.protos.v3"

  def javaClassFullNameV2(outerClass: String, innerClass: String): String =
    s"$javaPackageV2.$outerClass$$$innerClass"

  def javaClassFullNameV3(outerClass: String, innerClass: String): String =
    s"$javaPackageV3.$outerClass$$$innerClass"

  def descFilePathV2(descFileName: String): String =
    testFile(s"descriptor-set-v2/$descFileName").replace("file:/", "/")

  def descFilePathV3(descFileName: String): String =
    testFile(s"descriptor-set-v3/$descFileName").replace("file:/", "/")
}

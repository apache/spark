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
package org.apache.spark.sql.connect.client

/**
 * Dummy UDF class used for testing dynamic class loading and stub class behavior.
 *
 * At test time, this file is compiled by UDFClassLoadingE2ESuite via
 * createJarWithScalaSources() to generate a JAR and a serialized UdfPacket binary.
 * StubClassDummyUdfPacker (in a separate file) handles the serialization.
 */
class StubClassDummyUdf {
  val udf: Int => Int = (x: Int) => x + 1
  val dummy = (x: Int) => A(x)
}

case class A(x: Int) { def get: Int = x + 5 }

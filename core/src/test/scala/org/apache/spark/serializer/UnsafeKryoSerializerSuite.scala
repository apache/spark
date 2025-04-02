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

package org.apache.spark.serializer

import org.apache.spark.internal.config.Kryo._

class UnsafeKryoSerializerSuite extends KryoSerializerSuite {

  // This test suite should run all tests in KryoSerializerSuite with kryo unsafe.

  override def beforeAll(): Unit = {
    conf.set(KRYO_USE_UNSAFE, true)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    conf.set(KRYO_USE_UNSAFE, false)
    super.afterAll()
  }
}

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

package org.apache.spark.sql.catalyst.util

import scala.util.Random

import org.apache.spark.SparkFunSuite

class RandomUUIDGeneratorSuite extends SparkFunSuite {
  test("RandomUUIDGenerator should generate version 4, variant 2 UUIDs") {
    val generator = RandomUUIDGenerator(new Random().nextLong())
    for (_ <- 0 to 100) {
      val uuid = generator.getNextUUID()
      assert(uuid.version() == 4)
      assert(uuid.variant() == 2)
    }
  }

 test("UUID from RandomUUIDGenerator should be deterministic") {
   val r1 = new Random(100)
   val generator1 = RandomUUIDGenerator(r1.nextLong())
   val r2 = new Random(100)
   val generator2 = RandomUUIDGenerator(r2.nextLong())
   val r3 = new Random(101)
   val generator3 = RandomUUIDGenerator(r3.nextLong())

   for (_ <- 0 to 100) {
      val uuid1 = generator1.getNextUUID()
      val uuid2 = generator2.getNextUUID()
      val uuid3 = generator3.getNextUUID()
      assert(uuid1 == uuid2)
      assert(uuid1 != uuid3)
   }
 }

 test("Get UTF8String UUID") {
   val generator = RandomUUIDGenerator(new Random().nextLong())
   val utf8StringUUID = generator.getNextUUIDUTF8String()
   val uuid = java.util.UUID.fromString(utf8StringUUID.toString)
   assert(uuid.version() == 4 && uuid.variant() == 2 && utf8StringUUID.toString == uuid.toString)
 }
}

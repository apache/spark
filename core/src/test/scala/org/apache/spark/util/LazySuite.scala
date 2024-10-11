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
package org.apache.spark.util

import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}

import org.apache.spark.SparkFunSuite

class LazySuite extends SparkFunSuite {
  test("Lazy val works") {
    var test: Option[Object] = None

    val lazyval = new Lazy({
      test = Some(new Object())
      test
    })

    // Ensure no initialization happened before the lazy value was dereferenced
    assert(test.isEmpty)

    // Ensure the first invocation creates a new object
    assert(lazyval() == test && test.isDefined)

    // Ensure the subsequent invocation serves the same object
    assert(lazyval() == test && test.isDefined)
  }

  test("Lazy val is serializable") {
    val lazyval = new Lazy({
      new Object()
    })

    // Ensure we are serializable before the dereference
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(lazyval)

    @SuppressWarnings(Array("never used"))
    val dereferenced = lazyval()

    // Ensure we are not serializable after the dereference (type "Object" is not serializable)
    intercept[NotSerializableException] {
      val oos2 = new ObjectOutputStream(new ByteArrayOutputStream())
      oos2.writeObject(lazyval)
    }
  }
}

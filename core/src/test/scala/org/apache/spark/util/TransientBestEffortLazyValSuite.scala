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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite

class TransientBestEffortLazyValSuite extends SparkFunSuite {

  test("TransientBestEffortLazyVal val works") {
    var test: Option[Object] = None

    val lazyval = new TransientBestEffortLazyVal(() => {
      test = Some(new Object())
      test
    })

    // Ensure no initialization happened before the lazy value was invoked
    assert(test.isEmpty)

    // Ensure the first invocation creates a new object
    assert(lazyval() == test && test.isDefined)

    // Ensure the subsequent invocation serves the same object
    assert(lazyval() == test && test.isDefined)
  }

  test("TransientBestEffortLazyVal val is serializable") {
    val lazyval = new TransientBestEffortLazyVal(() => "test")

    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(lazyval)

    val v = lazyval()
    val oos2 = new ObjectOutputStream(new ByteArrayOutputStream())
    oos2.writeObject(lazyval)
  }

  test("TransientBestEffortLazyVal val is serializable: unserializable value") {
    val lazyval = new TransientBestEffortLazyVal(() => new Object())

    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(lazyval)

    val v = lazyval()
    val oos2 = new ObjectOutputStream(new ByteArrayOutputStream())
    oos2.writeObject(lazyval)
  }

  test("TransientBestEffortLazyVal val is serializable: failure in compute function") {
    val lazyval = new TransientBestEffortLazyVal[String](() => throw new RuntimeException("test"))

    // BestEffortLazyVal serializes the compute function before first invocation
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(lazyval)

    val e = intercept[RuntimeException] {
      val v = lazyval()
    }
    assert(e.getMessage.contains("test"))

    // BestEffortLazyVal still serializes the compute function after initialization failure
    val oos2 = new ObjectOutputStream(new ByteArrayOutputStream())
    oos2.writeObject(lazyval)
  }
}

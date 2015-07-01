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

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class ObjectPoolSuite extends SparkFunSuite with Matchers {

  test("pool") {
    val pool = new ObjectPool(1)
    assert(pool.put(1) === 0)
    assert(pool.put("hello") === 1)
    assert(pool.put(false) === 2)

    assert(pool.get(0) === 1)
    assert(pool.get(1) === "hello")
    assert(pool.get(2) === false)
    assert(pool.size() === 3)

    pool.replace(1, "world")
    assert(pool.get(1) === "world")
    assert(pool.size() === 3)
  }

  test("unique pool") {
    val pool = new UniqueObjectPool(1)
    assert(pool.put(1) === 0)
    assert(pool.put("hello") === 1)
    assert(pool.put(1) === 0)
    assert(pool.put("hello") === 1)

    assert(pool.get(0) === 1)
    assert(pool.get(1) === "hello")
    assert(pool.size() === 2)

    intercept[UnsupportedOperationException] {
      pool.replace(1, "world")
    }
  }
}

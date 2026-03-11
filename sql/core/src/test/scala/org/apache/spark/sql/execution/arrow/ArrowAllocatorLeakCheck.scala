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

package org.apache.spark.sql.execution.arrow

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.apache.spark.sql.util.ArrowUtils

/**
 * Mixin that asserts no memory remains allocated in the Arrow rootAllocator after all
 * tests complete. Mix into any suite that uses ArrowUtils.rootAllocator to catch leaks.
 */
trait ArrowAllocatorLeakCheck extends Suite with BeforeAndAfterAll {
  abstract override def afterAll(): Unit = {
    super.afterAll()
    val leaked = ArrowUtils.rootAllocator.getAllocatedMemory
    assert(leaked == 0, s"Arrow rootAllocator memory leak: $leaked bytes still allocated")
  }
}

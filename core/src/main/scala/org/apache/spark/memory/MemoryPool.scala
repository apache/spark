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

package org.apache.spark.memory

abstract class MemoryPool {

  private[this] var _poolSize: Long = 0

  final def poolSize: Long = _poolSize
  final def memoryFree: Long = _poolSize - memoryUsed
  def memoryUsed: Long

  def incrementPoolSize(delta: Long): Unit = {
    require(delta >= 0)
    _poolSize += delta
  }

  def decrementPoolSize(delta: Long): Unit = {
    require(delta >= 0)
    require(delta <= _poolSize)
    _poolSize -= delta
  }
}

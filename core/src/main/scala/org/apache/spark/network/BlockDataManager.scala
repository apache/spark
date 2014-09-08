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

package org.apache.spark.network

import org.apache.spark.storage.StorageLevel


trait BlockDataManager {

  /**
   * Interface to get local block data.
   *
   * @return Some(buffer) if the block exists locally, and None if it doesn't.
   */
  def getBlockData(blockId: String): Option[ManagedBuffer]

  /**
   * Put the block locally, using the given storage level.
   */
  def putBlockData(blockId: String, data: ManagedBuffer, level: StorageLevel): Unit
}

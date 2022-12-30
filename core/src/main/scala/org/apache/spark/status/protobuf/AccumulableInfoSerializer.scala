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

package org.apache.spark.status.protobuf

import java.util.{List => JList}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.status.api.v1.AccumulableInfo
import org.apache.spark.status.protobuf.Utils.getOptional

private[protobuf] object AccumulableInfoSerializer {

  def serialize(input: AccumulableInfo): StoreTypes.AccumulableInfo = {
    val builder = StoreTypes.AccumulableInfo.newBuilder()
      .setId(input.id)
      .setName(input.name)
      .setValue(input.value)
    input.update.foreach(builder.setUpdate)
    builder.build()
  }

  def deserialize(updates: JList[StoreTypes.AccumulableInfo]): ArrayBuffer[AccumulableInfo] = {
    val accumulatorUpdates = new ArrayBuffer[AccumulableInfo](updates.size())
    updates.forEach { update =>
      accumulatorUpdates.append(new AccumulableInfo(
        id = update.getId,
        name = update.getName,
        update = getOptional(update.hasUpdate, update.getUpdate),
        value = update.getValue))
    }
    accumulatorUpdates
  }
}

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
package org.apache.spark.sql.execution.streaming

import java.io.Serializable

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.ValueState
import org.apache.spark.sql.types._

class ValueStateImpl[S](
    store: StateStore,
    stateName: String) extends ValueState[S] with Logging{

  private def encodeKey(): UnsafeRow = {
    val keyOption = ImplicitKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw new UnsupportedOperationException("Implicit key not found for operation on" +
        s"stateName=$stateName")
    }

    val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
    val keyByteArr = SerializationUtils.serialize(keyOption.get.asInstanceOf[Serializable])
    val keyEncoder = UnsafeProjection.create(schemaForKeyRow)
    val keyRow = keyEncoder(InternalRow(keyByteArr))
    keyRow
  }

  private def encodeValue(value: S): UnsafeRow = {
    val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueEncoder = UnsafeProjection.create(schemaForValueRow)
    val valueRow = valueEncoder(InternalRow(valueByteArr))
    valueRow
  }

  override def exists(): Boolean = {
    val retRow = store.get(encodeKey(), stateName)
    if (retRow == null) {
      false
    } else {
      true
    }
  }

  override def getOption(): Option[S] = {
    if (exists()) {
      Some(get())
    } else {
      None
    }
  }

  override def get(): S = {
    val retRow = store.get(encodeKey(), stateName)

    val resState = SerializationUtils
      .deserialize(retRow.getBinary(0))
      .asInstanceOf[S]
    resState
  }

  override def update(newState: S): Unit = {
    store.put(encodeKey(), encodeValue(newState), stateName)
  }

  override def remove(): Unit = {
    store.remove(encodeKey(), stateName)
  }
}

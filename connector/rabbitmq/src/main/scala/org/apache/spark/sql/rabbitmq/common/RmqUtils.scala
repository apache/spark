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
package org.apache.spark.sql.rabbitmq.common

import com.rabbitmq.stream.Message
import com.rabbitmq.stream.MessageHandler.Context
import org.apache.qpid.proton.amqp.messaging.Data

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.unsafe.types.UTF8String

object RmqUtils extends Logging {
  def toInternalRow(message: Message, context: Context): InternalRow = {
    val routingKey: UTF8String = getRoutingKey(message)
    val headers: ArrayBasedMapData = getHeaders(message).orNull
    val body: UTF8String = getBody(message)
    InternalRow(routingKey, headers, body, context.timestamp() * 1000) // ms
  }

  private def getBody(message: Message): UTF8String = {
    message.getBody match {
      case data: Data => UTF8String.fromBytes(data.getValue.getArray)
      case other =>
        logWarning(s"[RMQ] getBody: unexpected type=${
          Option(other)
            .map(_.getClass.getName).orNull
        }, returning null")
        null
    }
  }


  private def getHeaders(message: Message): Option[ArrayBasedMapData] = {
    Option(message.getApplicationProperties).map { props =>
      val tuples = props.entrySet().stream().map { e =>
        (UTF8String.fromString(e.getKey), UTF8String.fromString(String.valueOf(e.getValue)))
      }
      val keys = ArrayData.toArrayData(tuples.map(_._1).toArray)
      val vals = ArrayData.toArrayData(tuples.map(_._2).toArray)
      new ArrayBasedMapData(keys, vals)
    }
  }


  private def getRoutingKey(message: Message) = {
    val routingKey: UTF8String = message.getMessageAnnotations.get("x-routing-key") match {
      case null => null
      case key: String => UTF8String.fromString(key)
      case other =>
        logWarning(s"[RMQ] getRoutingKey: unexpected type=${other.getClass.getName}," +
          s" value=$other; returning null")
        null
      case _ => null
    }
    routingKey
  }


}

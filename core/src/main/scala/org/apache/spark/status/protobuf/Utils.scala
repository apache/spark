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

import java.util.{Map => JMap}

private[protobuf] object Utils {
  def getOptional[T](condition: Boolean, result: () => T): Option[T] = if (condition) {
    Some(result())
  } else {
    None
  }

  def setStringField(input: String, f: String => Any): Unit = {
    if (input != null) {
      f(input)
    }
  }

  def getStringField(condition: Boolean, result: () => String): String = if (condition) {
    result()
  } else {
    null
  }

  def setJMapField[K, V](input: JMap[K, V], putAllFunc: JMap[K, V] => Any): Unit = {
    if (input != null && !input.isEmpty) {
      putAllFunc(input)
    }
  }
}

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

package org.apache.spark.serializer

import java.util.concurrent.ConcurrentHashMap


/**
 * A service that returns a serializer object given the serializer's class name. If a previous
 * instance of the serializer object has been created, the get method returns that instead of
 * creating a new one.
 */
private[spark] class SerializerManager {

  private val serializers = new ConcurrentHashMap[String, Serializer]
  private var _default: Serializer = _

  def default = _default

  def setDefault(clsName: String): Serializer = {
    _default = get(clsName)
    _default
  }

  def get(clsName: String): Serializer = {
    if (clsName == null) {
      default
    } else {
      var serializer = serializers.get(clsName)
      if (serializer != null) {
        // If the serializer has been created previously, reuse that.
        serializer
      } else this.synchronized {
        // Otherwise, create a new one. But make sure no other thread has attempted
        // to create another new one at the same time.
        serializer = serializers.get(clsName)
        if (serializer == null) {
          val clsLoader = Thread.currentThread.getContextClassLoader
          serializer =
            Class.forName(clsName, true, clsLoader).newInstance().asInstanceOf[Serializer]
          serializers.put(clsName, serializer)
        }
        serializer
      }
    }
  }
}

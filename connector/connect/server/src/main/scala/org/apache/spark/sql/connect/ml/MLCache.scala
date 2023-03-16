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
package org.apache.spark.sql.connect.ml

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.ml.Model

/**
 * This class is for managing server side object that is used by spark connect client side code.
 */
class ObjectCache[T](
    val objectMap: ConcurrentHashMap[String, T] = new ConcurrentHashMap[String, T]()
) {
  def register(obj: T): String = {
    val objectId = UUID.randomUUID().toString.takeRight(12)
    objectMap.put(objectId, obj)
    objectId
  }

  def get(id: String): T = objectMap.get(id)

  def remove(id: String): T = objectMap.remove(id)
}

class ModelCache(
    val cachedModel: ObjectCache[Model[_]] = new ObjectCache[Model[_]](),
    val modelToHandlerMap: ConcurrentHashMap[String, Algorithm] =
      new ConcurrentHashMap[String, Algorithm]()) {
  def register(model: Model[_], algorithm: Algorithm): String = {
    val refId = cachedModel.register(model)
    modelToHandlerMap.put(refId, algorithm)
    refId
  }

  def get(refId: String): (Model[_], Algorithm) = {
    (cachedModel.get(refId), modelToHandlerMap.get(refId))
  }

  def remove(refId: String): Unit = {
    cachedModel.remove(refId)
    modelToHandlerMap.remove(refId)
  }
}

case class MLCache(modelCache: ModelCache = new ModelCache())

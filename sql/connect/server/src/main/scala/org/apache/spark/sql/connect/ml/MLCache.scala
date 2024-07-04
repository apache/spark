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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model

// TODO need to support persistence for model if memory is tight
// TODO do we need to support cache estimator or evaluator?
class MLCache(
    private val cachedModel: ConcurrentHashMap[String, Model[_]] =
      new ConcurrentHashMap[String, Model[_]]())
    extends Logging {

  def register(model: Model[_]): String = {
    val objectId = UUID.randomUUID().toString.takeRight(12)
    cachedModel.put(objectId, model)
    objectId
  }

  def get(refId: String): Model[_] = {
    cachedModel.get(refId)
  }

  def remove(refId: String): Unit = {
    cachedModel.remove(refId)
  }

  def clear(): Unit = {
    cachedModel.clear()
  }
}

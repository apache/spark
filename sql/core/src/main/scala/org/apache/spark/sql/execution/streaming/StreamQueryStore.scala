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

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.streaming.StreamingQuery

/**
 * A class that holds and manages [[StreamingQuery]].
 */
private[sql] class StreamQueryStore {
  private val lock = new Object
  private val cache = new mutable.HashMap[UUID, (StreamingQuery, Long)]()

  def addStreamQuery(query: StreamingQuery): Unit = {
    lock.synchronized {
      if (!cache.contains(query.id)) {
        val curTime = System.currentTimeMillis()
        cache.put(query.id, (query, curTime))
      }
    }
  }

  def existingStreamQueries: Seq[(StreamingQuery, Long)] = {
    lock.synchronized {
      cache.values.toSeq
    }
  }
}

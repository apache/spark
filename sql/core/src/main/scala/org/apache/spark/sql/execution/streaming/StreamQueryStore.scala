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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.streaming.StreamingQuery

/**
 * A class that holds [[StreamingQuery]] active across all sessions to manage the lifecycle
 * of the stream.
 */
private[sql] class StreamQueryStore {
  private val activeStreamingQueries = new ConcurrentHashMap[UUID, (StreamingQuery, Long)]()
  // There maybe more than one inactive stream query with same query ID, as we can run same
  // stream query many times after it failed or terminated.
  private val inactiveStreamingQueries = new ConcurrentHashMap[(UUID, Long), StreamingQuery]()

  def putActive(query: StreamingQuery): StreamingQuery = {
    val curTime = System.currentTimeMillis()
    val prevQueryAndTime = activeStreamingQueries.put(query.id, (query, curTime))
    if (prevQueryAndTime == null) {
      // if `prevQuery` is null, it indicates this query start at first time or restart again.
      // So it is safe to remove this query from `inactiveStreamingQueries`
      val candidates = inactiveStreamingQueries.asScala.toSeq.filter { case ((uuid, _), _) =>
        uuid.equals(query.id)
      }
      candidates.foreach { case ((id, ts), _) => inactiveStreamingQueries.remove((id, ts)) }
      null
    } else {
      prevQueryAndTime._1
    }
  }

  def getActive(id: UUID): StreamingQuery = {
    val queryAndTime = activeStreamingQueries.get(id)
    if (queryAndTime == null) {
      null
    } else {
      queryAndTime._1
    }
  }

  def terminate(id: UUID): Unit = {
    val query = activeStreamingQueries.remove(id)
    if (query != null) {
      inactiveStreamingQueries.put((id, query._2), query._1)
    }
  }

  def allStreamQueries: Seq[(StreamingQuery, Long)] = {
    activeStreamingQueries.asScala.toSeq.map(_._2) ++
      inactiveStreamingQueries.asScala.toSeq.map { case ((_, startTime), query) =>
        (query, startTime)
      }
  }

  // only for test
  private[sql] def getActiveQueries: Seq[StreamingQuery] = {
    activeStreamingQueries.asScala.map(_._2._1).toSeq
  }

  // only for test
  private[sql] def getInactiveQueries: Seq[StreamingQuery] = {
    inactiveStreamingQueries.asScala.values.toSeq
  }
}

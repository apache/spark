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

package org.apache.spark.sql.internal

import java.lang.ref.Cleaner

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

private[sql] class SessionCleaner(session: SparkSession) extends Logging {
  private val cleaner = Cleaner.create()

  /** Register a CachedRemoteRelation for cleanup when it is garbage collected. */
  def register(relation: proto.CachedRemoteRelation): Unit = {
    val dfID = relation.getRelationId
    cleaner.register(relation, () => doCleanupCachedRemoteRelation(dfID))
  }

  private[sql] def doCleanupCachedRemoteRelation(dfID: String): Unit = {
    try {
      if (!session.client.channel.isShutdown) {
        session.execute {
          session.newCommand { builder =>
            builder.getRemoveCachedRemoteRelationCommandBuilder
              .setRelation(proto.CachedRemoteRelation.newBuilder().setRelationId(dfID).build())
          }
        }
      }
    } catch {
      case e: Throwable => logError("Error in cleaning thread", e)
    }
  }
}

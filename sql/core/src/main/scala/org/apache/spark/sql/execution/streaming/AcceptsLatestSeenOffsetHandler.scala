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

import org.apache.spark.sql.connector.read.streaming.{AcceptsLatestSeenOffset, SparkDataStream}

/**
 * This feeds "latest seen offset" to the sources that implement AcceptsLatestSeenOffset.
 */
object AcceptsLatestSeenOffsetHandler {
  def setLatestSeenOffsetOnSources(
      offsets: Option[OffsetSeq],
      sources: Seq[SparkDataStream]): Unit = {
    assertNoAcceptsLatestSeenOffsetWithDataSourceV1(sources)

    offsets.map(_.toStreamProgress(sources)) match {
      case Some(streamProgress) =>
        streamProgress.foreach {
          case (src: AcceptsLatestSeenOffset, offset) =>
            src.setLatestSeenOffset(offset)

          case _ => // no-op
        }
      case _ => // no-op
    }
  }

  private def assertNoAcceptsLatestSeenOffsetWithDataSourceV1(
      sources: Seq[SparkDataStream]): Unit = {
    val unsupportedSources = sources
      .filter(_.isInstanceOf[AcceptsLatestSeenOffset])
      .filter(_.isInstanceOf[Source])

    if (unsupportedSources.nonEmpty) {
      throw new UnsupportedOperationException(
        "AcceptsLatestSeenOffset is not supported with DSv1 streaming source: " +
          unsupportedSources)
    }
  }
}

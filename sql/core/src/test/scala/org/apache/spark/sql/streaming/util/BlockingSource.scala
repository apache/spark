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

package org.apache.spark.sql.streaming.util

import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Sink, Source}
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** Dummy provider: returns a SourceProvider with a blocking `createSource` call. */
class BlockingSource extends StreamSourceProvider with StreamSinkProvider {

  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    ("dummySource", fakeSchema)
  }

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    BlockingSource.latch.await()
    new Source {
      override def schema: StructType = fakeSchema
      override def getOffset: Option[Offset] = Some(new LongOffset(0))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        import spark.sparkSession.implicits._
        Seq[Int]().toDS().toDF()
      }
      override def stop(): Unit = {}
    }
  }

  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = (_: Long, _: DataFrame) => {}
}

object BlockingSource {
  var latch: CountDownLatch = null
}

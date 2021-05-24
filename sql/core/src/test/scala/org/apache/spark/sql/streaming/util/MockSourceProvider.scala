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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * A StreamSourceProvider that provides mocked Sources for unit testing. Example usage:
 *
 * {{{
 *    MockSourceProvider.withMockSources(source1, source2) {
 *      val df1 = spark.readStream
 *        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
 *        .load()
 *
 *      val df2 = spark.readStream
 *        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
 *        .load()
 *
 *      df1.union(df2)
 *      ...
 *    }
 * }}}
 */
class MockSourceProvider extends StreamSourceProvider {
  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    ("dummySource", MockSourceProvider.fakeSchema)
  }

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    MockSourceProvider.sourceProviderFunction()
  }
}

object MockSourceProvider {
  // Function to generate sources. May provide multiple sources if the user implements such a
  // function.
  private var sourceProviderFunction: () => Source = _

  final val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  def withMockSources(source: Source, otherSources: Source*)(f: => Unit): Unit = {
    var i = 0
    val sources = source +: otherSources
    sourceProviderFunction = () => {
      val source = sources(i % sources.length)
      i += 1
      source
    }
    try {
      f
    } finally {
      sourceProviderFunction = null
    }
  }
}

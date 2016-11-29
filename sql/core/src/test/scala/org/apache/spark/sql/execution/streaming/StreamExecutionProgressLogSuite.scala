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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class StreamExecutionProgressLogSuite extends SparkFunSuite with SharedSQLContext {

  test("serialization - deserialization") {
    withStreamExecutionProgressLog { sepLog =>
      val sep = new StreamExecutionProgress(
        OffsetSeq.fill(),
        100
      )
      val expected = s"""${StreamExecutionProgressLog.VERSION}
        |{
        |  "offsets" : [ ],
        |  "watermark" : 100
        |}""".stripMargin
      val baos = new ByteArrayOutputStream()
      sepLog.serialize(sep, baos)
      assert(expected === baos.toString(UTF_8.name()))
      val deserialized = sepLog.deserialize(new ByteArrayInputStream(baos.toByteArray))
      assert(sep === deserialized)
    }

    withStreamExecutionProgressLog { sepLog =>
      val sep = new StreamExecutionProgress(
        OffsetSeq.fill(LongOffset(1), null, null, LongOffset(2)),
        200
      )
      val expected = s"""${StreamExecutionProgressLog.VERSION}
        |{
        |  "offsets" : [ "1", "-", "-", "2" ],
        |  "watermark" : 200
        |}""".stripMargin
      val baos = new ByteArrayOutputStream()
      sepLog.serialize(sep, baos)
      assert(expected === baos.toString(UTF_8.name()))
      val deserialized = sepLog.deserialize(new ByteArrayInputStream(baos.toByteArray))
      assert(sep === deserialized)
    }
  }

  private def withStreamExecutionProgressLog(f: StreamExecutionProgressLog => Unit): Unit = {
    withTempDir { file =>
      val progressSink = new StreamExecutionProgressLog(spark, file.getCanonicalPath)
      f(progressSink)
    }
  }
}

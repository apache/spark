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

package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{DeserializerBuildHelper, SerializerBuildHelper}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ObjectType, TimeType}

/**
 * Tests for the Types Framework wiring of TimeType.
 *
 * The TIME-type-enabled gate lives in TimeTypeOps.createSerializer / createDeserializer; the
 * framework dispatch at the head of Serializer/DeserializerBuildHelper routes LocalTimeEncoder
 * through it. Disabling spark.sql.timeType.enabled must therefore still reject building a TIME
 * serializer/deserializer with UNSUPPORTED_TIME_TYPE, rather than silently producing one. The
 * enabled path is asserted too, so the gate is covered in both directions.
 */
class TimeTypeOpsSuite extends SparkFunSuite with SQLHelper {

  test("building a TIME serializer is rejected when the TIME type is disabled") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          SerializerBuildHelper.createSerializer(LocalTimeEncoder)
        },
        condition = "UNSUPPORTED_TIME_TYPE",
        parameters = Map.empty[String, String])
    }
  }

  test("building a TIME deserializer is rejected when the TIME type is disabled") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          DeserializerBuildHelper.createDeserializer(LocalTimeEncoder)
        },
        condition = "UNSUPPORTED_TIME_TYPE",
        parameters = Map.empty[String, String])
    }
  }

  test("building a TIME serializer succeeds when the TIME type is enabled") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      // The gate passes and the framework builds a TIME serializer.
      val serializer = SerializerBuildHelper.createSerializer(LocalTimeEncoder)
      assert(serializer.dataType.isInstanceOf[TimeType])
    }
  }

  test("building a TIME deserializer succeeds when the TIME type is enabled") {
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      // The gate passes and the framework builds a TIME deserializer returning java.time.LocalTime.
      val deserializer = DeserializerBuildHelper.createDeserializer(LocalTimeEncoder)
      assert(deserializer.dataType === ObjectType(classOf[java.time.LocalTime]))
    }
  }
}

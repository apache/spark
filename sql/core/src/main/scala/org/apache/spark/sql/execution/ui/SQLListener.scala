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

package org.apache.spark.sql.execution.ui

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.util.Converter

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{QueryExecution, SparkPlanInfo}

@DeveloperApi
case class SparkListenerSQLAdaptiveExecutionUpdate(
  executionId: Long,
  physicalPlanDescription: String,
  sparkPlanInfo: SparkPlanInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSQLAdaptiveSQLMetricUpdates(
    executionId: Long,
    sqlPlanMetrics: Seq[SQLPlanMetric])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSQLExecutionStart(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    sparkPlanInfo: SparkPlanInfo,
    time: Long,
    modifiedConfigs: Map[String, String] = Map.empty)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSQLExecutionEnd(executionId: Long, time: Long)
  extends SparkListenerEvent {

  // The name of the execution, e.g. `df.collect` will trigger a SQL execution with name "collect".
  @JsonIgnore private[sql] var executionName: Option[String] = None

  // The following 3 fields are only accessed when `executionName` is defined.

  // The duration of the SQL execution, in nanoseconds.
  @JsonIgnore private[sql] var duration: Long = 0L

  // The `QueryExecution` instance that represents the SQL execution
  @JsonIgnore private[sql] var qe: QueryExecution = null

  // The exception object that caused this execution to fail. None if the execution doesn't fail.
  @JsonIgnore private[sql] var executionFailure: Option[Throwable] = None
}

/**
 * A message used to update SQL metric value for driver-side updates (which doesn't get reflected
 * automatically).
 *
 * @param executionId The execution id for a query, so we can find the query plan.
 * @param accumUpdates Map from accumulator id to the metric value (metrics are always 64-bit ints).
 */
@DeveloperApi
case class SparkListenerDriverAccumUpdates(
    executionId: Long,
    @JsonDeserialize(contentConverter = classOf[LongLongTupleConverter])
    accumUpdates: Seq[(Long, Long)])
  extends SparkListenerEvent

/**
 * Jackson [[Converter]] for converting an (Int, Int) tuple into a (Long, Long) tuple.
 *
 * This is necessary due to limitations in how Jackson's scala module deserializes primitives;
 * see the "Deserializing Option[Int] and other primitive challenges" section in
 * https://github.com/FasterXML/jackson-module-scala/wiki/FAQ for a discussion of this issue and
 * SPARK-18462 for the specific problem that motivated this conversion.
 */
private class LongLongTupleConverter extends Converter[(Object, Object), (Long, Long)] {

  override def convert(in: (Object, Object)): (Long, Long) = {
    def toLong(a: Object): Long = a match {
      case i: java.lang.Integer => i.intValue()
      case l: java.lang.Long => l.longValue()
    }
    (toLong(in._1), toLong(in._2))
  }

  override def getInputType(typeFactory: TypeFactory): JavaType = {
    val objectType = typeFactory.constructType(classOf[Object])
    typeFactory.constructSimpleType(classOf[(_, _)], Array(objectType, objectType))
  }

  override def getOutputType(typeFactory: TypeFactory): JavaType = {
    val longType = typeFactory.constructType(classOf[Long])
    typeFactory.constructSimpleType(classOf[(_, _)], Array(longType, longType))
  }
}

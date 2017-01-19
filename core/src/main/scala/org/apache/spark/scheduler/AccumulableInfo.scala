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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * Information about an [[org.apache.spark.Accumulable]] modified during a task or stage.
 *
 * @param id accumulator ID
 * @param name accumulator name
 * @param update partial value from a task, may be None if used on driver to describe a stage
 * @param value total accumulated value so far, maybe None if used on executors to describe a task
 * @param internal whether this accumulator was internal
 * @param countFailedValues whether to count this accumulator's partial value if the task failed
 * @param metadata internal metadata associated with this accumulator, if any
 *
 * @note Once this is JSON serialized the types of `update` and `value` will be lost and be
 * cast to strings. This is because the user can define an accumulator of any type and it will
 * be difficult to preserve the type in consumers of the event log. This does not apply to
 * internal accumulators that represent task level metrics.
 */
@DeveloperApi
case class AccumulableInfo private[spark] (
    id: Long,
    name: Option[String],
    update: Option[Any], // represents a partial update within a task
    value: Option[Any],
    private[spark] val internal: Boolean,
    private[spark] val countFailedValues: Boolean,
    // TODO: use this to identify internal task metrics instead of encoding it in the name
    private[spark] val metadata: Option[String] = None)


/**
 * A collection of deprecated constructors. This will be removed soon.
 */
object AccumulableInfo {

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(
      id: Long,
      name: String,
      update: Option[String],
      value: String,
      internal: Boolean): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), update, Option(value), internal, countFailedValues = false)
  }

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(id: Long, name: String, update: Option[String], value: String): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), update, Option(value), internal = false, countFailedValues = false)
  }

  @deprecated("do not create AccumulableInfo", "2.0.0")
  def apply(id: Long, name: String, value: String): AccumulableInfo = {
    new AccumulableInfo(
      id, Option(name), None, Option(value), internal = false, countFailedValues = false)
  }
}

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
package org.apache.spark.sql.execution

import org.apache.spark.sql.connector.read.streaming.SparkDataStream

/**
 * This trait is a mixin for source physical nodes to represent the stream. This is required to the
 * physical nodes which is transformed from source logical nodes inheriting
 * [[org.apache.spark.sql.catalyst.plans.logical.StreamSourceAwareLogicalPlan]].
 *
 * The node implementing this trait should expose the number of output rows via "numOutputRows"
 * in `metrics`.
 */
trait StreamSourceAwareSparkPlan extends SparkPlan {
  /** Get the stream associated with this node. */
  def getStream: Option[SparkDataStream]
}

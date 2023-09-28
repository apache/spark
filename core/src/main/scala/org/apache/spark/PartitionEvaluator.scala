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

package org.apache.spark

import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * An evaluator for computing RDD partitions. Spark serializes and sends
 * [[PartitionEvaluatorFactory]] to executors, and then creates [[PartitionEvaluator]] via the
 * factory at the executor side.
 */
@DeveloperApi
@Since("3.5.0")
trait PartitionEvaluator[T, U] {

  /**
   * Evaluates the RDD partition at the given index. There can be more than one input iterator,
   * if the RDD was zipped from multiple RDDs.
   */
  def eval(partitionIndex: Int, inputs: Iterator[T]*): Iterator[U]
}

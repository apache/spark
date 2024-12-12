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
package org.apache.spark.sql.connect

import org.apache.spark.SparkUnsupportedOperationException

private[sql] object ConnectClientUnsupportedErrors {

  private def unsupportedFeatureException(
      subclass: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      "UNSUPPORTED_CONNECT_FEATURE." + subclass,
      Map.empty[String, String])
  }

  def rdd(): SparkUnsupportedOperationException =
    unsupportedFeatureException("RDD")

  def queryExecution(): SparkUnsupportedOperationException =
    unsupportedFeatureException("DATASET_QUERY_EXECUTION")

  def executeCommand(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_EXECUTE_COMMAND")

  def baseRelationToDataFrame(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_BASE_RELATION_TO_DATAFRAME")

  def experimental(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_EXPERIMENTAL_METHODS")

  def listenerManager(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_LISTENER_MANAGER")

  def sessionState(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_SESSION_STATE")

  def sharedState(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_SHARED_STATE")

  def sparkContext(): SparkUnsupportedOperationException =
    unsupportedFeatureException("SESSION_SPARK_CONTEXT")
}

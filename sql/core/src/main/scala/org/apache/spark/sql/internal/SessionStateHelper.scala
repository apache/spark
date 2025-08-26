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

package org.apache.spark.sql.internal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Helper trait to access session state related configurations and utilities.
 * It also provides type annotations for IDEs to build indexes.
 */
trait SessionStateHelper {
  private def sessionState(sparkSession: SparkSession): SessionState = {
    sparkSession.sessionState
  }

  private def sparkContext(sparkSession: SparkSession): SparkContext = {
    sparkSession.sparkContext
  }

  def getSparkConf(sparkSession: SparkSession): SparkConf = {
    sparkContext(sparkSession).conf
  }

  def getSqlConf(sparkSession: SparkSession): SQLConf = {
    sessionState(sparkSession).conf
  }

  def getHadoopConf(
      sparkSession: SparkSession,
      options: Map[String, String]): Configuration = {
    sessionState(sparkSession).newHadoopConfWithOptions(options)
  }
}

object SessionStateHelper extends SessionStateHelper

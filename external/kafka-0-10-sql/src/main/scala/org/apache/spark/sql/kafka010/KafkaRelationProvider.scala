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

package org.apache.spark.sql.kafka010

import java.util.UUID

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class KafkaRelationProvider extends RelationProvider with DataSourceRegister {

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   */
  override def shortName(): String = "kafka"

  /**
   * Returns a new base relation with the given parameters.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    // Each running query should use its own group id. Otherwise, the query may be only assigned
    // partial data since Kafka will assign partitions to multiple consumers having the same group
    // id. Hence, we should generate a unique id for each query.
    val uniqueGroupId = s"spark-kafka-relation-${UUID.randomUUID}"
    val kafkaConfigOptions = new KafkaConfigOptions(parameters, uniqueGroupId)
    val kafkaOffsetReader = new KafkaOffsetReader(kafkaConfigOptions.strategy,
      kafkaConfigOptions.kafkaParamsForDriver, parameters,
      driverGroupIdPrefix = s"$uniqueGroupId-driver")

    new KafkaRelation(
      sqlContext,
      kafkaOffsetReader,
      kafkaConfigOptions.kafkaParamsForExecutors,
      parameters,
      kafkaConfigOptions.failOnDataLoss,
      kafkaConfigOptions.startingRelationOffsets,
      kafkaConfigOptions.endingRelationOffsets)
  }
}

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
 *
 */

package org.apache.spark.cypher.conversions

import org.apache.spark.cypher.{SparkCypherNode, SparkCypherRelationship}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}

import scala.language.implicitConversions

object CypherValueEncoders {

  private implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = {
    v.asInstanceOf[ExpressionEncoder[T]]
  }

  implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = {
    kryo[CypherValue]
  }

  implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = {
    kryo[Map[String, CypherValue]]
  }

  implicit def cypherNodeEncoder: ExpressionEncoder[SparkCypherNode] = {
    kryo[SparkCypherNode]
  }

  implicit def cypherRelationshipEncoder: ExpressionEncoder[SparkCypherRelationship] = {
    kryo[SparkCypherRelationship]
  }

  implicit def cypherMapEncoder: ExpressionEncoder[CypherMap] = {
    kryo[CypherMap]
  }
}

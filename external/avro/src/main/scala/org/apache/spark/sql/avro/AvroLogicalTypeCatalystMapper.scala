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


package org.apache.spark.sql.avro

import org.apache.avro.{LogicalType, Schema}

import org.apache.spark.sql.avro.SchemaConverters.SchemaType
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.AbstractDataType

/**
 * Mapping interface between Catalyst struct type and Avro schemas
 */
trait AvroLogicalTypeCatalystMapper extends Serializable {

  /**
   * Given Avro logical type returns mapped Catalyst type
   * @return sql mappings
   */
  def toSqlType: PartialFunction[LogicalType, SchemaType]

  /**
   * Given Catalyst type returns mapped Avro schema type
   * @return catalyst mappings
   */
  def toAvroSchema: PartialFunction[(AbstractDataType, String, String), Schema]

  /**
   * Given Avro logical type and the Avro data value update the value to Catalyst data.
   * @return Avro deserialization mappings
   */
  def deserialize: PartialFunction[LogicalType, (CatalystDataUpdater, Int, Any) => Unit]

  /**
   * Given Avro logical type and the Catalyst data value update the value to Avro data.
   * @return Avro serialization mappings
   */
  def serialize: PartialFunction[LogicalType, (SpecializedGetters, Int) => Any]
}

class DefaultAvroLogicalTypeCatalystMapper extends AvroLogicalTypeCatalystMapper {
  override def toSqlType: PartialFunction[LogicalType, SchemaType] =
    Map.empty
  override def toAvroSchema: PartialFunction[(AbstractDataType, String, String), Schema] =
    Map.empty
  override def deserialize: PartialFunction[LogicalType, (CatalystDataUpdater, Int, Any) => Unit] =
    Map.empty
  override def serialize: PartialFunction[LogicalType, (SpecializedGetters, Int) => Any] =
    Map.empty
}

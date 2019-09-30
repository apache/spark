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
   * Given a [[org.apache.spark.sql.avro.RecordInfo]] returns mapped Avro schema type
   * @return partial function with mappings
   */
  def toAvroSchema: PartialFunction[RecordInfo, Schema]

  /**
   * Given Avro logical type and a [[org.apache.spark.sql.avro.DataDeserializer]] deserialize
   * de given Avro data to Catalyst data using the catalyst updater.
   * @return Avro deserialization mappings
   */
  def deserialize: PartialFunction[LogicalType, DataDeserializer => Unit]

  /**
   * Given Avro logical type and a [[org.apache.spark.sql.avro.DataSerializer]] serialize
   * de given Catalyst data to Avro data.
   * @return Avro serialization mappings
   */
  def serialize: PartialFunction[LogicalType, DataSerializer => Any]
}

class DefaultAvroLogicalTypeCatalystMapper extends AvroLogicalTypeCatalystMapper {
  override def toSqlType: PartialFunction[LogicalType, SchemaType] =
    Map.empty
  override def toAvroSchema: PartialFunction[RecordInfo, Schema] =
    Map.empty
  override def deserialize: PartialFunction[LogicalType, DataDeserializer => Unit] =
    Map.empty
  override def serialize: PartialFunction[LogicalType, DataSerializer => Any] =
    Map.empty
}

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

package org.apache.spark.sql.execution.datasources.parquet

/**
 * This is the wrapper class of repetition and definition level information used in Parquet
 * encoding for complext types.
 */
private[sql] case class RepetitionDefinitionInfo(repetition: Int, definition: Int)

/**
 * The following classes are defined to capture the schema structure for Parquet schema.
 * We don't care the actual types but only use these to have the structure and metadata such as
 * repetition and definition levels.
 */
private[sql] class ParquetField

private[sql] case class ParquetStruct(
    fields: Array[ParquetField],
    metadata: RepetitionDefinitionInfo) extends ParquetField

private[sql] case class ParquetArray(
    element: ParquetField,
    metadata: RepetitionDefinitionInfo) extends ParquetField

private[sql] case class ParquetMap(
    keyElement: ParquetField,
    valueElement: ParquetField,
    metadata: RepetitionDefinitionInfo) extends ParquetField

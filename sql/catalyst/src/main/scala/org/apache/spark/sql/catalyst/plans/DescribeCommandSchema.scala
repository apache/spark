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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

private[sql] object DescribeCommandSchema {
  def describeJsonTableAttributes(): Seq[AttributeReference] =
    Seq(
      AttributeReference("json_metadata", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "JSON metadata of the table").build())()
    )
  def describeTableAttributes(): Seq[AttributeReference] = {
      Seq(AttributeReference("col_name", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "name of the column").build())(),
        AttributeReference("data_type", StringType, nullable = false,
          new MetadataBuilder().putString("comment", "data type of the column").build())(),
        AttributeReference("comment", StringType, nullable = true,
          new MetadataBuilder().putString("comment", "comment of the column").build())())
    }

  def describeColumnAttributes(): Seq[AttributeReference] = Seq(
    AttributeReference("info_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column info").build())(),
    AttributeReference("info_value", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "value of the column info").build())())
}

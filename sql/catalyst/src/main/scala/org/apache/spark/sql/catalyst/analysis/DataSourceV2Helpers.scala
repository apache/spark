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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalog.v2.TableChange
import org.apache.spark.sql.catalyst.plans.logical.sql.QualifiedColType
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField}

object DataSourceV2Helpers {

  /** Creates the TableChanges required for adding columns through ALTER TABLE ADD COLUMNS */
  def addColumnChanges(cols: Seq[QualifiedColType]): Seq[TableChange] = {
    cols.map { col =>
      TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
    }
  }
}

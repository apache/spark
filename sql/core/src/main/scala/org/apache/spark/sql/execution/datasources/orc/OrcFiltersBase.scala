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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types.{AtomicType, BinaryType, DataType}

/**
 * Methods that can be shared when upgrading the built-in Hive.
 */
trait OrcFiltersBase {

  private[sql] def buildTree(filters: Seq[Filter]): Option[Filter] = {
    filters match {
      case Seq() => None
      case Seq(filter) => Some(filter)
      case Seq(filter1, filter2) => Some(And(filter1, filter2))
      case _ => // length > 2
        val (left, right) = filters.splitAt(filters.length / 2)
        Some(And(buildTree(left).get, buildTree(right).get))
    }
  }

  // Since ORC 1.5.0 (ORC-323), we need to quote for column names with `.` characters
  // in order to distinguish predicate pushdown for nested columns.
  protected[sql] def quoteAttributeNameIfNeeded(name: String) : String = {
    if (!name.contains("`") && name.contains(".")) {
      s"`$name`"
    } else {
      name
    }
  }

  /**
   * Return true if this is a searchable type in ORC.
   * Both CharType and VarcharType are cleaned at AstBuilder.
   */
  protected[sql] def isSearchableType(dataType: DataType) = dataType match {
    case BinaryType => false
    case _: AtomicType => true
    case _ => false
  }
}

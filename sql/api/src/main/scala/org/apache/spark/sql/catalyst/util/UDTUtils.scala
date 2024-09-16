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
package org.apache.spark.sql.catalyst.util

import scala.util.control.NonFatal

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.types.UserDefinedType
import org.apache.spark.util.SparkClassUtils

/**
 * Utilities for working with [[UserDefinedType]]s from within the `sql/api` project. UDTs are not
 * not fully supported in `sql/api` (and connect), they can currently only be used in conjunction
 * with catalyst because they (amongst others) require access to Spark SQLs internal data
 * representation.
 *
 * This interface and its companion object provide an escape hatch for working with UDTs from
 * within the api project (e.g. Row.toJSON). The companion will try to bind to an implementation
 * of the interface in catalyst, if none is found it will bind to [[DefaultUDTUtils]].
 */
private[sql] trait UDTUtils {

  /**
   * Convert the UDT instance to something that is compatible with [[org.apache.spark.sql.Row]].
   * The returned value must conform to the schema of the UDT.
   */
  def toRow(value: Any, udt: UserDefinedType[Any]): Any
}

private[sql] object UDTUtils extends UDTUtils {
  private val delegate =
    try {
      val cls = SparkClassUtils.classForName("org.apache.spark.sql.catalyst.util.UDTUtilsImpl")
      cls.getConstructor().newInstance().asInstanceOf[UDTUtils]
    } catch {
      case NonFatal(_) =>
        DefaultUDTUtils
    }

  override def toRow(value: Any, udt: UserDefinedType[Any]): Any = delegate.toRow(value, udt)
}

private[sql] object DefaultUDTUtils extends UDTUtils {
  override def toRow(value: Any, udt: UserDefinedType[Any]): Any = {
    throw SparkUnsupportedOperationException()
  }
}

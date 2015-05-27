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

import org.apache.spark.sql.types.{AtomicType, IntegralType, NumericType, DataType}

/**
 * Helper function to check valid data types
 */
object TypeUtils {

  def checkForNumericExpr(t: DataType, errorMsg: => String): Option[String] = {
    if (t.isInstanceOf[NumericType]) {
      None
    } else {
      Some(errorMsg)
    }
  }

  def checkForBitwiseExpr(t: DataType, errorMsg: => String): Option[String] = {
    if (t.isInstanceOf[IntegralType]) {
      None
    } else {
      Some(errorMsg)
    }
  }

  def checkForOrderingExpr(t: DataType, errorMsg: => String): Option[String] = {
    if (t.isInstanceOf[AtomicType]) {
      None
    } else {
      Some(errorMsg)
    }
  }

  def getNumeric(t: DataType): Numeric[Any] =
    t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  def getOrdering(t: DataType): Ordering[Any] =
    t.asInstanceOf[AtomicType].ordering.asInstanceOf[Ordering[Any]]
}

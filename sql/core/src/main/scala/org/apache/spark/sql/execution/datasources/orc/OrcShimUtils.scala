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

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.hadoop.hive.ql.io.sarg.{SearchArgument => OrcSearchArgument}
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.{Operator => OrcOperator}
import org.apache.hadoop.hive.serde2.io.{DateWritable, HiveDecimalWritable}

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.execution.datasources.DaysWritable
import org.apache.spark.sql.types.Decimal

/**
 * Various utilities for ORC used to upgrade the built-in Hive.
 */
private[sql] object OrcShimUtils {

  class VectorizedRowBatchWrap(val batch: VectorizedRowBatch) {}

  private[sql] type Operator = OrcOperator
  private[sql] type SearchArgument = OrcSearchArgument

  def getGregorianDays(value: Any): Int = {
    new DaysWritable(value.asInstanceOf[DateWritable]).gregorianDays
  }

  def getDecimal(value: Any): Decimal = {
    val decimal = value.asInstanceOf[HiveDecimalWritable].getHiveDecimal()
    Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
  }

  def getDateWritable(reuseObj: Boolean): (SpecializedGetters, Int) => DateWritable = {
    if (reuseObj) {
      val result = new DaysWritable()
      (getter, ordinal) =>
        result.set(getter.getInt(ordinal))
        result
    } else {
      (getter: SpecializedGetters, ordinal: Int) =>
        new DaysWritable(getter.getInt(ordinal))
    }
  }

  def getHiveDecimalWritable(precision: Int, scale: Int):
      (SpecializedGetters, Int) => HiveDecimalWritable = {
    (getter, ordinal) =>
      val d = getter.getDecimal(ordinal, precision, scale)
      new HiveDecimalWritable(HiveDecimal.create(d.toJavaBigDecimal))
  }
}

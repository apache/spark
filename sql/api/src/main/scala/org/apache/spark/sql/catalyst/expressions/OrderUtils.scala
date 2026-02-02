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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, NullType, StructType, UserDefinedType, VariantType}

object OrderUtils {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case VariantType => false
    case dt: AtomicType => true
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
    case array: ArrayType => isOrderable(array.elementType)
    case udt: UserDefinedType[_] => isOrderable(udt.sqlType)
    case _ => false
  }
}

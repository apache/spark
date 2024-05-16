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

package org.apache.spark.sql.internal.types

import org.apache.spark.sql.types.{AbstractDataType, DataType, MapType}


/**
 * Use AbstractMapType(AbstractDataType, AbstractDataType)
 * for defining expected types for expression parameters.
 */
case class AbstractMapType(
    keyType: AbstractDataType,
    valueType: AbstractDataType
  ) extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType =
    MapType(keyType.defaultConcreteType, valueType.defaultConcreteType, valueContainsNull = true)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[MapType] &&
      keyType.acceptsType(other.asInstanceOf[MapType].keyType) &&
      valueType.acceptsType(other.asInstanceOf[MapType].valueType)
  }

  override private[spark] def simpleString: String =
    s"map<${keyType.simpleString}, ${valueType.simpleString}>"
}

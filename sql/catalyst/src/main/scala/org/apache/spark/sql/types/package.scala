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

package org.apache.spark.sql

/**
 * Contains a type system for attributes produced by relations, including complex types like
 * structs, arrays and maps.
 */
package object types {

  /* Aliases for backwards compatibility. See SPARK-11780. */
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.ArrayBasedMapData", since = "1.6.0")
  type ArrayBasedMapData = org.apache.spark.sql.catalyst.util.ArrayBasedMapData
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.ArrayBasedMapData", since = "1.6.0")
  val ArrayBasedMapData = org.apache.spark.sql.catalyst.util.ArrayBasedMapData
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.ArrayData", since = "1.6.0")
  type ArrayData = org.apache.spark.sql.catalyst.util.ArrayData
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.DataTypeException", since = "1.6.0")
  type DataTypeException = org.apache.spark.sql.catalyst.util.DataTypeException
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.DataTypeParser", since = "1.6.0")
  type DataTypeParser = org.apache.spark.sql.catalyst.util.DataTypeParser
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.DataTypeParser", since = "1.6.0")
  val DataTypeParser = org.apache.spark.sql.catalyst.util.DataTypeParser
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.GenericArrayData", since = "1.6.0")
  type GenericArrayData = org.apache.spark.sql.catalyst.util.GenericArrayData
  @deprecated("Moved to org.apache.spark.sql.catalyst.util.MapData", since = "1.6.0")
  type MapData = org.apache.spark.sql.catalyst.util.MapData

}

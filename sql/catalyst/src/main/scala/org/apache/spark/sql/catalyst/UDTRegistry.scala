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

package org.apache.spark.sql.catalyst

import scala.collection.mutable
import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.annotation.SQLUserDefinedType
import org.apache.spark.sql.catalyst.types.UserDefinedType

/**
 * Global registry for user-defined types (UDTs).
 */
private[sql] object UDTRegistry {
  /** Map: UserType --> UserDefinedType */
  val udtRegistry = new mutable.HashMap[Any, UserDefinedType[_]]()

  /**
   * Register a user-defined type and its serializer, to allow automatic conversion between
   * RDDs of user types and SchemaRDDs.
   * If this type has already been registered, this does nothing.
   */
  def registerType[UserType](implicit userType: Type): Unit = {
    // TODO: Check to see if type is built-in.  Throw exception?
    if (!UDTRegistry.udtRegistry.contains(userType)) {
      val udt =
        getClass.getClassLoader.loadClass(userType.typeSymbol.asClass.fullName)
          .getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance()
      UDTRegistry.udtRegistry(userType) = udt
    }
    // TODO: Else: Should we check (assert) that udt is the same as what is in the registry?
  }
}

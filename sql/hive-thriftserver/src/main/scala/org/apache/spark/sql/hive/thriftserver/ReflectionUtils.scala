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

package org.apache.spark.sql.hive.thriftserver

private[hive] object ReflectionUtils {
  def setSuperField(obj : Object, fieldName: String, fieldValue: Object): Unit = {
    setAncestorField(obj, 1, fieldName, fieldValue)
  }

  def setAncestorField(obj: AnyRef, level: Int, fieldName: String, fieldValue: AnyRef): Unit = {
    val ancestor = Iterator.iterate[Class[_]](obj.getClass)(_.getSuperclass).drop(level).next()
    val field = ancestor.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.set(obj, fieldValue)
  }

  def getSuperField[T](obj: AnyRef, fieldName: String): T = {
    getAncestorField[T](obj, 1, fieldName)
  }

  def getAncestorField[T](clazz: Object, level: Int, fieldName: String): T = {
    val ancestor = Iterator.iterate[Class[_]](clazz.getClass)(_.getSuperclass).drop(level).next()
    val field = ancestor.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(clazz).asInstanceOf[T]
  }

  def invokeStatic(clazz: Class[_], methodName: String, args: (Class[_], AnyRef)*): AnyRef = {
    invoke(clazz, null, methodName, args: _*)
  }

  def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {

    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values: _*)
  }
}

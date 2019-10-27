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

package org.apache.spark.sql.thriftserver.cli.session

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, UndeclaredThrowableException}
import java.security.{PrivilegedActionException, PrivilegedExceptionAction}

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.Logging
import org.apache.spark.sql.thriftserver.cli.SparkThriftServerSQLException

class ThriftServerSessionProxy extends InvocationHandler with Logging {
  private var _base: ThriftServerSession = null
  private var _ugi: UserGroupInformation = _

  def this(session: ThriftServerSession, ugi: UserGroupInformation) = {
    this()
    this._base = session
    this._ugi = ugi
  }

  def invoke(method: Method, args: Array[AnyRef]): AnyRef = {
    try {
      return method.invoke(_base, args: _*)
    }
    catch {
      case e: InvocationTargetException =>
        if (e.getCause.isInstanceOf[SparkThriftServerSQLException]) {
          throw e.getCause.asInstanceOf[SparkThriftServerSQLException]
        }
        throw new RuntimeException(e.getCause)
      case e: IllegalArgumentException =>
        throw new RuntimeException(e)
      case e: IllegalAccessException =>
        throw new RuntimeException(e)
    }
  }

  override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    try {
      if (method.getDeclaringClass eq classOf[ThriftServerSessionBase]) {
        invoke(method, args)
      }
      _ugi.doAs(new PrivilegedExceptionAction[AnyRef]() {
        @throws[SparkThriftServerSQLException]
        override def run: AnyRef = invoke(method, args)
      })

    } catch {
      case e: UndeclaredThrowableException =>
        val innerException: Throwable = e.getCause
        if (innerException.isInstanceOf[PrivilegedActionException]) {
          throw innerException.getCause
        }
        else {
          throw e.getCause
        }
    }
  }
}

object ThriftServerSessionProxy {
  def getProxy(session: ThriftServerSession, ugi: UserGroupInformation): ThriftServerSession = {
    java.lang.reflect.Proxy.newProxyInstance(classOf[ThriftServerSession].getClassLoader,
      Array[Class[_]](classOf[ThriftServerSession]),
      new ThriftServerSessionProxy(session, ugi)).asInstanceOf[ThriftServerSession]
  }
}

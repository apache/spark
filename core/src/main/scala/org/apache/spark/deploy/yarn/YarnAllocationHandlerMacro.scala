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

package org.apache.spark.deploy.yarn

import scala.language.experimental.macros
import scala.reflect.macros.Context

private[yarn] object YarnAllocationHandlerMacro {
  def getAMResp(resp: Any): Any = macro getAMRespImpl

  /**
   * From Hadoop CDH 4.4.0+ (2.1.0-beta),
   * AMResponse is merged into AllocateResponse,
   * so we don't need to call getAMResponse(), just use AllocateResponse directly.
   * This macro will test the existence of AMResponse,
   * and generate diffenert expressions.
   *
   * This macro now is only used in spark's alpha version of yarn api.
   * It stays in the core project, for the two-stage compiling of
   * the scala macro system.
   */
  def getAMRespImpl(c: Context)(resp: c.Expr[Any]) = {
    try {
      import c.universe._
      c.mirror.staticClass("org.apache.hadoop.yarn.api.records.AMResponse")
      c.Expr[Any](Apply(Select(resp.tree, newTermName("getAMResponse")), List()))
    } catch {
      case _: Throwable => resp
    }
  }
}

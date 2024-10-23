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
package org.apache.spark.sql.catalyst.expressions.xml

import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class XPathEvaluator(path: UTF8String) {

  @transient private lazy val xpathUtil: UDFXPathUtil = new UDFXPathUtil

  final def evaluate(xml: UTF8String, dataType: DataType): Any = {
    if (xml == null || xml.toString.isEmpty || path == null || path.toString.isEmpty) return null
    dataType match {
      case BooleanType => xpathUtil.evalBoolean(xml.toString, path.toString)
      case ShortType =>
        val ret = xpathUtil.evalNumber(xml.toString, path.toString)
        if (ret eq null) null.asInstanceOf[Short] else ret.shortValue()
      case IntegerType =>
        val ret = xpathUtil.evalNumber(xml.toString, path.toString)
        if (ret eq null) null.asInstanceOf[Int] else ret.intValue()
      case LongType =>
        val ret = xpathUtil.evalNumber(xml.toString, path.toString)
        if (ret eq null) null.asInstanceOf[Long] else ret.longValue()
      case FloatType =>
        val ret = xpathUtil.evalNumber(xml.toString, path.toString)
        if (ret eq null) null.asInstanceOf[Float] else ret.floatValue()
      case DoubleType =>
        val ret = xpathUtil.evalNumber(xml.toString, path.toString)
        if (ret eq null) null.asInstanceOf[Double] else ret.doubleValue()
      case dt if dt.isInstanceOf[StringType] =>
        val ret = xpathUtil.evalString(xml.toString, path.toString)
        UTF8String.fromString(ret)
      case ArrayType(elementType, _) if elementType.isInstanceOf[StringType] =>
        val nodeList = xpathUtil.evalNodeList(xml.toString, path.toString)
        if (nodeList ne null) {
          val ret = new Array[AnyRef](nodeList.getLength)
          var i = 0
          while (i < nodeList.getLength) {
            ret(i) = UTF8String.fromString(nodeList.item(i).getNodeValue)
            i += 1
          }
          new GenericArrayData(ret)
        } else {
          null
        }
    }
  }
}

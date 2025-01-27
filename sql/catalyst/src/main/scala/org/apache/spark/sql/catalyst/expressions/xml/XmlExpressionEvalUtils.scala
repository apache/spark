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
import org.apache.spark.sql.catalyst.xml.XmlInferSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object XmlExpressionEvalUtils {

  def schemaOfXml(xmlInferSchema: XmlInferSchema, xml: UTF8String): UTF8String = {
    val dataType = xmlInferSchema.infer(xml.toString).get match {
      case st: StructType =>
        xmlInferSchema.canonicalizeType(st).getOrElse(StructType(Nil))
      case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
        xmlInferSchema
          .canonicalizeType(at.elementType)
          .map(ArrayType(_, containsNull = at.containsNull))
          .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
      case other: DataType =>
        xmlInferSchema.canonicalizeType(other).getOrElse(SQLConf.get.defaultStringType)
    }

    UTF8String.fromString(dataType.sql)
  }
}

trait XPathEvaluator {

  protected val path: UTF8String

  @transient protected lazy val xpathUtil: UDFXPathUtil = new UDFXPathUtil

  final def evaluate(xml: UTF8String): Any = {
    if (xml == null || xml.toString.isEmpty || path == null || path.toString.isEmpty) return null
    doEvaluate(xml)
  }

  def doEvaluate(xml: UTF8String): Any
}

case class XPathBooleanEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    xpathUtil.evalBoolean(xml.toString, path.toString)
  }
}

case class XPathShortEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Short] else ret.shortValue()
  }
}

case class XPathIntEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Int] else ret.intValue()
  }
}

case class XPathLongEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Long] else ret.longValue()
  }
}

case class XPathFloatEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Float] else ret.floatValue()
  }
}

case class XPathDoubleEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Double] else ret.doubleValue()
  }
}

case class XPathStringEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalString(xml.toString, path.toString)
    UTF8String.fromString(ret)
  }
}

case class XPathListEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
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

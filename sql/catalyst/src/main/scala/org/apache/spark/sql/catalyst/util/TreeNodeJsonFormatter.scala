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

package org.apache.spark.sql.catalyst.util

import java.util.UUID
import scala.collection.mutable.Stack
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.util.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions._

object TreeNodeJsonFormatter {

  def toJSON(node: TreeNode[_]): String = {
    pretty(render(jsonValue(node)))
  }

  private def jsonValue(node: TreeNode[_]): JValue = {
    val jsonValues = scala.collection.mutable.ArrayBuffer.empty[JValue]

    def collectJsonValue(tn: TreeNode[_]): Unit = {
      val jsonFields = ("class" -> JString(tn.getClass.getName)) ::
        ("num-children" -> JInt(tn.children.length)) :: getJsonFields(tn)
      jsonValues += JObject(jsonFields)
      tn.children.foreach(c => collectJsonValue(c.asInstanceOf[TreeNode[_]]))
    }

    collectJsonValue(node)
    jsonValues
  }

  private def getJsonFields(node: TreeNode[_]): List[JField] = {
    val fieldNames = getConstructorParas(node.getClass).map(_._1)
    val fieldValues = node.productIterator.toSeq ++ node.otherCopyArgs
    assert(fieldNames.length == fieldValues.length, s"${getClass.getSimpleName} fields: " +
      fieldNames.mkString(", ") + s", values: " + fieldValues.map(_.toString).mkString(", "))

    fieldNames.zip(fieldValues).map {
      case (name, value: TreeNode[_]) if node.containsChild(value) => name -> JNothing
      case (name, value: Seq[_]) if value.nonEmpty && value.forall {
        case n: TreeNode[_] => node.containsChild(n)
        case _ => false
      } => name -> JNothing
      case (name, value) => name -> parseToJson(value)
    }.toList
  }

  private def parseToJson(obj: Any): JValue = obj match {
    case b: Boolean => JBool(b)
    case b: Byte => JInt(b.toInt)
    case s: Short => JInt(s.toInt)
    case i: Int => JInt(i)
    case l: Long => JInt(l)
    case f: Float => JDouble(f)
    case d: Double => JDouble(d)
    case s: String => JString(s)
    case dt: DataType => dt.jsonValue
    case m: Metadata => m.jsonValue
    case e: ExprId => ("id" -> e.id) ~ ("jvmId" -> e.jvmId.toString)
    case s: SortDirection => s.toString
    case n: TreeNode[_] => jsonValue(n)
    case o: Option[_] => o.map(parseToJson).getOrElse(JNull)
    case t: Seq[_] => JArray(t.map(parseToJson).toList)
    case _ => throw new RuntimeException(s"Do not support type ${obj.getClass}.")
  }

  def fromJSON(json: String): TreeNode[_] = {
    val jsonAST = parse(json)
    assert(jsonAST.isInstanceOf[JArray])
    reconstruct(jsonAST.asInstanceOf[JArray])
  }

  import universe._

  private def reconstruct(treeNodeJson: JArray): TreeNode[_] = {
    assert(treeNodeJson.arr.forall(_.isInstanceOf[JObject]))
    val jsonNodes = Stack(treeNodeJson.arr.map(_.asInstanceOf[JObject]): _*)

    def parseNextNode(): TreeNode[_] = {
      val nextNode = jsonNodes.pop()

      val cls = Utils.classForName((nextNode \ "class").asInstanceOf[JString].s)
      val numChildren = (nextNode \ "num-children").asInstanceOf[JInt].num.toInt

      var children: Seq[TreeNode[_]] = (1 to numChildren).map(_ => parseNextNode())
      val fields = getConstructorParas(cls)

      val parameters: Array[AnyRef] = fields.map {
        case (fieldName, fieldType) =>
          val fieldJsonValue = nextNode \ fieldName
          if (fieldJsonValue == JNothing) {
            fieldType match {
              case t if t <:< localTypeOf[Seq[_]] =>
                val result = children
                children = Seq.empty
                result
              case _ =>
                val result = children.head
                children = children.drop(1)
                result
            }
          } else {
            parseFromJson(fieldJsonValue, fieldType)
          }
      }.toArray

      if (cls == classOf[org.apache.spark.sql.catalyst.expressions.Literal]) {
        val value: AnyRef = parameters(1).asInstanceOf[DataType] match {
          case ByteType => parameters(0).asInstanceOf[BigInt].toByte: java.lang.Byte
          case ShortType => parameters(0).asInstanceOf[BigInt].toShort: java.lang.Short
          case IntegerType => parameters(0).asInstanceOf[BigInt].toInt: java.lang.Integer
          case LongType => parameters(0).asInstanceOf[BigInt].toLong: java.lang.Long
          case FloatType => parameters(0).asInstanceOf[Double].toFloat: java.lang.Float
          case _ => parameters(0)
        }
        parameters(0) = value
      }

      val ctors = cls.getConstructors.filter(_.getParameterCount != 0)
      if (ctors.isEmpty) {
        sys.error(s"No valid constructor for ${cls.getName}")
      }
      val defaultCtor = ctors.maxBy(_.getParameterCount)
      defaultCtor.newInstance(parameters: _*).asInstanceOf[TreeNode[_]]
    }

    parseNextNode()
  }

  private def parseFromJson(
      value: JValue,
      expectedType: Type): AnyRef = ScalaReflectionLock.synchronized {
    expectedType match {
      case t if t <:< definitions.BooleanTpe =>
        value.asInstanceOf[JBool].value: java.lang.Boolean
      case t if t <:< definitions.ByteTpe =>
        value.asInstanceOf[JInt].num.toByte: java.lang.Byte
      case t if t <:< definitions.ShortTpe =>
        value.asInstanceOf[JInt].num.toShort: java.lang.Short
      case t if t <:< definitions.IntTpe =>
        value.asInstanceOf[JInt].num.toInt: java.lang.Integer
      case t if t <:< definitions.LongTpe =>
        value.asInstanceOf[JInt].num.toLong: java.lang.Long
      case t if t <:< definitions.FloatTpe =>
        value.asInstanceOf[JDouble].num.toFloat: java.lang.Float
      case t if t <:< definitions.DoubleTpe =>
        value.asInstanceOf[JDouble].num: java.lang.Double

      case t if t <:< localTypeOf[java.lang.String] => value.asInstanceOf[JString].s
      case t if t <:< localTypeOf[DataType] => DataType.parseDataType(value)
      case t if t <:< localTypeOf[Metadata] => Metadata.fromJObject(value.asInstanceOf[JObject])
      case t if t <:< localTypeOf[ExprId] =>
        val JInt(id) = value \ "id"
        val JString(jvmId) = value \ "jvmId"
        ExprId(id.toInt, UUID.fromString(jvmId))
      case t if t <:< localTypeOf[SortDirection] =>
        val JString(direction) = value
        if (direction == Ascending.toString) {
          Ascending
        } else if (direction == Descending.toString) {
          Descending
        } else {
          throw new RuntimeException(s"$direction is not a valid SortDirection string.")
        }
      case t if t <:< localTypeOf[TreeNode[_]] => reconstruct(value.asInstanceOf[JArray])
      case t if t <:< localTypeOf[Option[_]] =>
        if (value == JNull) {
          None
        } else {
          val TypeRef(_, _, Seq(optType)) = t
          Option(parseFromJson(value, optType))
        }
      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val JArray(elements) = value
        elements.map(parseFromJson(_, elementType)).toSeq
      case _ => value match {
        case JBool(b) => b: java.lang.Boolean
        case JInt(i) => i
        case JDouble(d) => d: java.lang.Double
        case JString(s) => s
        case _ => throw new RuntimeException(s"Do not support type $expectedType.")
      }
    }
  }
}

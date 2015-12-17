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
import scala.collection.Map
import scala.collection.mutable.Stack
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.util.Utils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.catalyst.{TableIdentifier, ScalaReflectionLock}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Statistics}

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

  private def getJsonFields(node: TreeNode[_]): List[JField] = node match {
    case lit: Literal =>
      val value = (lit.value, lit.dataType) match {
        case (null, _) => JNull
        case (i: Int, DateType) => JString(DateTimeUtils.toJavaDate(i).toString)
        case (l: Long, TimestampType) => JString(DateTimeUtils.toJavaTimestamp(l).toString)
        case (other, _) => JString(other.toString)
      }
      ("value" -> value) :: ("dataType" -> parseToJson(lit.dataType)) :: Nil
    case _ =>
      val fieldNames = getConstructorParameters(node.getClass).map(_._1)
      val fieldValues = node.productIterator.toSeq ++ node.otherCopyArgs
      assert(fieldNames.length == fieldValues.length, s"${getClass.getSimpleName} fields: " +
        fieldNames.mkString(", ") + s", values: " + fieldValues.map(_.toString).mkString(", "))

      fieldNames.zip(fieldValues).map {
        case (name, value: TreeNode[_]) if node.containsChild(value) =>
          name -> JInt(node.children.indexOf(value))
        case (name, value: Seq[_]) if value.nonEmpty && value.forall {
          case n: TreeNode[_] => node.containsChild(n)
          case _ => false
        } => name -> JArray(
          value.map(v => JInt(node.children.indexOf(v.asInstanceOf[TreeNode[_]]))).toList
        )
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
    case null => JNull
    case s: String => JString(s)
    case u: UUID => JString(u.toString)
    case dt: DataType => dt.jsonValue
    case m: Metadata => m.jsonValue
    case s: SortDirection => s.toString
    case j: JoinType => j.toString
    case a: AggregateMode => a.toString
    case s: Statistics => JInt(s.sizeInBytes)
    case s: StorageLevel =>
      ("useDisk" -> s.useDisk) ~ ("useMemory" -> s.useMemory) ~ ("useOffHeap" -> s.useOffHeap) ~
        ("deserialized" -> s.deserialized) ~ ("replication" -> s.replication)
    case n: TreeNode[_] => jsonValue(n)
    case o: Option[_] => o.map(parseToJson)
    case t: Seq[_] => JArray(t.map(parseToJson).toList)
    case m: Map[_, _] =>
      val fields = m.toList.map { case (k: String, v) => (k, parseToJson(v)) }
      JObject(fields)
    case r: RDD[_] => JNothing
    case p: Product if isSupported(p.getClass) =>
      val fieldNames = getConstructorParameters(p.getClass).map(_._1)
      val fieldValues = p.productIterator.toSeq
      assert(fieldNames.length == fieldValues.length)
      fieldNames.zip(fieldValues).map {
        case (name, value) => name -> parseToJson(value)
      }.toList
    case _ => JNull
  }

  private def isSupported(cls: Class[_]): Boolean = {
    cls == classOf[ExprId] || cls == classOf[TableIdentifier] ||
      cls == classOf[StructField] || cls.getName.startsWith("scala.Tuple")
  }

  def fromJSON(json: String, sc: SparkContext): TreeNode[_] = {
    val jsonAST = parse(json)
    assert(jsonAST.isInstanceOf[JArray])
    reconstruct(jsonAST.asInstanceOf[JArray], sc)
  }

  private def reconstruct(treeNodeJson: JArray, sc: SparkContext): TreeNode[_] = {
    assert(treeNodeJson.arr.forall(_.isInstanceOf[JObject]))
    val jsonNodes = Stack(treeNodeJson.arr.map(_.asInstanceOf[JObject]): _*)

    def parseNextNode(): TreeNode[_] = {
      val nextNode = jsonNodes.pop()

      val cls = Utils.classForName((nextNode \ "class").asInstanceOf[JString].s)
      if (cls == classOf[Literal]) {
        parseToLiteral(nextNode)
      } else if (cls == OneRowRelation.getClass) {
        OneRowRelation
      } else {
        val numChildren = (nextNode \ "num-children").asInstanceOf[JInt].num.toInt

        val children: Seq[TreeNode[_]] = (1 to numChildren).map(_ => parseNextNode())
        val fields = getConstructorParameters(cls)

        val parameters: Array[AnyRef] = fields.map {
          case (fieldName, fieldType) => parseFromJson(nextNode \ fieldName, fieldType, children, sc)
        }.toArray

        val ctors = cls.getConstructors
        if (ctors.isEmpty) {
          sys.error(s"No valid constructor for ${cls.getName}")
        } else {
          val defaultCtor = ctors.maxBy(_.getParameterTypes.size)
          defaultCtor.newInstance(parameters: _*).asInstanceOf[TreeNode[_]]
        }
      }
    }

    parseNextNode()
  }

  private def parseToLiteral(json: JObject): Literal = {
    val dataType = DataType.parseDataType(json \ "dataType")
    json \ "value" match {
      case JNull => Literal.create(null, dataType)
      case JString(str) =>
        val value = dataType match {
          case BooleanType => str.toBoolean
          case ByteType => str.toByte
          case ShortType => str.toShort
          case IntegerType => str.toInt
          case LongType => str.toLong
          case FloatType => str.toFloat
          case DoubleType => str.toDouble
          case StringType => UTF8String.fromString(str)
          case DateType => java.sql.Date.valueOf(str)
          case TimestampType => java.sql.Timestamp.valueOf(str)
          case CalendarIntervalType => CalendarInterval.fromString(str)
          case t: DecimalType =>
            val d = Decimal(str)
            assert(d.changePrecision(t.precision, t.scale))
            d
          case _ => null
        }
        Literal.create(value, dataType)
      case other => sys.error(s"$other is not a valid Literal json value")
    }
  }

  import universe._

  private def parseFromJson(
      value: JValue,
      expectedType: Type,
      children: Seq[TreeNode[_]],
      sc: SparkContext): AnyRef = ScalaReflectionLock.synchronized {
    if (value == JNull) return null

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
      case t if t <:< localTypeOf[UUID] => UUID.fromString(value.asInstanceOf[JString].s)
      case t if t <:< localTypeOf[DataType] => DataType.parseDataType(value)
      case t if t <:< localTypeOf[Metadata] => Metadata.fromJObject(value.asInstanceOf[JObject])
      case t if t <:< localTypeOf[SortDirection] => value.asInstanceOf[JString].s match {
          case "Ascending" => Ascending
          case "Descending" => Descending
          case other => throw new RuntimeException(s"$other is not a valid SortDirection string.")
        }
      case t if t <:< localTypeOf[JoinType] => value.asInstanceOf[JString].s match {
          case "Inner" => Inner
          case "LeftOuter" => LeftOuter
          case "RightOuter" => RightOuter
          case "FullOuter" => FullOuter
          case "LeftSemi" => LeftSemi
          case other => throw new RuntimeException(s"$other is not a valid JoinType string.")
      }
      case t if t <:< localTypeOf[AggregateMode] => value.asInstanceOf[JString].s match {
        case "Partial" => Partial
        case "PartialMerge" => PartialMerge
        case "Final" => Final
        case "Complete" => Complete
        case other => throw new RuntimeException(s"$other is not a valid AggregateMode string.")
      }
      case t if t <:< localTypeOf[Statistics] =>
        val JInt(sizeInBytes) = value
        Statistics(sizeInBytes)
      case t if t <:< localTypeOf[StorageLevel] =>
        val JBool(useDisk) = value \ "useDisk"
        val JBool(useMemory) = value \ "useMemory"
        val JBool(useOffHeap) = value \ "useOffHeap"
        val JBool(deserialized) = value \ "deserialized"
        val JInt(replication) = value \ "replication"
        StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication.toInt)
      case t if t <:< localTypeOf[TreeNode[_]] => value match {
        case JInt(i) => children(i.toInt)
        case arr: JArray => reconstruct(arr, sc)
        case _ => throw new RuntimeException(s"$value is not a valid json value for tree node.")
      }
      case t if t <:< localTypeOf[Option[_]] =>
        if (value == JNothing) {
          None
        } else {
          val TypeRef(_, _, Seq(optType)) = t
          Option(parseFromJson(value, optType, children, sc))
        }
      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val JArray(elements) = value
        elements.map(parseFromJson(_, elementType, children, sc)).toSeq
      case t if t <:< localTypeOf[Map[_, _]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val JObject(fields) = value
        fields.map {
          case (name, value) => name -> parseFromJson(value, valueType, children, sc)
        }.toMap
      case t if t <:< localTypeOf[RDD[_]] =>
        new EmptyRDD[Any](sc)
      case t if t <:< localTypeOf[Product] =>
        val fields = getConstructorParameters(t)
        val parameters: Array[AnyRef] = fields.map {
          case (fieldName, fieldType) => parseFromJson(value \ fieldName, fieldType, children, sc)
        }.toArray
        val ctor = Utils.classForName(getClassNameFromType(t))
          .getConstructors.maxBy(_.getParameterTypes.size)
        ctor.newInstance(parameters: _*).asInstanceOf[AnyRef]
      case _ => sys.error(s"Do not support type $expectedType with json $value.")
    }
  }
}

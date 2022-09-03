
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

package org.apache.spark.sql.proto

// import com.google.protobuf.{ByteString, DynamicMessage, Message}
import com.google.protobuf.{ByteString, DynamicMessage, Message}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, NoopFilters, StructFilters}
// import org.apache.spark.sql.catalyst.OrderedFilters
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.proto.CatalystTypes.{BytesMsg, Person}
// import org.apache.spark.sql.sources.{EqualTo, Not}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ProtoCatalystDataConversionSuite extends SparkFunSuite
  with SharedSparkSession
  with ExpressionEvalHelper {

  private def checkResult(data: Literal, descFilePath: String,
                          messageName: String, expected: Any): Unit = {

    /*
    val ctoP = CatalystDataToProto(data, descFilePath,
      messageName)
    // scalastyle:off println
    println("==========cTop========", ctoP)

    val pToC = ProtoDataToCatalyst(ctoP, descFilePath, messageName, Map.empty)

    // scalastyle:off println
    println("==========pToC========", pToC)

    val y = prepareExpectedResult(expected)

    // scalastyle:off println
    println("=====Expected=======", expected, y)

    checkEvaluation(pToC, y) */

    checkEvaluation(
      ProtoDataToCatalyst(CatalystDataToProto(data, descFilePath,
        messageName), descFilePath, messageName, Map.empty),
      prepareExpectedResult(expected))
  }

  protected def prepareExpectedResult(expected: Any): Any = expected match {
    // Spark byte and short both map to avro int
    case b: Byte => b.toInt
    case s: Short => s.toInt
    case row: GenericInternalRow => InternalRow.fromSeq(row.values.map(prepareExpectedResult))
    case array: GenericArrayData => new GenericArrayData(array.array.map(prepareExpectedResult))
    case map: MapData =>
      val keys = new GenericArrayData(
        map.keyArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      val values = new GenericArrayData(
        map.valueArray().asInstanceOf[GenericArrayData].array.map(prepareExpectedResult))
      new ArrayBasedMapData(keys, values)
    case other => other
  }

  private val testingTypes = Seq(
    StructType(StructField("bool_type", BooleanType, false) :: Nil),
    StructType(StructField("int32_type", IntegerType, false) :: Nil),
    StructType(StructField("double_type", DoubleType, false) :: Nil),
    StructType(StructField("float_type", FloatType, false) :: Nil),
    StructType(StructField("bytes_type", BinaryType, false) :: Nil),
    StructType(StructField("string_type", StringType, false) :: Nil),
    StructType(StructField("int32_type", ByteType, false) :: Nil),
    StructType(StructField("int32_type", ShortType, false) :: Nil),
  )

  private val catalystTypesToProtoMessages: Map[DataType, String] = Map(
    BooleanType -> "BooleanMsg",
    IntegerType -> "IntegerMsg",
    DoubleType -> "DoubleMsg",
    FloatType -> "FloatMsg",
    BinaryType -> "BytesMsg",
    StringType -> "StringMsg",
    ByteType -> "IntegerMsg",
    ShortType -> "IntegerMsg"
  )

  testingTypes.foreach { dt =>
    val seed = scala.util.Random.nextLong()
    val filePath = testFile("protobuf/catalyst_types.desc").replace("file:/", "/")
    test(s"single $dt with seed $seed") {
      val rand = new scala.util.Random(seed)
      val data = RandomDataGenerator.forType(dt, rand = rand).get.apply()
      val converter = CatalystTypeConverters.createToCatalystConverter(dt)
      val input = Literal.create(converter(data), dt)
      checkResult(input, filePath, catalystTypesToProtoMessages(dt.fields(0).dataType),
        input.eval())
    }
  }

  private def checkDeserialization(
                                    descFilePath: String,
                                    messageName: String,
                                    data: Message,
                                    expected: Option[Any],
                                    filters: StructFilters = new NoopFilters): Unit = {

    val descriptor = ProtoUtils.buildDescriptor(descFilePath, messageName)
    val dataType = SchemaConverters.toSqlType(descriptor).dataType

    val deserializer = new ProtoDeserializer(
      descriptor,
      dataType,
      false,
      RebaseSpec(SQLConf.LegacyBehaviorPolicy.CORRECTED),
      filters)

    val dynMsg = DynamicMessage.parseFrom(descriptor, data.toByteArray)
    val deserialized = deserializer.deserialize(dynMsg)
    expected match {
      case None => assert(deserialized == None)
      case Some(d) =>
        assert(checkResult(d, deserialized.get, dataType, exprNullable = false))
    }
  }

  test("SPARK-32346: filter push-down to Avro deserializer") {

    val filePath = testFile("protobuf/catalyst_types.desc").replace("file:/", "/")
    val sqlSchema = new StructType().add("age", "int").add("name", "string")

    val person = Person.newBuilder()
      .setName("Maxim")
      .setAge(39)
      .build()

    val expectedRow = Some(InternalRow(UTF8String.fromString("Maxim"), 39))

    checkDeserialization(filePath, "Person", person, expectedRow)

    /*
    checkDeserialization(
      filePath,
      "Person",
      person,
      expectedRow,
      new OrderedFilters(Seq(EqualTo("age", 39)), sqlSchema))

    checkDeserialization(
      filePath,
      "Person",
      person,
      None,
      new OrderedFilters(Seq(Not(EqualTo("name", "Maxim"))), sqlSchema))*/
  }

  test("ProtoDeserializer with binary type") {

    val filePath = testFile("protobuf/catalyst_types.desc").replace("file:/", "/")
    val bb = java.nio.ByteBuffer.wrap(Array[Byte](97, 48, 53))

    val bytesMessage = BytesMsg.newBuilder()
      .setBytesType(ByteString.copyFrom(bb))
      .build()

    val expected = InternalRow(Array[Byte](97, 48, 53))
    checkDeserialization(filePath, "BytesMsg", bytesMessage, Some(expected))
  }
}
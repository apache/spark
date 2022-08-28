package org.apache.spark.sql.proto

import com.google.protobuf.Descriptors.Descriptor
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}

import org.apache.spark.sql.types._

class ProtoCatalystDataConversionSuite extends SparkFunSuite
  with SharedSparkSession
  with ExpressionEvalHelper {

  val SIMPLE_MESSAGE = "protobuf/simple_message.desc"
  val simpleMessagePath = testFile(SIMPLE_MESSAGE).replace("file:/", "/")

  private def roundTripTest(data: Literal): Unit = {
    val protoDesc = ProtoUtils.buildDescriptor(simpleMessagePath, "SimpleMessage")
    checkResult(data, protoDesc, data.eval())
  }

  private def checkResult(data: Literal, protoDesc: Descriptor, expected: Any): Unit = {
    checkEvaluation(
      ProtoDataToCatalyst(CatalystDataToProto(data, simpleMessagePath, "SimpleMessage"), simpleMessagePath, "SimpleMessage", Map.empty),
      prepareExpectedResult(expected))
  }

  protected def checkUnsupportedRead(data: Literal, protoDesc: Descriptor): Unit = {
    val binary = CatalystDataToProto(data, simpleMessagePath, "SimpleMessage")
    intercept[Exception] {
      ProtoDataToCatalyst(binary, simpleMessagePath, "SimpleMessage", Map.empty).eval()
    }

    val expected = {
      val protoSchema = ProtoUtils.buildDescriptor(simpleMessagePath, "SimpleMessage")
      SchemaConverters.toSqlType(protoSchema).dataType match {
        case st: StructType => Row.fromSeq((0 until st.length).map(_ => null))
        case _ => null
      }
    }

    checkEvaluation(ProtoDataToCatalyst(binary, simpleMessagePath, "SimpleMessage", Map.empty),
      expected)
  }

  private val testingTypes = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType(8, 0), // 32 bits decimal without fraction
    DecimalType(8, 4), // 32 bits decimal
    DecimalType(16, 0), // 64 bits decimal without fraction
    DecimalType(16, 11), // 64 bits decimal
    DecimalType(38, 0),
    DecimalType(38, 38),
    StringType,
    BinaryType)

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

  testingTypes.foreach { dt =>
    val seed = scala.util.Random.nextLong()
    test(s"single $dt with seed $seed") {
      val rand = new scala.util.Random(seed)
      val data = RandomDataGenerator.forType(dt, rand = rand).get.apply()
      val converter = CatalystTypeConverters.createToCatalystConverter(dt)
      val input = Literal.create(converter(data), dt)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomSchema(rand, 5, testingTypes)
    test(s"flat schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

  for (_ <- 1 to 5) {
    val seed = scala.util.Random.nextLong()
    val rand = new scala.util.Random(seed)
    val schema = RandomDataGenerator.randomNestedSchema(rand, 10, testingTypes)
    test(s"nested schema ${schema.catalogString} with seed $seed") {
      val data = RandomDataGenerator.randomRow(rand, schema)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      val input = Literal.create(converter(data), schema)
      roundTripTest(input)
    }
  }

}

package org.apache.spark.sql.parquet

import java.net.URL

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, QueryTest, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Attribute}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.FunSuite
import parquet.schema.{GroupType, PrimitiveType, MessageType}

import scala.collection.mutable.ArrayBuffer

class ProtoParquetTypesConverterTest extends QueryTest with ParquetTest {
  override val sqlContext: SQLContext = TestSQLContext

  test("parquet-schema conversion retains repeated primitive type") {
    val actualSchema: MessageType = new MessageType("root", new PrimitiveType(parquet.schema.Type.Repetition.REPEATED,PrimitiveType.PrimitiveTypeName.INT32,"repeated_field"))
    val attributes: Seq[Attribute] = ParquetTypesConverter.convertToAttributes(actualSchema,isBinaryAsString = false,isInt96AsTimestamp = true)
    val convertedSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes, isProtobufSchema = true)
    assert(actualSchema === convertedSchema)
  }

  test("parquet-schema conversion retains repeated group type") {
    val actualSchema: MessageType = new MessageType("root", new GroupType(parquet.schema.Type.Repetition.REPEATED,"inner",new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL,PrimitiveType.PrimitiveTypeName.DOUBLE,"something")))
    val attributes: Seq[Attribute] = ParquetTypesConverter.convertToAttributes(actualSchema,isBinaryAsString = false,isInt96AsTimestamp = true)
    val convertedSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes, isProtobufSchema = true)
    assert(actualSchema === convertedSchema)
  }

  test("paquet-schema conversion retains arrays nested within groups") {
    val field1: PrimitiveType = new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "something")
    val repeated: PrimitiveType = new PrimitiveType(parquet.schema.Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT96, "an_int")
    val actualSchema: MessageType = new MessageType("root",
      new GroupType(parquet.schema.Type.Repetition.REQUIRED,"my_struct",field1, repeated))
    val attributes: Seq[Attribute] = ParquetTypesConverter.convertToAttributes(actualSchema,isBinaryAsString = false,isInt96AsTimestamp = true)
    val convertedSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes, isProtobufSchema = true)
    assert(actualSchema === convertedSchema)
  }

  test("paquet-schema conversion retains multiple nesting") {

    val actualSchema: MessageType = new MessageType("root",
      new GroupType(parquet.schema.Type.Repetition.REPEATED,"outer",
        new GroupType(parquet.schema.Type.Repetition.OPTIONAL, "inner",
          new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL,PrimitiveType.PrimitiveTypeName.DOUBLE,"something"),
          new GroupType(parquet.schema.Type.Repetition.REPEATED,"inner_inner",
            new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL,PrimitiveType.PrimitiveTypeName.DOUBLE,"something_else")))))

    val attributes: Seq[Attribute] = ParquetTypesConverter.convertToAttributes(actualSchema,isBinaryAsString = false,isInt96AsTimestamp = true)
    val convertedSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes, isProtobufSchema = true)
    assert(actualSchema === convertedSchema)

  }

  test("should work with repeated primitive") {
    val resource: URL = getClass.getResource("/old-repeated-int.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_test_table")
    val rows: Array[Row] = sqlContext.sql("select * from my_test_table").collect()
    val ints: ArrayBuffer[Int] = rows(0)(0).asInstanceOf[ArrayBuffer[Int]]
    assert(ints(0) === 1)
    assert(ints(1) === 2)
    assert(ints(2) === 3)
    assert(ints.length === 3)
  }

  test("should work with repeated complex") {
    val resource: URL = getClass.getResource("/old-repeated-message.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_complex_table")
    val rows: Array[Row] = sqlContext.sql("select * from my_complex_table").collect()
    val array: ArrayBuffer[GenericRow] = rows(0)(0).asInstanceOf[ArrayBuffer[GenericRow]]
    assert(array.length === 3)
  }

  test("should work with repeated complex2") {
    val resource: URL = getClass.getResource("/proto-repeated-struct.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_complex_table")
    val rows: Array[Row] = sqlContext.sql("select * from my_complex_table").collect()
    assert(rows.length === 1)
    val array: ArrayBuffer[GenericRow] = rows(0)(0).asInstanceOf[ArrayBuffer[GenericRow]]
    assert(array.length === 2)
    assert(array(0)(0) === "0 - 1")
    assert(array(0)(1) === "0 - 2")
    assert(array(0)(2) === "0 - 3")
    assert(array(1)(0) === "1 - 1")
    assert(array(1)(1) === "1 - 2")
    assert(array(1)(2) === "1 - 3")
  }

  test("should work with repeated complex with many rows") {
    val resource: URL = getClass.getResource("/proto-struct-with-array-many.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_complex_table")
    val rows: Array[Row] = sqlContext.sql("select * from my_complex_table").collect()
    assert(rows.length === 3)
    val row0: ArrayBuffer[GenericRow] = rows(0)(0).asInstanceOf[ArrayBuffer[GenericRow]]
    val row1: ArrayBuffer[GenericRow] = rows(1)(0).asInstanceOf[ArrayBuffer[GenericRow]]
    val row2: ArrayBuffer[GenericRow] = rows(2)(0).asInstanceOf[ArrayBuffer[GenericRow]]
    assert(row0(0)(0) === "0 - 0 - 1")
    assert(row0(0)(1) === "0 - 0 - 2")
    assert(row0(0)(2) === "0 - 0 - 3")
    assert(row0(1)(0) === "0 - 1 - 1")
    assert(row0(1)(1) === "0 - 1 - 2")
    assert(row0(1)(2) === "0 - 1 - 3")
    assert(row1(0)(0) === "1 - 0 - 1")
    assert(row1(0)(1) === "1 - 0 - 2")
    assert(row1(0)(2) === "1 - 0 - 3")
    assert(row1(1)(0) === "1 - 1 - 1")
    assert(row1(1)(1) === "1 - 1 - 2")
    assert(row1(1)(2) === "1 - 1 - 3")
    assert(row2(0)(0) === "2 - 0 - 1")
    assert(row2(0)(1) === "2 - 0 - 2")
    assert(row2(0)(2) === "2 - 0 - 3")
    assert(row2(1)(0) === "2 - 1 - 1")
    assert(row2(1)(1) === "2 - 1 - 2")
    assert(row2(1)(2) === "2 - 1 - 3")
  }

  test("should work with complex type containing array") {
    val resource: URL = getClass.getResource("/proto-struct-with-array.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_complex_struct")
    val rows: Array[Row] = sqlContext.sql("select * from my_complex_struct").collect()
    assert(rows.length === 1)
    val theRow: GenericRow = rows(0).asInstanceOf[GenericRow]
    val optionalStruct = theRow(3).asInstanceOf[GenericRow]
    val requiredStruct = theRow(4).asInstanceOf[GenericRow]
    val arrayOfStruct = theRow(5).asInstanceOf[ArrayBuffer[GenericRow]]
    assert(theRow.length === 6)
    assert(theRow(0) === 10)
    assert(theRow(1) === 9)
    assert(theRow(2) == null)
    assert(optionalStruct === null)
    assert(requiredStruct(0) === 9)
    assert(arrayOfStruct(0)(0) === 9)
    assert(arrayOfStruct(1)(0) === 10)
  }

  test("should work with mulitple levels of nesting") {
    val resource: URL = getClass.getResource("/nested-array-struct.parquet")
    val pf: DataFrame = sqlContext.parquetFile(resource.toURI.toString)
    pf.registerTempTable("my_nested_struct")
    val rows: Array[Row] = sqlContext.sql("select * from my_nested_struct").collect()
    assert(rows.length === 3)
    val row0: GenericRow = rows(0).asInstanceOf[GenericRow]
    val row1: GenericRow = rows(1).asInstanceOf[GenericRow]
    val row2: GenericRow = rows(2).asInstanceOf[GenericRow]
    val nestedR0: ArrayBuffer[GenericRow] = row0(1).asInstanceOf[ArrayBuffer[GenericRow]]
    val nestedR1: ArrayBuffer[GenericRow] = row1(1).asInstanceOf[ArrayBuffer[GenericRow]]
    val nestedR2: ArrayBuffer[GenericRow] = row2(1).asInstanceOf[ArrayBuffer[GenericRow]]
    val nestedR0Array: ArrayBuffer[GenericRow] = nestedR0(0)(1).asInstanceOf[ArrayBuffer[GenericRow]]
    val nestedR1Array: ArrayBuffer[GenericRow] = nestedR1(0)(1).asInstanceOf[ArrayBuffer[GenericRow]]
    val nestedR2Array: ArrayBuffer[GenericRow] = nestedR2(0)(1).asInstanceOf[ArrayBuffer[GenericRow]]
    assert(row0(0) === 2)
    assert(row1(0) === 5)
    assert(row2(0) === 8)
    assert(nestedR0.length ===1)
    assert(nestedR1.length ===1)
    assert(nestedR2.length ===1)
    assert(nestedR0(0)(0) === 1)
    assert(nestedR1(0)(0) === 4)
    assert(nestedR2(0)(0) === 7)
    assert(nestedR0Array(0)(0) === 3)
    assert(nestedR1Array(0)(0) === 6)
    assert(nestedR2Array(0)(0) === 9)
  }
}

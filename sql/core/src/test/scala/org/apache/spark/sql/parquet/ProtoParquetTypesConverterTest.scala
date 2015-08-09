package org.apache.spark.sql.parquet

import java.net.URL

import org.apache.parquet.schema.{GroupType, PrimitiveType, MessageType}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, QueryTest, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Attribute}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.FunSuite


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ProtoParquetTypesConverterTest extends QueryTest with ParquetTest {
  override val sqlContext: SQLContext = TestSQLContext

  def fetchRows(file: String, table: String ): Array[Row] = {
    val resource: URL = getClass.getResource(file)
    val pf: DataFrame = sqlContext.read.parquet(resource.toURI.toString)
    pf.registerTempTable(table)
    sqlContext.sql("select * from "  + table).collect()
  }

  test("should work with repeated primitive") {
    val rows: Array[Row] = fetchRows("/old-repeated-int.parquet", "repeated_int")
    assert(rows(0) == Row(Seq(1,2,3)))
  }

  test("should work with repeated complex") {
    val rows: Array[Row] = fetchRows("/old-repeated-message.parquet", "repeated_struct")
    val array: mutable.WrappedArray[GenericRow] = rows(0)(0).asInstanceOf[mutable.WrappedArray[GenericRow]]
    assert(array.length === 3)
    assert(array(0)=== Row("First inner",null,null))
    assert(array(1) === Row(null,"Second inner",null))
    assert(array(2) === Row(null, null,"Third inner"))
  }


  test("should work with repeated complex with more than one item in array") {
    val rows: Array[Row] = fetchRows("/proto-repeated-struct.parquet", "my_complex_table")
    assert(rows.length === 1)
    val array: mutable.WrappedArray[GenericRow] = rows(0)(0).asInstanceOf[mutable.WrappedArray[GenericRow]]
    assert(array.length === 2)
    assert(array(0) === Row("0 - 1", "0 - 2", "0 - 3"))
    assert(array(1) === Row("1 - 1", "1 - 2", "1 - 3"))
  }

  test("should work with repeated complex with many rows") {
    val rows: Array[Row] = fetchRows("/proto-struct-with-array-many.parquet", "many_complex_rows")
    assert(rows.length === 3)
    val row0: mutable.WrappedArray[GenericRow] = rows(0)(0).asInstanceOf[mutable.WrappedArray[GenericRow]]
    val row1: mutable.WrappedArray[GenericRow] = rows(1)(0).asInstanceOf[mutable.WrappedArray[GenericRow]]
    val row2: mutable.WrappedArray[GenericRow] = rows(2)(0).asInstanceOf[mutable.WrappedArray[GenericRow]]
    assert(row0(0) === Row("0 - 0 - 1", "0 - 0 - 2","0 - 0 - 3"))
    assert(row0(1)=== Row("0 - 1 - 1", "0 - 1 - 2", "0 - 1 - 3"))
    assert(row1(0) === Row("1 - 0 - 1", "1 - 0 - 2","1 - 0 - 3"))
    assert(row1(1) === Row("1 - 1 - 1", "1 - 1 - 2", "1 - 1 - 3"))
    assert(row2(0) === Row("2 - 0 - 1", "2 - 0 - 2","2 - 0 - 3"))
    assert(row2(1) === Row("2 - 1 - 1", "2 - 1 - 2", "2 - 1 - 3"))
  }

  test("should work with complex type containing array") {
    val rows: Array[Row] = fetchRows("/proto-struct-with-array.parquet", "struct_containing_array")
    assert(rows.length === 1)
    val theRow: GenericRow = rows(0).asInstanceOf[GenericRow]
    val expected = Row(10,9,null,null,Row(9),Seq(Row(9),Row(10)))
    assert(theRow === expected)
  }

  test("should work with multiple levels of nesting") {
    val rows: Array[Row] = fetchRows("/nested-array-struct.parquet", "multiple_nesting")
    assert(rows.length === 3)
    val row0: GenericRow = rows(0).asInstanceOf[GenericRow]
    val row1: GenericRow = rows(1).asInstanceOf[GenericRow]
    val row2: GenericRow = rows(2).asInstanceOf[GenericRow]
    assert(row0 === Row(2, Seq(Row(1,Seq(Row(3))))))
    assert(row1 === Row(5, Seq(Row(4,Seq(Row(6))))))
    assert(row2 === Row(8, Seq(Row(7,Seq(Row(9))))))

  }

  test("should convert array of strings") {
    val rows: Array[Row] = fetchRows("/proto-repeated-string.parquet", "strings")
    assert(rows.length === 3)
    assert(rows(0)=== Row(Seq("hello", "world")))
    assert(rows(1)=== Row(Seq("good", "bye")))
    assert(rows(2)=== Row(Seq("one","two", "three")))
  }
}

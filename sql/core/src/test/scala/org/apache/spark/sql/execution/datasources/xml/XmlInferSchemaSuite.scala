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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.sql.{DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DecimalType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}

class XmlInferSchemaSuite extends QueryTest with SharedSparkSession with TestXmlData {

  val baseOptions = Map("rowTag" -> "ROW")

  def readData(xmlString: Seq[String], options: Map[String, String] = Map.empty): DataFrame = {
    val dataset = spark.createDataset(spark.sparkContext.parallelize(xmlString))(Encoders.STRING)
    spark.read.options(baseOptions ++ options).xml(dataset)
  }

  // TODO: add tests for type widening
  test("Type conflict in primitive field values") {
    val xmlDF = readData(primitiveFieldValueTypeConflict, Map("nullValue" -> ""))
    val expectedSchema = StructType(
      StructField("num_bool", StringType, true) ::
      StructField("num_num_1", LongType, true) ::
      StructField("num_num_2", DoubleType, true) ::
      StructField("num_num_3", DoubleType, true) ::
      StructField("num_str", StringType, true) ::
      StructField("str_bool", StringType, true) :: Nil
    )
    val expectedAns = Row("true", 11L, null, 1.1, "13.1", "str1") ::
      Row("12", null, 21474836470.9, null, null, "true") ::
      Row("false", 21474836470L, 92233720368547758070d, 100, "str1", "false") ::
      Row(null, 21474836570L, 1.1, 21474836470L, "92233720368547758070", null) :: Nil
    assert(expectedSchema == xmlDF.schema)
    checkAnswer(xmlDF, expectedAns)
  }

  test("Type conflict in complex field values") {
    val xmlDF = readData(
      complexFieldValueTypeConflict,
      Map("nullValue" -> "", "ignoreSurroundingSpaces" -> "true")
    )
    // XML will merge an array and a singleton into an array
    val expectedSchema = StructType(
      StructField("array", ArrayType(LongType, true), true) ::
      StructField("num_struct", StringType, true) ::
      StructField("str_array", ArrayType(StringType), true) ::
      StructField("struct", StructType(StructField("field", StringType, true) :: Nil), true) ::
      StructField("struct_array", ArrayType(StringType), true) :: Nil
    )

    assert(expectedSchema === xmlDF.schema)
    checkAnswer(
      xmlDF,
      Row(Seq(null), "11", Seq("1", "2", "3"), Row(null), Seq(null)) ::
      Row(Seq(null), """<field>false</field>""", Seq(null), Row(null), Seq(null)) ::
      Row(Seq(4, 5, 6), null, Seq("str"), Row(null), Seq("7", "8", "9")) ::
      Row(Seq(7), null, Seq("str1", "str2", "33"), Row("str"), Seq("""<field>true</field>""")) ::
      Nil
    )
  }

  test("Type conflict in array elements") {
    val xmlDF =
      readData(
        arrayElementTypeConflict,
        Map("ignoreSurroundingSpaces" -> "true", "nullValue" -> ""))

    val expectedSchema = StructType(
      StructField(
        "array1",
        ArrayType(StructType(StructField("element", ArrayType(StringType)) :: Nil), true),
        true
      ) ::
      StructField(
        "array2",
        ArrayType(StructType(StructField("field", LongType, true) :: Nil), true),
        true
      ) ::
      StructField("array3", ArrayType(StringType, true), true) :: Nil
    )

    assert(xmlDF.schema === expectedSchema)
    checkAnswer(
      xmlDF,
      Row(
        Seq(
          Row(List("1", "1.1", "true", null, "<array></array>", "<object></object>")),
          Row(
            List(
              """<array>
            |            <element>2</element>
            |            <element>3</element>
            |            <element>4</element>
            |         </array>""".stripMargin,
              """<object>
            |            <field>str</field>
            |         </object>""".stripMargin
            )
          )
        ),
        Seq(Row(214748364700L), Row(1)),
        null
      ) ::
      Row(null, null, Seq("""<field>str</field>""", """<field>1</field>""")) ::
      Row(null, null, Seq("1", "2", "3")) :: Nil
    )
  }

  test("Handling missing fields") {
    val xmlDF = readData(missingFields)

    val expectedSchema = StructType(
      StructField("a", BooleanType, true) ::
      StructField("b", LongType, true) ::
      StructField("c", ArrayType(LongType, true), true) ::
      StructField("d", StructType(StructField("field", BooleanType, true) :: Nil), true) ::
      StructField("e", StringType, true) :: Nil
    )

    assert(expectedSchema === xmlDF.schema)

  }

  test("Complex field and type inferring") {
    val xmlDF = readData(complexFieldAndType1, Map("prefersDecimal" -> "true"))
    val expectedSchema = StructType(
      StructField(
        "arrayOfArray1",
        ArrayType(StructType(StructField("item", ArrayType(StringType, true)) :: Nil)),
        true
      ) ::
      StructField(
        "arrayOfArray2",
        ArrayType(StructType(StructField("item", ArrayType(DecimalType(21, 1), true)) :: Nil), true)
      ) ::
      StructField("arrayOfBigInteger", ArrayType(DecimalType(21, 0), true), true) ::
      StructField("arrayOfBoolean", ArrayType(BooleanType, true), true) ::
      StructField("arrayOfDouble", ArrayType(DoubleType, true), true) ::
      StructField("arrayOfInteger", ArrayType(LongType, true), true) ::
      StructField("arrayOfLong", ArrayType(DecimalType(20, 0), true), true) ::
      StructField("arrayOfNull", ArrayType(StringType, true), true) ::
      StructField("arrayOfString", ArrayType(StringType, true), true) ::
      StructField(
        "arrayOfStruct",
        ArrayType(
          StructType(
            StructField("field1", BooleanType, true) ::
            StructField("field2", StringType, true) ::
            StructField("field3", StringType, true) :: Nil
          ),
          true
        ),
        true
      ) ::
      StructField(
        "struct",
        StructType(
          StructField("field1", BooleanType, true) ::
          StructField("field2", DecimalType(20, 0), true) :: Nil
        ),
        true
      ) ::
      StructField(
        "structWithArrayFields",
        StructType(
          StructField("field1", ArrayType(LongType, true), true) ::
          StructField("field2", ArrayType(StringType, true), true) :: Nil
        ),
        true
      ) :: Nil
    )
    assert(expectedSchema === xmlDF.schema)
  }

  test("complex arrays") {
    val xmlDF = readData(complexFieldAndType2)
    val expectedSchemaArrayOfArray1 = new StructType().add(
      "arrayOfArray1",
      ArrayType(
        new StructType()
          .add("array", ArrayType(new StructType().add("item", ArrayType(LongType))))
      )
    )
    assert(xmlDF.select("arrayOfArray1").schema === expectedSchemaArrayOfArray1)
    checkAnswer(
      xmlDF.select("arrayOfArray1"),
      Row(
        Seq(
          Row(Seq(Row(Seq(5)))),
          Row(Seq(Row(Seq(6, 7)), Row(Seq(8))))
        )
      ) :: Nil
    )
    val expectedSchemaArrayOfArray2 = new StructType().add(
      "arrayOfArray2",
      ArrayType(
        new StructType()
          .add(
            "array",
            ArrayType(
              new StructType().add(
                "item",
                ArrayType(
                  new StructType()
                    .add("inner1", StringType)
                    .add("inner2", ArrayType(StringType))
                    .add("inner3", ArrayType(new StructType().add("inner4", ArrayType(LongType))))
                )
              )
            )
          )
      )
    )
    assert(xmlDF.select("arrayOfArray2").schema === expectedSchemaArrayOfArray2)
    checkAnswer(
      xmlDF.select("arrayOfArray2"),
      Row(
        Seq(
          Row(Seq(Row(Seq(Row("str1", null, null))))),
          Row(
            Seq(
              Row(null),
              Row(Seq(Row(null, Seq("str3", "str33"), null), Row("str11", Seq("str4"), null)))
            )
          ),
          Row(Seq(Row(Seq(Row(null, null, Seq(Row(Seq(2, 3)), Row(null)))))))
        )
      ) :: Nil
    )
  }

  test("Complex field and type inferring with null in sampling") {
    val xmlDF = readData(xmlNullStruct)
    val expectedSchema = StructType(
      StructField(
        "headers",
        StructType(
          StructField("Charset", StringType, true) ::
          StructField("Host", StringType, true) :: Nil
        ),
        true
      ) ::
      StructField("ip", StringType, true) ::
      StructField("nullstr", StringType, true) :: Nil
    )

    assert(expectedSchema === xmlDF.schema)
    checkAnswer(
      xmlDF.select("nullStr", "headers.Host"),
      Seq(Row("", "1.abc.com"), Row("", null), Row("", null), Row("", null))
    )
  }

  test("empty records") {
    val emptyDF = readData(emptyRecords)
    val expectedSchema = new StructType()
      .add(
        "a",
        new StructType()
          .add(
            "struct",
            StructType(StructField("b", StructType(StructField("c", StringType) :: Nil)) :: Nil)))
      .add(
        "b",
        new StructType()
          .add(
            "item",
            ArrayType(
              new StructType().add("c", StructType(StructField("struct", StringType) :: Nil)))))
    assert(emptyDF.schema === expectedSchema)
  }

}

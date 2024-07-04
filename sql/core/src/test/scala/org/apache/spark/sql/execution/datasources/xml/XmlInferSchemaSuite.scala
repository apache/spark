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

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DecimalType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

class XmlInferSchemaSuite extends QueryTest with SharedSparkSession with TestXmlData {

  private val baseOptions = Map("rowTag" -> "ROW")

  private val ignoreSurroundingSpacesOptions = Map("ignoreSurroundingSpaces" -> "true")

  private val notIgnoreSurroundingSpacesOptions = Map("ignoreSurroundingSpaces" -> "false")

  private val valueTagName = "_VALUE"

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

  test("nulls in arrays") {
    val expectedSchema = StructType(
      StructField(
        "field1",
        ArrayType(
          new StructType()
            .add("array1", ArrayType(new StructType().add("array2", ArrayType(StringType))))
        )
      ) ::
      StructField(
        "field2",
        ArrayType(
          new StructType()
            .add("array1", ArrayType(StructType(StructField("Test", LongType) :: Nil)))
        )
      ) :: Nil
    )
    val expectedAns = Seq(
      Row(Seq(Row(Seq(Row(Seq("value1", "value2")), Row(null))), Row(null)), null),
      Row(null, Seq(Row(null), Row(Seq(Row(1), Row(null))))),
      Row(Seq(Row(null), Row(Seq(Row(null)))), Seq(Row(null)))
    )
    val xmlDF = readData(nullsInArrays)
    assert(xmlDF.schema === expectedSchema)
    checkAnswer(xmlDF, expectedAns)
  }

  test("corrupt records: fail fast mode") {
    // fail fast mode is covered in the testcase: DSL test for failing fast in XmlSuite
    val schemaOne = StructType(
      StructField("a", StringType, true) ::
      StructField("b", StringType, true) ::
      StructField("c", StringType, true) :: Nil
    )
    // `DROPMALFORMED` mode should skip corrupt records
    val xmlDFOne = readData(corruptRecords, Map("mode" -> "DROPMALFORMED"))
    checkAnswer(
      xmlDFOne,
      Row("1", "2", null) ::
      Row("str_a_4", "str_b_4", "str_c_4") :: Nil
    )
    assert(xmlDFOne.schema === schemaOne)
  }

  test("turn non-nullable schema into a nullable schema") {
    // XML field is missing.
    val missingFieldInput = """<ROW><c1>1</c1></ROW>"""
    val missingFieldInputDS =
      spark.createDataset(spark.sparkContext.parallelize(missingFieldInput :: Nil))(Encoders.STRING)
    // XML filed is null.
    val nullValueInput = """<ROW><c1>1</c1><c2/></ROW>"""
    val nullValueInputDS =
      spark.createDataset(spark.sparkContext.parallelize(nullValueInput :: Nil))(Encoders.STRING)

    val schema = StructType(
      Seq(
        StructField("c1", IntegerType, nullable = false),
        StructField("c2", IntegerType, nullable = false)
      )
    )
    val expected = schema.asNullable

    Seq(missingFieldInputDS, nullValueInputDS).foreach { xmlStringDS =>
      Seq("DROPMALFORMED", "FAILFAST", "PERMISSIVE").foreach { mode =>
        val df = spark.read
          .option("mode", mode)
          .option("rowTag", "ROW")
          .schema(schema)
          .xml(xmlStringDS)
        assert(df.schema == expected)
        checkAnswer(df, Row(1, null) :: Nil)
      }
      withSQLConf(SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION.key -> "true") {
        checkAnswer(
          spark.read
            .schema(
              StructType(
                StructField("c1", LongType, nullable = false) ::
                StructField("c2", LongType, nullable = false) :: Nil
              )
            )
            .option("rowTag", "ROW")
            .option("mode", "DROPMALFORMED")
            .xml(xmlStringDS),
          // It is for testing legacy configuration. This is technically a bug as
          // `0` has to be `null` but the schema is non-nullable.
          Row(1, 0)
        )
      }
    }
  }

  test("XML with partitions") {
    def makePartition(rdd: RDD[String], parent: File, partName: String, partValue: Any): File = {
      val p = new File(parent, s"$partName=${partValue.toString}")
      rdd.saveAsTextFile(p.getCanonicalPath)
      p
    }

    withTempPath(root => {
      withTempView("test_myxml_with_part") {
        val d1 = new File(root, "d1=1")
        // root/d1=1/col1=abc
        makePartition(
          sparkContext.parallelize(2 to 5).map(i => s"""<ROW><a>1</a><b>str$i</b></ROW>"""),
          d1,
          "col1",
          "abc"
        )

        // root/d1=1/col1=abd
        makePartition(
          sparkContext.parallelize(6 to 10).map(i => s"""<ROW><a>1</a><c>str$i</c></ROW>"""),
          d1,
          "col1",
          "abd"
        )
        val expectedSchema = new StructType()
          .add("a", LongType)
          .add("b", StringType)
          .add("c", StringType)
          .add("d1", IntegerType)
          .add("col1", StringType)

        val df = spark.read.option("rowTag", "ROW").xml(root.getAbsolutePath)
        assert(df.schema === expectedSchema)
        assert(df.where(col("d1") === 1).where(col("col1") === "abc").select("a").count() == 4)
        assert(df.where(col("d1") === 1).where(col("col1") === "abd").select("a").count() == 5)
        assert(df.where(col("d1") === 1).select("a").count() == 9)
      }
    })
  }

  test("value tag - type conflict and root level value tags") {
    val xmlDF = readData(valueTagsTypeConflict, ignoreSurroundingSpacesOptions)
    val expectedSchema = new StructType()
      .add(valueTagName, ArrayType(StringType))
      .add(
        "a",
        new StructType()
          .add(valueTagName, LongType)
          .add("b", new StructType().add(valueTagName, StringType).add("c", LongType))
      )
    assert(xmlDF.schema == expectedSchema)
    val expectedAns = Seq(
      Row(Seq("13.1", "string"), Row(11, Row("true", 1))),
      Row(Seq("string", "true"), Row(21474836470L, Row("false", 2))),
      Row(Seq("92233720368547758070"), Row(null, Row("12", 3)))
    )
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tag - spaces and empty values") {
    val expectedSchema = new StructType()
      .add(valueTagName, ArrayType(StringType))
      .add("a", new StructType().add(valueTagName, StringType).add("b", LongType))
    // even though we don't ignore the surrounding spaces of characters,
    // we won't put whitespaces as value tags :)
    val xmlDFWSpaces =
      readData(emptyValueTags, notIgnoreSurroundingSpacesOptions)
    val xmlDFWOSpaces = readData(emptyValueTags, ignoreSurroundingSpacesOptions)
    assert(xmlDFWSpaces.schema == expectedSchema)
    assert(xmlDFWOSpaces.schema == expectedSchema)

    val expectedAnsWSpaces = Seq(
      Row(Seq("\n    str1\n    ", "str2\n"), Row(null, 1)),
      Row(null, Row(" value", null)),
      Row(null, Row(null, 3)),
      Row(Seq("\n    str3\n"), Row(null, 4))
    )
    checkAnswer(xmlDFWSpaces, expectedAnsWSpaces)
    val expectedAnsWOSpaces = Seq(
      Row(Seq("str1", "str2"), Row(null, 1)),
      Row(null, Row("value", null)),
      Row(null, Row(null, 3)),
      Row(Seq("str3"), Row(null, 4))
    )
    checkAnswer(xmlDFWOSpaces, expectedAnsWOSpaces)
  }

  test("value tags - multiple lines") {
    val xmlDF = readData(multilineValueTags, ignoreSurroundingSpacesOptions)
    val expectedSchema =
      new StructType().add(valueTagName, ArrayType(StringType)).add("a", LongType)
    val expectedAns = Seq(
      Row(Seq("value1", "value2"), 1),
      Row(Seq("value3\n    value4"), 1)
    )
    assert(xmlDF.schema == expectedSchema)
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tags - around structs") {
    val xmlDF = readData(valueTagsAroundStructs)
    val expectedSchema = new StructType()
      .add(valueTagName, ArrayType(StringType))
      .add(
        "a",
        new StructType()
          .add(valueTagName, ArrayType(StringType))
          .add("b", new StructType().add(valueTagName, LongType).add("c", LongType))
      )

    assert(xmlDF.schema == expectedSchema)
    val expectedAns = Seq(
      Row(
        Seq("value1", "value5"),
        Row(Seq("value2", "value4"), Row(3, 1))
      ),
      Row(
        Seq("value6"),
        Row(Seq("value4", "value5"), Row(null, null))
      ),
      Row(
        Seq("value1", "value5"),
        Row(Seq("value2", "value4"), Row(3, null))
      ),
      Row(
        Seq("value1"),
        Row(Seq("value2", "value4"), Row(3, null))
      )
    )
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tags - around arrays") {
    val xmlDF = readData(valueTagsAroundArrays)
    val expectedSchema = new StructType()
      .add(valueTagName, ArrayType(StringType))
      .add(
        "array1",
        ArrayType(
          new StructType()
            .add(valueTagName, ArrayType(StringType))
            .add(
              "array2",
              ArrayType(new StructType()
                // The value tag is not of long type due to:
                // 1. when we infer the type for the array2 in the second array1,
                // it combines a struct type and a primitive type and results in a string type
                // 2. when we merge the inferred type for the first array2 and the second,
                // we are merging a struct with longtype value tag and a string type.
                // It results in merging the long type value tag with the primitive type
                // and thus finally we got a struct with string type value tag.
                .add(valueTagName, ArrayType(StringType))
                .add("num", LongType)))))
    assert(xmlDF.schema === expectedSchema)
    val expectedAns = Seq(
      Row(
        Seq("value1", "value8", "value12", "value13"),
        Seq(
          Row(
            Seq("value2", "value3", "value4", "value5", "value6\n        value7"),
            Seq(Row(Seq("1", "2"), 1), Row(Seq("2"), null))),
          Row(
            Seq("value9", "value10", "value11"),
            Seq(Row(null, 2), Row(null, null), Row(null, null), Row(Seq("3"), null))))),
      Row(
        null,
        Seq(
          Row(
            Seq("value1"), null))),
      Row(
        Seq("value1"),
        Seq(
          Row(
            null,
            Seq(Row(Seq("1"), null))))))
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tag - user specifies a conflicting name for valueTag") {
    val xmlDF = readData(valueTagConflictName, Map("valueTag" -> "a"))
    val expectedSchema = new StructType().add("a", ArrayType(LongType))
    assert(xmlDF.schema == expectedSchema)
    checkAnswer(xmlDF, Seq(Row(Seq(1, 2))))
  }

  test("value tag - comments") {
    val xmlDF = readData(valueTagWithComments)
    val expectedSchema = new StructType()
      .add(valueTagName, LongType)
      .add("a", ArrayType(new StructType().add("_attr", LongType)))
    val expectedAns = Seq(
      Row(2, Seq(Row(null), Row(1))))
    assert(xmlDF.schema === expectedSchema)
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tags - CDATA") {
    val xmlDF = readData(valueTagWithCDATA)
    val expectedSchema = new StructType()
      .add(valueTagName, StringType)
      .add("a", new StructType()
        .add(valueTagName, ArrayType(StringType))
        .add("b", ArrayType(LongType)))

    val expectedAns = Seq(
      Row(
        "This is a CDATA section containing <sample1> text.",
        Row(
          Seq(
            "This is a CDATA section containing <sample2> text.\n" +
              "        This is a CDATA section containing <sample3> text.",
            "This is a CDATA section containing <sample4> text."
          ),
          Seq(1, 2)
        )
      )
    )
    assert(xmlDF.schema === expectedSchema)
    checkAnswer(xmlDF, expectedAns)
  }

  test("value tag - equals to null value") {
    // we don't consider options.nullValue during schema inference
    val xmlDF = readData(valueTagIsNullValue, Map("nullValue" -> "1"))
    val expectedSchema = new StructType()
      .add(valueTagName, LongType)
    val expectedAns = Seq(Row(null))
    // nullValue option is used during parsing
    assert(xmlDF.schema === expectedSchema)
    checkAnswer(xmlDF, expectedAns)
  }
}

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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.Closeable

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.io.api.{Binary, RecordConsumer}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetIdExternalMapping.EmptyMapping
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class ParquetIdExternalMappingSuite extends ParquetCompatibilityTest with SharedSparkSession {

  test("parse primitive and struct external id mapping string") {
    val json =
      """[ { "field-id": 1, "names": ["id", "record_id"] },
        |   { "field-id": 2, "names": ["data"] },
        |   { "field-id": 3, "names": ["location"], "fields": [
        |       { "field-id": 4, "names": ["latitude", "lat"] },
        |       { "field-id": 5, "names": ["longitude", "long"] }
        |     ] } ]""".stripMargin

    val mapping = ParquetIdExternalMapping.fromJson(json)
    assertResult(Some(1))(mapping.getFieldId("id"))
    assertResult(Some(1))(mapping.getFieldId("record_id"))
    assertResult(None)(mapping.getFieldId("notExist"))
    assertResult(Some(2))(mapping.getFieldId("data"))
    assertResult(Some(3))(mapping.getFieldId("location"))

    val subMapping = mapping.getChild("location")
    assertResult(Some(4))(subMapping.getFieldId("latitude"))
    assertResult(Some(4))(subMapping.getFieldId("lat"))
    assertResult(Some(5))(subMapping.getFieldId("longitude"))
    assertResult(Some(5))(subMapping.getFieldId("long"))
  }

  test("parse array and map") {
    val json =
      """[ { "field-id": 1, "names": ["struct", "struct_n1"], "fields": [
        |      {
        |         "field-id": 2, "names": ["element"], "fields": [
        |           {
        |             "field-id": 3, "names": ["list_field1"]
        |           },
        |           {
        |             "field-id": 4, "names": ["list_field2"]
        |           }
        |         ]
        |      }
        |   ] },
        |   { "field-id": 5, "names": ["location"], "fields": [
        |       { "field-id": 6, "names": ["key"], "fields": [
        |           {
        |             "field-id": 7, "names": ["key_field1"]
        |           },
        |           {
        |             "field-id": 8, "names": ["key_field2"]
        |           }
        |         ]
        |       },
        |       { "field-id": 9, "names": ["value"], "fields": [
        |           {
        |             "field-id": 10, "names": ["value_field1"]
        |           },
        |           {
        |             "field-id": 11, "names": ["value_field2"]
        |           }
        |         ]
        |       }
        |     ] } ]""".stripMargin
    val mapping = ParquetIdExternalMapping.fromJson(json)
    assertResult(Some(1))(mapping.getFieldId("struct"))
    assertResult(Some(1))(mapping.getFieldId("struct_n1"))
    assertResult(None)(mapping.getFieldId("notExist"))
    assertResult(Some(5))(mapping.getFieldId("location"))

    val structContentMapping = mapping.getChild("struct_n1").getArray
    assertResult(Some(3))(structContentMapping.getFieldId("list_field1"))
    assertResult(Some(4))(structContentMapping.getFieldId("list_field2"))

    assertResult(Some(5))(mapping.getFieldId("location"))
    val mapKeyMapping = mapping.getChild("location").getMapKey
    val mapValueMapping = mapping.getChild("location").getMapValue
    assertResult(Some(7))(mapKeyMapping.getFieldId("key_field1"))
    assertResult(Some(8))(mapKeyMapping.getFieldId("key_field2"))
    assertResult(Some(10))(mapValueMapping.getFieldId("value_field1"))
    assertResult(Some(11))(mapValueMapping.getFieldId("value_field2"))
  }

  test("parse invalid input") {
    val nullStr: String = null
    val empty = ""
    val invalidJson = "[}"
    val duplicateId =
      """[ { "field-id": 1, "names": ["id", "record_id"] },
        |   { "field-id": 2, "names": ["data"] },
        |   { "field-id": 3, "names": ["location"], "fields": [
        |       { "field-id": 3, "names": ["latitude", "lat"] },
        |       { "field-id": 5, "names": ["longitude", "long"] }
        |     ] } ]""".stripMargin
    val duplicateNameWithinFields =
      """[ { "field-id": 1, "names": ["id", "id"] },
        |   { "field-id": 2, "names": ["data"] },
        |   { "field-id": 3, "names": ["location"], "fields": [
        |       { "field-id": 3, "names": ["latitude", "lat"] },
        |       { "field-id": 5, "names": ["longitude", "long"] }
        |     ] } ]""".stripMargin
    val duplicateNameBetweenFields =
      """[ { "field-id": 1, "names": ["id", "record_id"] },
        |   { "field-id": 2, "names": ["data"] },
        |   { "field-id": 3, "names": ["location"], "fields": [
        |       { "field-id": 3, "names": ["latitude", "lat"] },
        |       { "field-id": 5, "names": ["longitude", "long"] }
        |     ] } ]""".stripMargin
    Seq(nullStr, empty, invalidJson, duplicateId,
      duplicateNameWithinFields, duplicateNameBetweenFields)
      .foreach(str => assertResult(EmptyMapping)(ParquetIdExternalMapping.fromJson(str)))
  }

  def readAndVerify(
      parquetPath: String,
      sparkSchema: StructType,
      extMapping: String): Seq[InternalRow] = {

    val fsPath = new Path(parquetPath)
    val fileSystem = fsPath.getFileSystem(spark.sessionState.newHadoopConf())
    val file = fileSystem.getFileStatus(fsPath)

    val reader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = spark,
      dataSchema = sparkSchema,
      partitionSchema = new StructType(),
      requiredSchema = sparkSchema,
      filters = Nil,
      options = Map(
        SQLConf.PARQUET_FIELD_ID_READ_EXTERNAL_MAPPING.key -> extMapping,
        FileFormat.OPTION_RETURNING_BATCH -> "false"
      ),
      hadoopConf = spark.sessionState.newHadoopConf())

    val partitionedFile = PartitionedFile(
      partitionValues = InternalRow(),
      filePath = SparkPath.fromPath(file.getPath),
      start = 0L,
      length = file.getLen
    )

    val parquetReaderIter: Iterator[Object] = reader(partitionedFile)

    val schemaAsAttribute = DataTypeUtils.toAttributes(sparkSchema)
    val toUnsafe = UnsafeProjection.create(schemaAsAttribute, schemaAsAttribute)

    val rows = ArrayBuffer[InternalRow]()
    try {
      parquetReaderIter.foreach {
        case batch: ColumnarBatch =>
          batch.rowIterator().asScala.map(toUnsafe).map(_.copy()).foreach { row =>
            rows.append(row)
          }
        case row: InternalRow =>
          rows.append(row.copy())
        case _ => throw new UnsupportedOperationException()
      }
    } finally {
      parquetReaderIter match {
        case closeable: Closeable =>
          closeable.close()
        case _ =>
      }
    }
    rows.toSeq
  }

  // InternalRow.toSeq does not convert nested InternalRows
  private def deepGet(row: InternalRow, dataType: StructType): Seq[Any] = {
    if (row == null) {
      return null
    }

    def arrayToSeq(data: ArrayData, dataType: DataType): Seq[Any] = {
      dataType match {
        case _: StringType =>
          Option(data)
            .map(_.toArray[UTF8String](dataType).toList
              .map(Option(_).map(_.toString).orNull))
            .orNull
        case _: AtomicType =>
          Option(data)
            .map(_.toArray(dataType).toList)
            .orNull
        case _: StructType =>
          Option(data)
            .map(_.toArray[InternalRow](dataType)
              .map(row => deepGet(
                row, dataType.asInstanceOf[StructType]))
              .toList)
            .orNull
        case _ =>
          throw new UnsupportedOperationException
      }
    }

    dataType.fields.toIndexedSeq.zipWithIndex.map {
      case (field, idx) =>
        field.dataType match {
          case st: StructType =>
            deepGet(row.getStruct(idx, 0), st)
          case arr: ArrayType =>
            arrayToSeq(row.getArray(idx), arr.elementType)
          case map: MapType =>
            Option(row.getMap(idx))
              .map(mapData =>
                Seq(
                  arrayToSeq(mapData.keyArray(), map.keyType),
                  arrayToSeq(mapData.valueArray(), map.valueType)
                )
              ).orNull
          case s: StringType => Option(row.get(idx, s))
            .map(_.toString).orNull // UTF8String to String
          case _ => row.get(idx, field.dataType)
        }
    }
  }

  import ParquetCompatibilityTest._

  private def testExtRead(testNamePrefix: String)(testFun: => Unit): Unit = {
    for (param <- Seq("true", "false")) {
      withSQLConf(
        SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> param) {
        test(testNamePrefix + s" ($param)")(testFun)
      }
    }
  }

  testExtRead("read Parquet with external id mapping - primitive") {
    withTempDir { dir =>
      val parquetSchema =
        """message table {
          |  required int32 id;
          |  required int32 neg_id;
          |}
        """.stripMargin
      val parquetPath = s"$dir/extid.parquet"
      val recordWriters: Seq[RecordConsumer => Unit] = Seq({ rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(7)
          }
          rc.field("neg_id", 1) {
            rc.addInteger(-7)
          }
        }
      }, { rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(9)
          }
          rc.field("neg_id", 1) {
            rc.addInteger(-9)
          }
        }
      }, { rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(11)
          }
          rc.field("neg_id", 1) {
            rc.addInteger(-11)
          }
        }
      })
      ParquetCompatibilityTest.writeDirect(
        parquetPath,
        parquetSchema,
        recordWriters: _*)

      val sparkSchema = new StructType()
        .add("id", IntegerType, nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 1)
            .build())
        .add("neg_id", IntegerType, nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 2)
            .build())
      // This ext mapping will map Spark id to Parquet neg_id
      val extMappingNeg =
        """
          |[
          | {
          |   "field-id": 2, "names": ["id", "new_id"]
          | },
          | {
          |   "field-id": 1, "names": ["old_id", "used_id", "neg_id" ]
          | }
          |]
          |""".stripMargin

      val fieldTypes = Seq(IntegerType, IntegerType)

      assertResult(Seq(Seq(-7, 7), Seq(-9, 9), Seq(-11, 11)))(
        readAndVerify(
          parquetPath,
          sparkSchema,
          extMappingNeg
        ).map(_.toSeq(fieldTypes)))

      // This ext mapping will map Spark id to Parquet id
      val extMappingNormal =
        """
          |[
          | {
          |   "field-id": 1, "names": ["id", "new_id"]
          | },
          | {
          |   "field-id": 2, "names": ["old_id", "used_id", "neg_id" ]
          | }
          |]
          |""".stripMargin

      assertResult(Seq(Seq(7, -7), Seq(9, -9), Seq(11, -11)))(
        readAndVerify(
          parquetPath,
          sparkSchema,
          extMappingNormal
        ).map(_.toSeq(fieldTypes)))
    }
  }

  testExtRead("read Parquet with external id mapping - struct") {
    withTempDir { dir =>
      val parquetSchema =
        """message table {
          |  required int64 order_id;
          |  required group customer {
          |    required int32 customer_id;
          |    optional binary name (UTF8);
          |    optional group contact {
          |      required binary email (UTF8);
          |      optional binary phone (UTF8);
          |    }
          |  }
          |}
        """.stripMargin
      val parquetPath = s"$dir/extid.parquet"
      val recordWriters: Seq[RecordConsumer => Unit] = Seq({ rc =>
        rc.message {
          rc.field("order_id", 0) {
            rc.addLong(1L)
          }
          rc.field("customer", 1) {
            rc.group {
              rc.field("customer_id", 0) {
                rc.addInteger(100)
              }
              rc.field("name", 1) {
                rc.addBinary(Binary.fromString("Customer1"))
              }
              rc.field("contact", 2) {
                rc.group {
                  rc.field("email", 0) {
                    rc.addBinary(Binary.fromString("a@b.com"))
                  }
                  rc.field("phone", 1) {
                    rc.addBinary(Binary.fromString("1234567890"))
                  }
                }
              }
            }
          }
        }
      }, { rc =>
        rc.message {
          rc.field("order_id", 0) {
            rc.addLong(5L)
          }
          rc.field("customer", 1) {
            rc.group {
              rc.field("customer_id", 0) {
                rc.addInteger(200)
              }
            }
          }
        }
      })
      ParquetCompatibilityTest.writeDirect(
        parquetPath,
        parquetSchema,
        recordWriters: _*)

      val sparkSchema = new StructType()
        .add("oid", IntegerType, nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 1)
            .build())
        .add("cust",
          new StructType()
            .add("cust_id", IntegerType, nullable = false,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 3)
                .build())
            .add("cust_name", StringType, nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 4)
                .build())
            .add("cust_contact",
              new StructType()
                .add("email", StringType, nullable = false,
                  metadata = new MetadataBuilder()
                    .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 6)
                    .build())
                .add("phone", StringType, nullable = true,
                  metadata = new MetadataBuilder()
                  .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 7)
                  .build()),
              nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 5)
                .build()),
          nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 2)
            .build())
      val extMapping =
        """
          |[
          | {
          |   "field-id": 1, "names": ["order_id"]
          | },
          | {
          |   "field-id": 2, "names": ["customer"], "fields": [
          |     {
          |       "field-id": 3, "names": ["customer_id"]
          |     },
          |     {
          |       "field-id": 4, "names": ["name"]
          |     },
          |     {
          |       "field-id": 5, "names": ["contact"], "fields": [
          |         {
          |           "field-id": 6, "names": ["email"]
          |         },
          |         {
          |           "field-id": 7, "names": ["phone"]
          |         }
          |       ]
          |     }
          |   ]
          | }
          |]
          |""".stripMargin

      val fieldTypes =
        new StructType()
          .add("oid", LongType)
          .add("customer",
            new StructType()
              .add("cust_id", IntegerType)
              .add("cust_name", StringType)
              .add("contact",
                new StructType()
                  .add("email", StringType)
                  .add("phone", StringType)
              )
            )

      val result = readAndVerify(
        parquetPath,
        sparkSchema,
        extMapping
      ).map(deepGet(_, fieldTypes))
      assertResult(Seq(
        Seq(1, Seq(100, "Customer1", Seq("a@b.com", "1234567890"))),
        Seq(5, Seq(200, null, null))
      ))(result)
    }
  }

  testExtRead("read Parquet with external id mapping - list") {
    withTempDir { dir =>
      val parquetSchema =
        """message table {
          |  required int32 id;
          |  required group tags (LIST) {
          |    repeated group list {
          |      required int32 element;
          |    }
          |  }
          |  optional group items (LIST) {
          |    repeated group list {
          |      optional group element {
          |        optional binary name (UTF8);
          |        optional double price;
          |      }
          |    }
          |  }
          |}""".stripMargin
      val parquetPath = s"$dir/extid.parquet"
      val recordWriters: Seq[RecordConsumer => Unit] = Seq({ rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(1)
          }
          rc.field("tags", 1) {
            rc.group {
              rc.field("list", 0) {
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(100)
                  }
                }
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(200)
                  }
                }
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(300)
                  }
                }
              }
            }
          }
          rc.field("items", 2) {
            rc.group {
              rc.field("list", 0) {
                rc.group {
                  rc.field("element", 0) {
                    rc.group {
                      rc.field("name", 0) {
                        rc.addBinary(Binary.fromString("name1"))
                      }
                      rc.field("price", 1) {
                        rc.addDouble(1.2)
                      }
                    }
                  }
                }
                rc.group {
                  rc.field("element", 0) {
                    rc.group {
                      rc.field("name", 0) {
                        rc.addBinary(Binary.fromString("name2"))
                      }
                      rc.field("price", 1) {
                        rc.addDouble(2.2)
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }, { rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(2)
          }
          rc.field("tags", 1) {
            rc.group {
              rc.field("list", 0) {
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(400)
                  }
                }
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(500)
                  }
                }
                rc.group {
                  rc.field("element", 0) {
                    rc.addInteger(600)
                  }
                }
              }
            }
          }
        }
      })
      ParquetCompatibilityTest.writeDirect(
        parquetPath,
        parquetSchema,
        recordWriters: _*)

      val sparkSchema = new StructType()
        .add("spark_id", IntegerType, nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 1)
            .build())
        .add("spark_tags",
          ArrayType(IntegerType),
          nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 2)
            .build())
        .add("spark_items",
          ArrayType(new StructType()
            .add("spark_name", StringType, nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 6)
                .build())
            .add("spark_price", DoubleType, nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 7)
                .build())
          ),
          nullable = true,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 3)
            .build())
      val extMapping =
        """
          |[
          | {
          |   "field-id": 1, "names": ["id"]
          | },
          | {
          |   "field-id": 2, "names": ["tags"], "fields": [
          |     {
          |       "field-id": 4, "names": ["element"]
          |     }
          |   ]
          | },
          | {
          |   "field-id": 3, "names": ["items"], "fields": [
          |     {
          |       "field-id": 5, "names": ["element"], "fields": [
          |         {
          |           "field-id": 6, "names": ["name"]
          |         },
          |         {
          |           "field-id": 7, "names": ["price"]
          |         }
          |       ]
          |     }
          |   ]
          | }
          |]
          |""".stripMargin

      val fieldTypes =
        new StructType().add("id", LongType)
          .add("tags", ArrayType(IntegerType))
          .add("items", ArrayType(
            new StructType()
              .add("name", StringType)
              .add("price", DoubleType)
          ))

      val result = readAndVerify(
        parquetPath,
        sparkSchema,
        extMapping
      ).map(deepGet(_, fieldTypes))

      assertResult(Seq(
        Seq(1, Seq(100, 200, 300), Seq(Seq("name1", 1.2), Seq("name2", 2.2))),
        Seq(2, Seq(400, 500, 600), null)
      ))(
        result
      )
    }
  }

  test("read Parquet with external id mapping - map") {
    withTempDir { dir =>
      val parquetSchema =
        """message table {
          |  required int32 id;
          |  required group simple (MAP) {
          |    repeated group key_value {
          |      required int32 key;
          |      required binary value (STRING);
          |    }
          |  }
          |  optional group complex (MAP) {
          |    repeated group key_value {
          |      required int32 key;
          |      required group value {
          |        required binary name (STRING);
          |        required double price;
          |      }
          |    }
          |  }
          |}
          |""".stripMargin
      val parquetPath = s"$dir/extid.parquet"
      val recordWriters: Seq[RecordConsumer => Unit] = Seq({ rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(1)
          }
          rc.field("simple", 1) {
            rc.group {
              rc.field("key_value", 0) {
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(100)
                  }
                  rc.field("value", 1) {
                    rc.addBinary(Binary.fromString("val1"))
                  }
                }
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(200)
                  }
                  rc.field("value", 1) {
                    rc.addBinary(Binary.fromString("val2"))
                  }
                }
              }
            }
          }
          rc.field("complex", 2) {
            rc.group {
              rc.field("key_value", 0) {
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(10)
                  }
                  rc.field("value", 1) {
                    rc.group {
                      rc.field("name", 0) {
                        rc.addBinary(Binary.fromString("name1"))
                      }
                      rc.field("price", 1) {
                        rc.addDouble(1.2)
                      }
                    }
                  }
                }
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(20)
                  }
                  rc.field("value", 1) {
                    rc.group {
                      rc.field("name", 0) {
                        rc.addBinary(Binary.fromString("name2"))
                      }
                      rc.field("price", 1) {
                        rc.addDouble(2.2)
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }, { rc =>
        rc.message {
          rc.field("id", 0) {
            rc.addInteger(2)
          }
          rc.field("simple", 1) {
            rc.group {
              rc.field("key_value", 0) {
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(400)
                  }
                  rc.field("value", 1) {
                    rc.addBinary(Binary.fromString("val4"))
                  }
                }
                rc.group {
                  rc.field("key", 0) {
                    rc.addInteger(500)
                  }
                  rc.field("value", 1) {
                    rc.addBinary(Binary.fromString("val5"))
                  }
                }
              }
            }
          }
        }
      })
      ParquetCompatibilityTest.writeDirect(
        parquetPath,
        parquetSchema,
        recordWriters: _*)

      val sparkSchema = new StructType()
        .add("spark_id", IntegerType, nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 1)
            .build())
        .add("spark_simple",
          MapType(IntegerType, StringType),
          nullable = false,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 2)
            .build())
        .add("spark_complex",
          MapType(IntegerType, new StructType()
            .add("spark_name", StringType, nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 8)
                .build())
            .add("spark_price", DoubleType, nullable = true,
              metadata = new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 9)
                .build())
          ),
          nullable = true,
          metadata = new MetadataBuilder()
            .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, 3)
            .build())
      val extMapping =
        """
          |[
          | {
          |   "field-id": 1, "names": ["id"]
          | },
          | {
          |   "field-id": 2, "names": ["simple"], "fields": [
          |     {
          |       "field-id": 4, "names": ["key"]
          |     },
          |     {
          |       "field-id": 5, "names": ["value"]
          |     }
          |   ]
          | },
          | {
          |   "field-id": 3, "names": ["complex"], "fields": [
          |     {
          |       "field-id": 6, "names": ["key"]
          |     },
          |     {
          |       "field-id": 7, "names": ["value"], "fields": [
          |         {
          |           "field-id": 8, "names": ["name"]
          |         },
          |         {
          |           "field-id": 9, "names": ["price"]
          |         }
          |       ]
          |     }
          |   ]
          | }
          |]
          |""".stripMargin

      val fieldTypes =
        new StructType().add("id", LongType)
          .add("simple", MapType(IntegerType, StringType))
          .add("complex", MapType(
            IntegerType,
            new StructType()
              .add("name", StringType)
              .add("price", DoubleType)
          ))

      val result = readAndVerify(
        parquetPath,
        sparkSchema,
        extMapping
      ).map(deepGet(_, fieldTypes))

      assertResult(Seq(
        Seq(1,
          Seq(Seq(100, 200), Seq("val1", "val2")),
          Seq(Seq(10, 20), Seq(Seq("name1", 1.2), Seq("name2", 2.2)))),
        Seq(2, Seq(Seq(400, 500), Seq("val4", "val5")), null)
      ))(result)
    }
  }
}

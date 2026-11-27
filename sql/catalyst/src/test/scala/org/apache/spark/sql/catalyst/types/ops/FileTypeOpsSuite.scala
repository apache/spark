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
package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.{SparkException, SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, DeserializerBuildHelper, InternalRow, SerializerBuildHelper}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{FileRefEncoder, OptionEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateMutableProjection, GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.types.{DataTypeUtils, PhysicalDataType, PhysicalStructType}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class FileHolder(id: Int, file: FileRef, maybe: Option[FileRef], files: Seq[FileRef])

/**
 * Tests for FileType in the Types Framework: its physical representation, the conversions
 * between [[FileRef]] and InternalRow, the row and code generation paths, formatting and the
 * FileRef encoder.
 */
class FileTypeOpsSuite extends SparkFunSuite with SQLHelper {

  private val allFields = FileRefInternals.create(
    "s3://bucket/dir/doc.pdf", 10L, 2048L, "application/pdf", "MD5:abc123", Array[Byte](1, 2, 3))

  private val uriOnly = new FileRef("/data/a.txt", null, null, null, null)

  private def fileRow(
      uri: String,
      offset: java.lang.Long = null,
      size: java.lang.Long = null,
      contentType: String = null,
      checksum: String = null,
      inline: Array[Byte] = null): InternalRow = new GenericInternalRow(Array[Any](
    UTF8String.fromString(uri), offset, size, UTF8String.fromString(contentType),
    UTF8String.fromString(checksum), inline))

  private def assertFileRow(row: InternalRow, expected: FileRef): Unit = {
    assert(FileTypeOps.toFileRef(row) === expected)
  }

  // ==================== Physical Type Representation ====================

  test("FileType has a struct physical type and row representation") {
    Seq(FileType(), FileType.external).foreach { ft =>
      val ops = TypeOps(ft).get
      assert(ops.isInstanceOf[FileTypeOps])
      assert(PhysicalDataType(ft) === PhysicalStructType(FileType.FIELDS))
      assert(ops.rowRepresentationType === Some(FileType.asStruct))
      assert(ops.getElementSize === 8)
      assert(ops.getJavaClass === classOf[InternalRow])
      assert(ops.getMutableValue.isInstanceOf[MutableAny])
      assert(TypeApiOps(ft).get.isInstanceOf[FileTypeApiOps])
    }
    assert(TypeOps.rowRepresentationOrSelf(FileType()) === FileType.asStruct)
    assert(TypeOps.rowRepresentationOrSelf(IntegerType) === IntegerType)
  }

  test("default literal") {
    val lit = Literal.default(FileType.external)
    assert(lit.dataType === FileType.external)
    assertFileRow(lit.value.asInstanceOf[InternalRow], new FileRef("", null, null, null, null))
  }

  test("there is no Java literal") {
    val e = intercept[SparkException] {
      FileTypeOps(FileType()).getJavaLiteral(fileRow("/a"))
    }
    assert(e.getCondition === "INTERNAL_ERROR")
  }

  test("literal validation") {
    Literal.create(fileRow("/a"), FileType())
    intercept[IllegalArgumentException](Literal.create("/a", FileType()))
  }

  // ==================== External Type Conversion ====================

  test("FileRef to InternalRow and back keeps every field, including inline") {
    val ops = FileTypeOps(FileType())
    Seq(allFields, uriOnly, new FileRef(null, null, 3L, "image/png", null),
        FileRefInternals.create(null, null, null, null, null, Array[Byte](7))).foreach { f =>
      val row = ops.toCatalystImpl(f).asInstanceOf[InternalRow]
      assert(row.numFields === FileType.FIELDS.length)
      assert(ops.toScala(row) === f)
    }
    val row = ops.toCatalystImpl(allFields).asInstanceOf[InternalRow]
    assert(row.getUTF8String(FileType.URI_INDEX).toString === "s3://bucket/dir/doc.pdf")
    assert(row.getLong(FileType.OFFSET_INDEX) === 10L)
    assert(row.getLong(FileType.SIZE_INDEX) === 2048L)
    assert(row.getUTF8String(FileType.CONTENT_TYPE_INDEX).toString === "application/pdf")
    assert(row.getUTF8String(FileType.CHECKSUM_INDEX).toString === "MD5:abc123")
    assert(row.getBinary(FileType.INLINE_INDEX).toSeq === Seq[Byte](1, 2, 3))
    val nulls = ops.toCatalystImpl(uriOnly).asInstanceOf[InternalRow]
    (1 until FileType.FIELDS.length).foreach(i => assert(nulls.isNullAt(i), s"field $i"))
    assert(ops.toScala(null) === null)
  }

  test("CatalystTypeConverters round-trip") {
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(FileType.external)
    val toScala = CatalystTypeConverters.createToScalaConverter(FileType.external)
    assert(toScala(toCatalyst(allFields)) === allFields)
    assert(toCatalyst(null) === null)
    val structType = new StructType().add("f", FileType.external)
    val rowToCatalyst = CatalystTypeConverters.createToCatalystConverter(structType)
    val rowToScala = CatalystTypeConverters.createToScalaConverter(structType)
    assert(rowToScala(rowToCatalyst(Row(allFields))) === Row(allFields))
  }

  // ==================== Row Access and Code Generation ====================

  test("read through SpecializedGettersReader") {
    val row = new GenericInternalRow(Array[Any](fileRow("/r.txt", size = 100L)))
    val value = SpecializedGettersReader.read(row, 0, FileType(), true, false)
    assertFileRow(value.asInstanceOf[InternalRow], new FileRef("/r.txt", null, 100L, null, null))
  }

  private val values: Seq[FileRef] = Seq(allFields, uriOnly)

  private def unsafeProjections(dt: DataType): Seq[(String, UnsafeProjection)] = {
    val exprs = Seq(BoundReference(0, dt, nullable = true))
    Seq(
      "interpreted" -> new InterpretedUnsafeProjection(exprs.toArray),
      "codegen" -> GenerateUnsafeProjection.generate(exprs))
  }

  test("unsafe projection of a FILE column, a null value and null fields") {
    val ops = FileTypeOps(FileType())
    unsafeProjections(FileType()).foreach { case (mode, projection) =>
      values.foreach { f =>
        val out = projection(new GenericInternalRow(Array[Any](ops.toCatalystImpl(f)))).copy()
        assert(FileTypeOps.toFileRef(out.getStruct(0, FileType.FIELDS.length)) === f, mode)
      }
      assert(projection(new GenericInternalRow(Array[Any](null))).isNullAt(0), mode)
    }
  }

  test("unsafe projection of an array of FILE, a map with FILE values and a struct with FILE") {
    val ops = FileTypeOps(FileType())
    val elems = values.map(ops.toCatalystImpl) :+ null
    val arrayType = ArrayType(FileType(), containsNull = true)
    unsafeProjections(arrayType).foreach { case (mode, projection) =>
      val out = projection(new GenericInternalRow(Array[Any](new GenericArrayData(elems))))
      val array = out.getArray(0)
      assert(array.numElements() === 3, mode)
      values.indices.foreach { i =>
        assert(FileTypeOps.toFileRef(array.getStruct(i, FileType.FIELDS.length)) === values(i))
      }
      assert(array.isNullAt(2), mode)
    }
    val mapType = MapType(StringType, FileType(), valueContainsNull = true)
    unsafeProjections(mapType).foreach { case (mode, projection) =>
      val map = ArrayBasedMapData(
        Array[Any](UTF8String.fromString("k1"), UTF8String.fromString("k2")),
        Array[Any](ops.toCatalystImpl(allFields), null))
      val out = projection(new GenericInternalRow(Array[Any](map))).getMap(0)
      assert(FileTypeOps.toFileRef(out.valueArray().getStruct(0, FileType.FIELDS.length)) ===
        allFields, mode)
      assert(out.valueArray().isNullAt(1), mode)
    }
    val structType = new StructType().add("id", IntegerType).add("f", FileType())
    unsafeProjections(structType).foreach { case (mode, projection) =>
      val inner = new GenericInternalRow(Array[Any](1, ops.toCatalystImpl(uriOnly)))
      val out = projection(new GenericInternalRow(Array[Any](inner))).getStruct(0, 2)
      assert(out.getInt(0) === 1, mode)
      assert(FileTypeOps.toFileRef(out.getStruct(1, FileType.FIELDS.length)) === uriOnly, mode)
    }
  }

  test("safe projection copies a FILE value out of a reused UnsafeRow") {
    val ops = FileTypeOps(FileType())
    val producer = UnsafeProjection.create(Array[DataType](FileType()))
    val input = producer(new GenericInternalRow(Array[Any](ops.toCatalystImpl(allFields))))
    val exprs = Seq(BoundReference(0, FileType(), nullable = true))
    Seq(
      "interpreted" -> InterpretedSafeProjection.createProjection(exprs),
      "codegen" -> GenerateSafeProjection.generate(exprs)).foreach { case (mode, projection) =>
      val out = projection(input)
      producer(new GenericInternalRow(Array[Any](ops.toCatalystImpl(uriOnly))))
      assert(FileTypeOps.toFileRef(out.getStruct(0, FileType.FIELDS.length)) === allFields, mode)
      producer(new GenericInternalRow(Array[Any](ops.toCatalystImpl(allFields))))
    }
  }

  // ==================== Formatting ====================

  test("format renders the canonical JSON and omits null fields") {
    val ops = FileTypeOps(FileType())
    assert(ops.format(ops.toCatalystImpl(allFields)) ===
      """{"uri":"s3://bucket/dir/doc.pdf","offset":10,"size":2048,""" +
        """"content_type":"application/pdf","checksum":"MD5:abc123","inline":"AQID"}""")
    assert(ops.format(ops.toCatalystImpl(uriOnly)) === """{"uri":"/data/a.txt"}""")
    assert(ops.format(fileRow("""a "quoted"\path""")) === """{"uri":"a \"quoted\"\\path"}""")
    assert(ops.toSQLValue(ops.toCatalystImpl(uriOnly)) === """FILE {"uri":"/data/a.txt"}""")
    // The external value renders the same way.
    assert(new FileTypeApiOps(FileType()).format(allFields) ===
      ops.format(ops.toCatalystImpl(allFields)))
    assert(ops.formatExternal(uriOnly) === Some("""{"uri":"/data/a.txt"}"""))
  }

  test("the string and SQL forms of a FILE literal") {
    val lit = Literal.create(FileTypeOps.fromFileRef(uriOnly), FileType.external)
    assert(lit.toString === """{"uri":"/data/a.txt"}""")
    assert(lit.sql === """FILE {"uri":"/data/a.txt"}""")
    assert(Literal.default(FileType()).toString === """{"uri":""}""")
    assert(Literal.create(null, FileType()).sql === "CAST(NULL AS FILE)")
  }

  test("FileRef equality, hash code and string representation") {
    val same = FileRefInternals.create(
      "s3://bucket/dir/doc.pdf", 10L, 2048L, "application/pdf", "MD5:abc123", Array[Byte](1, 2, 3))
    assert(allFields === same)
    assert(allFields.hashCode === same.hashCode)
    assert(allFields !== FileRefInternals.create(
      "s3://bucket/dir/doc.pdf", 10L, 2048L, "application/pdf", "MD5:abc123", Array[Byte](1, 2)))
    Seq(
      new FileRef("/data/b.txt", null, null, null, null),
      new FileRef("/data/a.txt", 0L, null, null, null),
      new FileRef("/data/a.txt", null, 0L, null, null),
      new FileRef("/data/a.txt", null, null, "text/plain", null),
      new FileRef("/data/a.txt", null, null, null, "MD5:abc"),
      FileRefInternals.create("/data/a.txt", null, null, null, null, Array.emptyByteArray))
      .foreach(other => assert(uriOnly !== other, other))
    assert(allFields.toString === "FileRef{uri='s3://bucket/dir/doc.pdf', offset=10, size=2048, " +
      "contentType='application/pdf', checksum='MD5:abc123', inline=<3 bytes>}")
    assert(new FileRef(null, null, null, null, null).toString === "FileRef{}")
  }

  // ==================== Encoders ====================

  test("encoders of FileRef, of a case class with FileRef fields and of a Row") {
    val enc = ExpressionEncoder[FileRef]().resolveAndBind()
    assert(enc.schema === new StructType().add("value", FileType(), nullable = true))
    values.foreach { f =>
      assert(enc.createDeserializer()(enc.createSerializer()(f)) === f)
    }

    val holderEnc = ExpressionEncoder[FileHolder]().resolveAndBind()
    assert(holderEnc.schema("file").dataType === FileType())
    assert(holderEnc.schema("files").dataType === ArrayType(FileType(), containsNull = true))
    Seq(FileHolder(1, allFields, Some(uriOnly), Seq(uriOnly, null)),
        FileHolder(2, null, None, Nil)).foreach { h =>
      assert(holderEnc.createDeserializer()(holderEnc.createSerializer()(h).copy()) === h)
    }

    val schema = new StructType().add("f", FileType.external).add("fs", ArrayType(FileType()))
    val rowEnc = ExpressionEncoder(RowEncoder.encoderFor(schema)).resolveAndBind()
    assert(rowEnc.schema === schema)
    val row = rowEnc.createDeserializer()(rowEnc.createSerializer()(Row(allFields, Seq(uriOnly))))
    assert(row.getFile(0) === allFields)
    assert(row.getSeq[FileRef](1) === Seq(uriOnly))
  }

  test("the FileRef encoder reads a FILE of any mode") {
    val enc = ExpressionEncoder[FileRef]()
    val external = new StructType().add("f", FileType.external)
    val bound = enc.resolveAndBind(DataTypeUtils.toAttributes(external))
    val row = InternalRow(FileTypeOps.fromFileRef(allFields))
    assert(bound.createDeserializer()(row) === allFields)
  }

  test("encoder builders for FileRefEncoder, also inside OptionEncoder") {
    Seq(FileTypeMode.UNKNOWN, FileTypeMode.EXTERNAL).foreach { mode =>
      assert(SerializerBuildHelper.createSerializer(FileRefEncoder(mode)).dataType ===
        FileType(mode))
      assert(DeserializerBuildHelper.createDeserializer(FileRefEncoder(mode)).dataType ===
        ObjectType(classOf[FileRef]))
    }
    val serializer = SerializerBuildHelper.createSerializer(OptionEncoder(FileRefEncoder()))
    assert(serializer.dataType === FileType())
    assert(serializer.nullable)
    assert(DeserializerBuildHelper.createDeserializer(OptionEncoder(FileRefEncoder())).dataType ===
      ObjectType(classOf[Option[_]]))
  }

  test("the FileRef encoder needs the FILE type to be enabled") {
    withSQLConf(SQLConf.FILE_TYPE_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException](ExpressionEncoder[FileRef]()),
        condition = "UNSUPPORTED_FILE_TYPE",
        parameters = Map.empty)
    }
  }

  // ==================== Casts ====================

  test("a FILE casts and up-casts to a FILE of unknown mode only") {
    Seq(FileType(), FileType.external).foreach { from =>
      assert(Cast.canCast(from, FileType()))
      assert(Cast.canAnsiCast(from, FileType()))
      assert(UpCastRule.canUpCast(from, FileType()))
    }
    assert(!Cast.canCast(FileType(), FileType.external))
    assert(!Cast.canAnsiCast(FileType(), FileType.external))
    assert(!UpCastRule.canUpCast(FileType(), FileType.external))
    assert(!UpCastRule.canUpCast(FileType.asStruct, FileType()))
    assert(!UpCastRule.canUpCast(FileType(), FileType.asStruct))
    // Writing to a table stores a FILE of any mode as FILE EXTERNAL, through a cast that only
    // table insertion makes.
    assert(DataTypeUtils.canWrite("t", FileType(), FileType.external, byName = true,
      SQLConf.get.resolver, "f", SQLConf.StoreAssignmentPolicy.STRICT, _ => ()))
    intercept[AnalysisException](DataTypeUtils.canWrite("t", FileType.external, FileType(),
      byName = true, SQLConf.get.resolver, "f", SQLConf.StoreAssignmentPolicy.STRICT, _ => ()))
    val nested = ArrayType(new StructType().add("f", FileType()))
    Seq(FileType() -> FileType.external,
        nested -> ArrayType(new StructType().add("f", FileType.external))).foreach {
      case (from, to) =>
        Seq(true, false).foreach { ansi =>
          val insertion = Cast(Literal.create(null, from), to, None, ansiEnabled = ansi)
          assert(insertion.checkInputDataTypes().isFailure, s"$from $ansi")
          insertion.setTagValue(Cast.BY_TABLE_INSERTION, ())
          assert(insertion.checkInputDataTypes().isSuccess, s"$from $ansi")
        }
    }
    // A cast to STRING, which is to read the referenced bytes, is not supported yet.
    Seq(FileType(), ArrayType(FileType()), new StructType().add("f", FileType())).foreach { dt =>
      assert(!Cast.canCast(dt, StringType), dt)
      assert(!Cast.canAnsiCast(dt, StringType), dt)
    }

    val input = Literal.create(fileRow("/c.txt", size = 1L), FileType.external)
    val cast = Cast(input, FileType())
    assert(cast.checkInputDataTypes().isSuccess)
    Seq(cast.eval(), GenerateSafeProjection.generate(Seq(cast)).apply(null).get(0, FileType()))
      .foreach { v =>
        assertFileRow(v.asInstanceOf[InternalRow], new FileRef("/c.txt", null, 1L, null, null))
      }
  }

  // ==================== Hashing ====================

  test("a non-null FILE value cannot be hashed, a null one keeps the seed") {
    val ops = FileTypeOps(FileType())
    val structType = new StructType().add("id", IntegerType).add("f", FileType())
    def hashes(child: Expression): Seq[HashExpression[_]] =
      Seq(Murmur3Hash(Seq(child), 42), XxHash64(Seq(child), 42L), HiveHash(Seq(child)))
    Seq[(DataType, Any)](
      FileType() -> ops.toCatalystImpl(uriOnly),
      structType -> InternalRow(1, ops.toCatalystImpl(uriOnly))).foreach { case (dt, value) =>
      val fileHashes = hashes(BoundReference(0, dt, nullable = true))
      val seedHashes = hashes(BoundReference(0, IntegerType, nullable = true))
      fileHashes.zip(seedHashes).foreach { case (hash, seedHash) =>
        val projection = GenerateMutableProjection.generate(Seq(hash))
        Seq[(String, InternalRow => Any)](
          "interpreted" -> (row => hash.eval(row)),
          "codegen" -> (row => projection(row).get(0, hash.dataType))
        ).foreach { case (mode, evaluate) =>
          withClue(s"$mode ${hash.prettyName} of $dt") {
            checkError(
              exception = intercept[SparkUnsupportedOperationException](
                evaluate(InternalRow(value))),
              condition = "UNSUPPORTED_DATATYPE",
              parameters = Map("typeName" -> "\"FILE\""))
            assert(evaluate(InternalRow(null)) === seedHash.eval(InternalRow(null)))
          }
        }
      }
    }
  }
}

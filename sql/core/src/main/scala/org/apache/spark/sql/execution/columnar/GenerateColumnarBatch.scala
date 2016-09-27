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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, OnHeapUnsafeColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._


/**
 * A helper class to expose the scala iterator to Java.
 */
abstract class ColumnarBatchIterator extends Iterator[ColumnarBatch]


/**
 * Generate code to batch [[InternalRow]]s into [[ColumnarBatch]]es.
 */
class GenerateColumnarBatch(
    schema: StructType,
    batchSize: Int,
    storageLevel: StorageLevel,
    conf: SparkConf)
  extends CodeGenerator[Iterator[InternalRow], Iterator[CachedColumnarBatch]] {

  protected def canonicalize(in: Iterator[InternalRow]): Iterator[InternalRow] = in

  protected def bind(
      in: Iterator[InternalRow],
      inputSchema: Seq[Attribute]): Iterator[InternalRow] = {
    in
  }

  protected def create(rowIterator: Iterator[InternalRow]): Iterator[CachedColumnarBatch] = {
    import scala.collection.JavaConverters._
    val ctx = newCodeGenContext()
    val columnStatsCls = classOf[ColumnStats].getName
    val rowVar = ctx.freshName("row")
    val batchVar = ctx.freshName("columnarBatch")
    val rowNumVar = ctx.freshName("rowNum")
    val numBytesVar = ctx.freshName("bytesInBatch")
    ctx.addMutableState("long", numBytesVar, s"$numBytesVar = 0;")
    val rowIterVar = ctx.addReferenceObj(
      "rowIterator", rowIterator.asJava, classOf[java.util.Iterator[_]].getName)
    val schemas = StructType(
      schema.fields.map(s => StructField(s.name,
        s.dataType match {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }, s.nullable))
    )
    val schemaVar = ctx.addReferenceObj("schema", schemas, classOf[StructType].getName)
    val maxNumBytes = ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE
    val numColumns = schema.fields.length

    val colStatVars = (0 to numColumns - 1).map(i => ctx.freshName("colStat" + i))
    val colStatCode = ctx.splitExpressions(
      "row",
      (schemas.fields zip colStatVars).zipWithIndex.map {
        case ((field, varName), i) =>
          val (columnStatsCls, arg) = field.dataType match {
            case BooleanType => (classOf[BooleanColumnStats].getName, "()")
            case ByteType => (classOf[ByteColumnStats].getName, "()")
            case ShortType => (classOf[ShortColumnStats].getName, "()")
            case IntegerType | DateType => (classOf[IntColumnStats].getName, "()")
            case LongType | TimestampType => (classOf[LongColumnStats].getName, "()")
            case FloatType => (classOf[FloatColumnStats].getName, "()")
            case DoubleType => (classOf[DoubleColumnStats].getName, "()")
            case StringType => (classOf[StringColumnStats].getName, "()")
            case BinaryType => (classOf[BinaryColumnStats].getName, "()")
            case dt: DecimalType =>
              (classOf[DecimalColumnStats].getName, s"(${dt.precision}, ${dt.scale})")
            case dt => (classOf[OtherColumnStats].getName, "()")
          }
        ctx.addMutableState(columnStatsCls, varName, "")
        s"$varName = new $columnStatsCls$arg;\n"
      }
    )
    val assignCollectedStatistics = ctx.splitExpressions(
      "row",
      colStatVars.zipWithIndex.map { case (name, i) =>
        s"assignStats(array, $name, $i);\n"
      },
      Seq(("Object[]", "array"))
    )
    val numColStats = colStatVars.length * 5

    val populateColumnVectorsCode = ctx.splitExpressions(
      rowVar,
      (schemas.fields zip colStatVars).zipWithIndex.map {
        case ((field, colStatVar), i) =>
          GenerateColumnarBatch.putColumnCode(ctx, field.dataType, field.nullable,
            batchVar, rowVar, rowNumVar, colStatVar, i, numBytesVar)
      },
      Seq(("ColumnarBatch", batchVar), ("int", rowNumVar))
    )

    val confVar = ctx.addReferenceObj("conf", conf, classOf[SparkConf].getName)
    val compress =
      s"""
       for (int i = 0; i < $numColumns; i++) {
         ((OnHeapUnsafeColumnVector)$batchVar.column(i)).compress($confVar);
       }
     """

    val code = s"""
      import org.apache.spark.memory.MemoryMode;
      import org.apache.spark.sql.catalyst.InternalRow;
      import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
      import org.apache.spark.sql.execution.vectorized.ColumnVector;
      import org.apache.spark.sql.execution.vectorized.OnHeapUnsafeColumnVector;

      public GeneratedColumnarBatchIterator generate(Object[] references) {
        return new GeneratedColumnarBatchIterator(references);
      }

      class GeneratedColumnarBatchIterator extends ${classOf[ColumnarBatchIterator].getName} {
        ${ctx.declareMutableStates()}

        public GeneratedColumnarBatchIterator(Object[] references) {
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        private void allocateColumnStats() {
          InternalRow row = null;
          $colStatCode
        }

        private void assignStats(Object[] array, $columnStatsCls stat, int i) {
          Object[] stats = stat.collectedStats();
          int idx = i * 5;
          array[idx] = stats[0];
          array[idx+1] = stats[1];
          array[idx+2] = stats[2];
          array[idx+3] = stats[3];
          array[idx+4] = stats[4];
        }

        private InternalRow allocateStats() {
          InternalRow row = null;
          Object[] array = new Object[$numColStats];
          $assignCollectedStatistics
          return new GenericInternalRow(array);
        }

        @Override
        public boolean hasNext() {
          return $rowIterVar.hasNext();
        }

        @Override
        public ${classOf[CachedColumnarBatch].getName} next() {
          ColumnarBatch $batchVar =
          ColumnarBatch.allocate($schemaVar, MemoryMode.ON_HEAP_UNSAFE, $batchSize);
          allocateColumnStats();
          int $rowNumVar = 0;
          $numBytesVar = 0;
          while ($rowIterVar.hasNext() && $rowNumVar < $batchSize && $numBytesVar < $maxNumBytes) {
            InternalRow $rowVar = (InternalRow) $rowIterVar.next();
            $populateColumnVectorsCode
            $rowNumVar += 1;
          }
          $batchVar.setNumRows($rowNumVar);
          $compress
          return ${classOf[CachedColumnarBatch].getName}.apply($batchVar, allocateStats());
        }
      }
      """
    val formattedCode = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(code, ctx.getPlaceHolderToComments()))
    CodeGenerator.compile(formattedCode).generate(ctx.references.toArray)
      .asInstanceOf[Iterator[CachedColumnarBatch]]
  }

}


private[sql] object GenerateColumnarBatch {

  def compressStorageLevel(storageLevel: StorageLevel): StorageLevel = {
    storageLevel match {
      case MEMORY_ONLY => MEMORY_ONLY_SER
      case MEMORY_ONLY_2 => MEMORY_ONLY_SER_2
      case MEMORY_AND_DISK => MEMORY_AND_DISK_SER
      case MEMORY_AND_DISK_2 => MEMORY_AND_DISK_SER_2
      case sl => sl
    }
  }

  def isCompress(storageLevel: StorageLevel) : Boolean = {
    (storageLevel == MEMORY_ONLY_SER || storageLevel == MEMORY_ONLY_SER_2 ||
     storageLevel == MEMORY_AND_DISK_SER || storageLevel == MEMORY_AND_DISK_SER_2)
  }

  private val typeToName = Map[AbstractDataType, String](
    BooleanType -> "boolean",
    ByteType -> "byte",
    ShortType -> "short",
    IntegerType -> "int",
    LongType -> "long",
    FloatType -> "float",
    DoubleType -> "double",
    DateType -> "int",
    TimestampType -> "long",
    StringType -> "UTF8String",
    BinaryType -> "Binary"
  )

  def putColumnCode(ctx: CodegenContext, dt: DataType, nullable: Boolean, batchVar: String,
      rowVar: String, rowNumVar: String, colStatVar: String, colNum: Int, numBytesVar: String)
      : String = {
    val colVar = s"$batchVar.column($colNum)"
    val body = dt match {
      case t if ctx.isPrimitiveType(dt) =>
        val typeName = GenerateColumnarBatch.typeToName(dt)
        val put = "put" + typeName.capitalize
        val get = "get" + typeName.capitalize
        s"""
         |$typeName val = $rowVar.$get($colNum);
         |$colVar.$put($rowNumVar, val);
         |$numBytesVar += ${dt.defaultSize};
         |$colStatVar.gatherValueStats(val);
       """.stripMargin
      case StringType | BinaryType =>
        val typeName = GenerateColumnarBatch.typeToName(dt)
        val typeDeclName = dt match {
          case StringType => "UTF8String"
          case BinaryType => "byte[]"
        }
        val put = "put" + typeName.capitalize
        val get = "get" + typeName.capitalize
        s"""
         |$typeDeclName val = $rowVar.$get($colNum);
         |int size = $colVar.$put($rowNumVar, val);
         |$numBytesVar += size;
         |$colStatVar.gatherValueStats(val, size);
       """.stripMargin
      case NullType =>
        return s"""
        |if ($rowVar.isNullAt($colNum)) {
        |  $colVar.putNull($rowNumVar);
        |} else {
        |  $colVar.putNotNull($rowNumVar);
        |}
        |$numBytesVar += 1;
        |$colStatVar.gatherValueStats(null, 1);
       """.stripMargin
      case dt: DecimalType =>
        val precision = dt.precision
        val scale = dt.scale
        s"""
         |Decimal val = $rowVar.getDecimal($colNum, $precision, $scale);
         |int size = $colVar.putDecimal($rowNumVar, val, $precision);
         |$numBytesVar += size;
         |$colStatVar.gatherValueStats(val, size);
       """.stripMargin
      case array: ArrayType =>
        s"""
         |ArrayData val = $rowVar.getArray($colNum);
         |int size = $colVar.putArray($rowNumVar, val);
         |$numBytesVar += size;
         |$colStatVar.gatherValueStats(val, size);
       """.stripMargin
      case t: MapType =>
        s"""
         |MapData val = $rowVar.getMap($colNum);
         |int size = $colVar.putMap($rowNumVar, val);
         |$numBytesVar += size;
         |$colStatVar.gatherValueStats(val, size);
       """.stripMargin
      case struct: StructType =>
        s"""
         |InternalRow val = $rowVar.getStruct($colNum, ${struct.length});
         |int size = $colVar.putStruct($rowNumVar,val);
         |$numBytesVar += size;
         |$colStatVar.gatherValueStats(val, size);
       """.stripMargin
      case _ =>
        throw new UnsupportedOperationException("Unsupported data type " + dt.simpleString);
    }
    if (nullable) {
      s"""
       |if ($rowVar.isNullAt($colNum)) {
       |  $colVar.putNull($rowNumVar);
       |  $colStatVar.gatherNullStats();
       |} else {
       |  $body
       |}
      """.stripMargin
    } else {
      s"""
       |{
       |  $body
       |}
      """.stripMargin
    }
  }
}

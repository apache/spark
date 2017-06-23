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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
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
    storageLevel: StorageLevel)
  extends CodeGenerator[Iterator[InternalRow], Iterator[CachedColumnarBatch]] {

  protected def canonicalize(in: Iterator[InternalRow]): Iterator[InternalRow] = in

  protected def bind(
    in: Iterator[InternalRow], inputSchema: Seq[Attribute]): Iterator[InternalRow] = {
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
      (schemas.fields zip colStatVars).zipWithIndex.map {
        case ((field, varName), i) =>
          val columnStatsCls = field.dataType match {
            case IntegerType => classOf[IntColumnStats].getName
            case DoubleType => classOf[DoubleColumnStats].getName
            case others => throw new UnsupportedOperationException(s"$others is not supported yet")
          }
          ctx.addMutableState(columnStatsCls, varName, "")
          s"$varName = new $columnStatsCls(); statsArray[$i] = $varName;\n"
      },
      "apply",
      Seq.empty
    )

    val populateColumnVectorsCode = ctx.splitExpressions(
      (schemas.fields zip colStatVars).zipWithIndex.map {
        case ((field, colStatVar), i) =>
          GenerateColumnarBatch.putColumnCode(ctx, field.dataType, field.nullable,
            batchVar, rowVar, rowNumVar, colStatVar, i, numBytesVar).trim + "\n"
      },
      "apply",
      Seq(("InternalRow", rowVar), ("ColumnarBatch", batchVar), ("int", rowNumVar))
    )

    val code = s"""
      import org.apache.spark.memory.MemoryMode;
      import org.apache.spark.sql.catalyst.InternalRow;
      import org.apache.spark.sql.execution.columnar.CachedColumnarBatch;
      import org.apache.spark.sql.execution.columnar.GenerateColumnarBatch;
      import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
      import org.apache.spark.sql.execution.vectorized.ColumnVector;

      public GeneratedColumnarBatchIterator generate(Object[] references) {
        return new GeneratedColumnarBatchIterator(references);
      }

      class GeneratedColumnarBatchIterator extends ${classOf[ColumnarBatchIterator].getName} {
        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedColumnarBatchIterator(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        $columnStatsCls[] statsArray = new $columnStatsCls[$numColumns];
        private void allocateColumnStats() {
          ${colStatCode.trim}
        }

        @Override
        public boolean hasNext() {
          return $rowIterVar.hasNext();
        }

        @Override
        public CachedColumnarBatch next() {
          ColumnarBatch $batchVar =
          ColumnarBatch.allocate($schemaVar, MemoryMode.ON_HEAP, $batchSize);
          allocateColumnStats();
          int $rowNumVar = 0;
          $numBytesVar = 0;
          while ($rowIterVar.hasNext() && $rowNumVar < $batchSize && $numBytesVar < $maxNumBytes) {
            InternalRow $rowVar = (InternalRow) $rowIterVar.next();
            $populateColumnVectorsCode
            $rowNumVar += 1;
          }
          $batchVar.setNumRows($rowNumVar);
          return CachedColumnarBatch.apply(
            $batchVar, GenerateColumnarBatch.generateStats(statsArray));
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
  def compressStorageLevel(storageLevel: StorageLevel, useCompression: Boolean): StorageLevel = {
    if (!useCompression) return storageLevel
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
    IntegerType -> "int",
    DoubleType -> "double"
  )

  def putColumnCode(ctx: CodegenContext, dt: DataType, nullable: Boolean, batchVar: String,
      rowVar: String, rowNumVar: String,
      colStatVar: String, colNum: Int, numBytesVar: String): String = {
    val colVar = s"$batchVar.column($colNum)"
    val body = dt match {
      case t if ctx.isPrimitiveType(dt) =>
        val typeName = GenerateColumnarBatch.typeToName(dt)
        val put = "put" + typeName.capitalize
        val get = "get" + typeName.capitalize
        s"""
         $typeName val = $rowVar.$get($colNum);
         $colVar.$put($rowNumVar, val);
         $numBytesVar += ${dt.defaultSize};
         $colStatVar.gatherValueStats(val);
       """
      case _ =>
        throw new UnsupportedOperationException("Unsupported data type " + dt.simpleString);
    }
    if (nullable) {
      s"""
       if ($rowVar.isNullAt($colNum)) {
         $colVar.putNull($rowNumVar);
         $colStatVar.gatherNullStats();
       } else {
         ${body.trim}
       }
      """
    } else {
      s"""
       {
         ${body.trim}
       }
      """
    }
  }

  def generateStats(columnStats: Array[ColumnStats]): InternalRow = {
    val array = columnStats.flatMap(_.collectedStatistics)
    InternalRow.fromSeq(array)
  }
}

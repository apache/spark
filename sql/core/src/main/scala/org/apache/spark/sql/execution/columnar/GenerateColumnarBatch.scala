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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel


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
  extends CodeGenerator[Iterator[InternalRow], Iterator[ColumnarBatch]] {

  protected def canonicalize(in: Iterator[InternalRow]): Iterator[InternalRow] = in

  protected def bind(
      in: Iterator[InternalRow],
      inputSchema: Seq[Attribute]): Iterator[InternalRow] = {
    in
  }

  protected def create(rowIterator: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    import scala.collection.JavaConverters._
    val ctx = newCodeGenContext()
    val batchVar = ctx.freshName("columnarBatch")
    val rowNumVar = ctx.freshName("rowNum")
    val numBytesVar = ctx.freshName("bytesInBatch")
    val rowIterVar = ctx.addReferenceObj(
      "rowIterator", rowIterator.asJava, classOf[java.util.Iterator[_]].getName)
    val schemaVar = ctx.addReferenceObj("schema", schema, classOf[StructType].getName)
    val maxNumBytes = ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE
    // Code to populate column vectors with the values of the input rows
    val colVars = schema.fields.indices.map(i => ctx.freshName("colInstance" + i))
    val columnInstanceCode = colVars.zipWithIndex.map { case (name, i) =>
      s"ColumnVector $name = $batchVar.column($i);"
    }.mkString("\n")

    val populateColumnVectorsCode = (schema.fields zip colVars).zipWithIndex.map {
      case ((field, colVar), i) =>
      GenerateColumnarBatch.putColumnCode(ctx, field.dataType, field.nullable,
        colVar, "row", rowNumVar, i, numBytesVar)
    }.mkString("")
    val code = s"""
      import org.apache.spark.memory.MemoryMode;
      import org.apache.spark.sql.catalyst.InternalRow;
      import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
      import org.apache.spark.sql.execution.vectorized.ColumnVector;

      public GeneratedColumnarBatchIterator generate(Object[] references) {
        return new GeneratedColumnarBatchIterator(references);
      }

      class GeneratedColumnarBatchIterator extends ${classOf[ColumnarBatchIterator].getName} {
        ${ctx.declareMutableStates()}

        public GeneratedColumnarBatchIterator(Object[] references) {
          ${ctx.initMutableStates()}
        }

        @Override
        public boolean hasNext() {
          return $rowIterVar.hasNext();
        }

        @Override
        public ColumnarBatch next() {
          ColumnarBatch $batchVar =
            ColumnarBatch.allocate($schemaVar, MemoryMode.ON_HEAP_UNSAFE, $batchSize);
          $columnInstanceCode
          int $rowNumVar = 0;
          long $numBytesVar = 0;
          while ($rowIterVar.hasNext() && $rowNumVar < $batchSize && $numBytesVar < $maxNumBytes) {
            InternalRow row = (InternalRow) $rowIterVar.next();
            $populateColumnVectorsCode
            $rowNumVar += 1;
          }
          $batchVar.setNumRows($rowNumVar);
          return $batchVar;
        }
      }
      """
    val formattedCode = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(code, ctx.getPlaceHolderToComments()))
    CodeGenerator.compile(formattedCode).generate(ctx.references.toArray)
      .asInstanceOf[Iterator[ColumnarBatch]]
  }

}


private[columnar] object GenerateColumnarBatch {

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

  def putColumnCode(ctx: CodegenContext, dt: DataType, nullable: Boolean, colVar: String,
      rowVar: String, rowNumVar: String, colNum: Int, numBytesVar: String) : String = {
    val body = dt match {
      case t if ctx.isPrimitiveType(dt) =>
        val typeName = GenerateColumnarBatch.typeToName(dt)
        val put = "put" + typeName.capitalize
        val get = "get" + typeName.capitalize
        s"""
         |$colVar.$put($rowNumVar, $rowVar.$get($colNum));
         |$numBytesVar += ${dt.defaultSize};
       """.stripMargin
      case StringType | BinaryType =>
        val typeName = GenerateColumnarBatch.typeToName(dt)
        val put = "put" + typeName.capitalize
        val get = "get" + typeName.capitalize
        s"""$numBytesVar += $colVar.$put($rowNumVar, $rowVar.$get($colNum));"""
      case NullType =>
        return s"""
        |if ($rowVar.isNullAt($colNum)) {
        |  $colVar.putNull($rowNumVar);
        |} else {
        |  $colVar.putNotNull($rowNumVar);
        |}
        |$numBytesVar += 1;
       """.stripMargin
      case dt: DecimalType =>
        val precision = dt.precision
        val scale = dt.scale
        s"""
         $numBytesVar += $colVar.putDecimal($rowNumVar,
           $rowVar.getDecimal($colNum, $precision, $scale), $precision);
       """.stripMargin
      case array: ArrayType =>
        s"""$numBytesVar += $colVar.putArray($rowNumVar, $rowVar.getArray($colNum));"""
      case t: MapType =>
        s"""$numBytesVar += $colVar.putMap($rowNumVar, $rowVar.getMap($colNum));"""
      case struct: StructType =>
        s"""
         $numBytesVar += $colVar.putStruct($rowNumVar,
           $rowVar.getStruct($colNum, ${struct.length}));
       """.stripMargin
      case _ =>
        throw new UnsupportedOperationException("Unsupported data type " + dt.simpleString);
    }
    if (nullable) {
      s"""
       |if ($rowVar.isNullAt($colNum)) {
       |  $colVar.putNull($rowNumVar);
       |} else {
       |  $body
       |}
      """.stripMargin
    } else {
      body
    }
  }
}

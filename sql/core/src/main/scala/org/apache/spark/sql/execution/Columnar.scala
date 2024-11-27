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

package org.apache.spark.sql.execution

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.V1WriteCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Holds a user defined rule that can be used to inject columnar implementations of various
 * operators in the plan. The [[preColumnarTransitions]] [[Rule]] can be used to replace
 * [[SparkPlan]] instances with versions that support a columnar implementation. After this
 * Spark will insert any transitions necessary. This includes transitions from row to columnar
 * [[RowToColumnarExec]] and from columnar to row [[ColumnarToRowExec]]. At this point the
 * [[postColumnarTransitions]] [[Rule]] is called to allow replacing any of the implementations
 * of the transitions or doing cleanup of the plan, like inserting stages to build larger batches
 * for more efficient processing, or stages that transition the data to/from an accelerator's
 * memory.
 */
class ColumnarRule {
  def preColumnarTransitions: Rule[SparkPlan] = plan => plan
  def postColumnarTransitions: Rule[SparkPlan] = plan => plan
}

/**
 * A trait that is used as a tag to indicate a transition from columns to rows. This allows plugins
 * to replace the current [[ColumnarToRowExec]] with an optimized version and still have operations
 * that walk a spark plan looking for this type of transition properly match it.
 */
trait ColumnarToRowTransition extends UnaryExecNode

/**
 * Provides a common executor to translate an [[RDD]] of [[ColumnarBatch]] into an [[RDD]] of
 * [[InternalRow]]. This is inserted whenever such a transition is determined to be needed.
 *
 * The implementation is based off of similar implementations in
 * [[org.apache.spark.sql.execution.python.ArrowEvalPythonExec]] and
 * [[MapPartitionsInRWithArrowExec]]. Eventually this should replace those implementations.
 */
case class ColumnarToRowExec(child: SparkPlan) extends ColumnarToRowTransition with CodegenSupport {
  // supportsColumnar requires to be only called on driver side, see also SPARK-37779.
  assert(Utils.isInRunningSparkTask || child.supportsColumnar)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // `ColumnarToRowExec` processes the input RDD directly, which is kind of a leaf node in the
  // codegen stage and needs to do the limit check.
  protected override def canCheckLimitNotReached: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches")
  )

  override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory = new ColumnarToRowEvaluatorFactory(
      child.output,
      longMetric("numOutputRows"),
      longMetric("numInputBatches"))
    if (conf.usePartitionEvaluator) {
      child.executeColumnar().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.executeColumnar().mapPartitionsWithIndexInternal { (index, batches) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, batches)
      }
    }
  }

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[ColumnVector]] in the batch.
   */
  private def genCodeColumnVector(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val value = CodeGenerator.getValueFromVector(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${CodeGenerator.defaultValue(dataType)} : ($value);
      """
    } else {
      code"$javaType $valueVar = $value;"
    })
    ExprCode(code, isNullVar, JavaCode.variable(valueVar, dataType))
  }

  /**
   * Produce code to process the input iterator as [[ColumnarBatch]]es.
   * This produces an [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] for each row in
   * each batch.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input",
      v => s"$v = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numInputBatches = metricTerm(ctx, "numInputBatches")

    val columnarBatchClz = classOf[ColumnarBatch].getName
    val batch = ctx.addMutableState(columnarBatchClz, "batch")

    val idx = ctx.addMutableState(CodeGenerator.JAVA_INT, "batchIdx") // init as batchIdx = 0
    val columnVectorClzs = child.vectorTypes.getOrElse(
      Seq.fill(output.indices.size)(classOf[ColumnVector].getName))
    val (colVars, columnAssigns) = columnVectorClzs.zipWithIndex.map {
      case (columnVectorClz, i) =>
        val name = ctx.addMutableState(columnVectorClz, s"colInstance$i")
        (name, s"$name = ($columnVectorClz) $batch.column($i);")
    }.unzip

    val nextBatch = ctx.freshName("nextBatch")
    val nextBatchFuncName = ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numInputBatches.add(1);
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    s"""
       |if ($batch == null) {
       |  $nextBatchFuncName();
       |}
       |while ($limitNotReachedCond $batch != null) {
       |  int $numRows = $batch.numRows();
       |  int $localEnd = $numRows - $idx;
       |  for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |    int $rowidx = $idx + $localIdx;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    $shouldStop
       |  }
       |  $idx = $numRows;
       |  $batch.closeIfNotWritable();
       |  $batch = null;
       |  $nextBatchFuncName();
       |}
       |// clean up resources
       |if ($batch != null) {
       |  $batch.close();
       |}
     """.stripMargin
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]]) // Hack because of type erasure
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarToRowExec =
    copy(child = newChild)
}

/**
 * Provides an optimized set of APIs to append row based data to an array of
 * [[WritableColumnVector]].
 */
private[execution] class RowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => RowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: InternalRow, vectors: Array[WritableColumnVector]): Unit = {
    var idx = 0
    while (idx < row.numFields) {
      converters(idx).append(row, idx, vectors(idx))
      idx += 1
    }
  }
}

/**
 * Provides an optimized set of APIs to extract a column from a row and append it to a
 * [[WritableColumnVector]].
 */
private object RowToColumnConverter {
  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit
  }

  private final case class BasicNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendNull
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private final case class StructNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendStruct(true)
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    val core = dataType match {
      case BinaryType => BinaryConverter
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType | DateType | _: YearMonthIntervalType => IntConverter
      case FloatType => FloatConverter
      case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType => LongConverter
      case DoubleType => DoubleConverter
      case StringType => StringConverter
      case CalendarIntervalType => CalendarConverter
      case VariantType => VariantConverter
      case at: ArrayType => ArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case st: StructType => new StructConverter(st.fields.map(
        (f) => getConverterForType(f.dataType, f.nullable)))
      case dt: DecimalType => new DecimalConverter(dt)
      case mt: MapType => MapConverter(getConverterForType(mt.keyType, nullable = false),
        getConverterForType(mt.valueType, mt.valueContainsNull))
      case unknown => throw ExecutionErrors.unsupportedDataTypeError(unknown)
    }

    if (nullable) {
      dataType match {
        case CalendarIntervalType | VariantType => new StructNullableTypeConverter(core)
        case st: StructType => new StructNullableTypeConverter(core)
        case _ => new BasicNullableTypeConverter(core)
      }
    } else {
      core
    }
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val bytes = row.getBinary(column)
      cv.appendByteArray(bytes, 0, bytes.length)
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendBoolean(row.getBoolean(column))
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendByte(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendShort(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendInt(row.getInt(column))
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendFloat(row.getFloat(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendLong(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendDouble(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val data = row.getUTF8String(column).getBytes
      cv.appendByteArray(data, 0, data.length)
    }
  }

  private object CalendarConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val c = row.getInterval(column)
      cv.appendStruct(false)
      cv.getChild(0).appendInt(c.months)
      cv.getChild(1).appendInt(c.days)
      cv.getChild(2).appendLong(c.microseconds)
    }
  }

  private object VariantConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val v = row.getVariant(column)
      cv.appendStruct(false)
      cv.getChild(0).appendByteArray(v.getValue, 0, v.getValue.length)
      cv.getChild(1).appendByteArray(v.getMetadata, 0, v.getMetadata.length)
    }
  }

  private case class ArrayConverter(childConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val values = row.getArray(column)
      val numElements = values.numElements()
      cv.appendArray(numElements)
      val arrData = cv.arrayData()
      for (i <- 0 until numElements) {
        childConverter.append(values, i, arrData)
      }
    }
  }

  private case class StructConverter(childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      cv.appendStruct(false)
      val data = row.getStruct(column, childConverters.length)
      for (i <- childConverters.indices) {
        childConverters(i).append(data, i, cv.getChild(i))
      }
    }
  }

  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val d = row.getDecimal(column, dt.precision, dt.scale)
      if (dt.precision <= Decimal.MAX_INT_DIGITS) {
        cv.appendInt(d.toUnscaledLong.toInt)
      } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
        cv.appendLong(d.toUnscaledLong)
      } else {
        val integer = d.toJavaBigDecimal.unscaledValue
        val bytes = integer.toByteArray
        cv.appendByteArray(bytes, 0, bytes.length)
      }
    }
  }

  private case class MapConverter(keyConverter: TypeConverter, valueConverter: TypeConverter)
    extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val m = row.getMap(column)
      val keys = cv.getChild(0)
      val values = cv.getChild(1)
      val numElements = m.numElements()
      cv.appendArray(numElements)

      val srcKeys = m.keyArray()
      val srcValues = m.valueArray()

      for (i <- 0 until numElements) {
        keyConverter.append(srcKeys, i, keys)
        valueConverter.append(srcValues, i, values)
      }
    }
  }
}

/**
 * A trait that is used as a tag to indicate a transition from rows to columns. This allows plugins
 * to replace the current [[RowToColumnarExec]] with an optimized version and still have operations
 * that walk a spark plan looking for this type of transition properly match it.
 */
trait RowToColumnarTransition extends UnaryExecNode

/**
 * Provides a common executor to translate an [[RDD]] of [[InternalRow]] into an [[RDD]] of
 * [[ColumnarBatch]]. This is inserted whenever such a transition is determined to be needed.
 *
 * This is similar to some of the code in ArrowConverters.scala and
 * [[org.apache.spark.sql.execution.arrow.ArrowWriter]]. That code is more specialized
 * to convert [[InternalRow]] to Arrow formatted data, but in the future if we make
 * [[OffHeapColumnVector]] internally Arrow formatted we may be able to replace much of that code.
 *
 * This is also similar to
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate()]] and
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.toBatch()]] toBatch is only ever
 * called from tests and can probably be removed, but populate is used by both Orc and Parquet
 * to initialize partition and missing columns. There is some chance that we could replace
 * populate with [[RowToColumnConverter]], but the performance requirements are different and it
 * would only be to reduce code.
 */
case class RowToColumnarExec(child: SparkPlan) extends RowToColumnarTransition {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches")
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val evaluatorFactory = new RowToColumnarEvaluatorFactory(
      conf.offHeapColumnVectorEnabled,
      // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
      // combine with some of the Arrow conversion tools we will need to unify some of the configs.
      conf.columnBatchSize,
      schema,
      longMetric("numInputRows"),
      longMetric("numOutputBatches"))
    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): RowToColumnarExec =
    copy(child = newChild)
}

/**
 * Apply any user defined [[ColumnarRule]]s and find the correct place to insert transitions
 * to/from columnar formatted data.
 *
 * @param columnarRules custom columnar rules
 * @param outputsColumnar whether or not the produced plan should output columnar format.
 */
case class ApplyColumnarRulesAndInsertTransitions(
    columnarRules: Seq[ColumnarRule],
    outputsColumnar: Boolean)
  extends Rule[SparkPlan] {

  /**
   * Inserts an transition to columnar formatted data.
   */
  private def insertRowToColumnar(plan: SparkPlan): SparkPlan = {
    if (!plan.supportsColumnar) {
      // The tree feels kind of backwards
      // Columnar Processing will start here, so transition from row to columnar
      RowToColumnarExec(insertTransitions(plan, outputsColumnar = false))
    } else if (!plan.isInstanceOf[RowToColumnarTransition]) {
      plan.withNewChildren(plan.children.map(insertRowToColumnar))
    } else {
      plan
    }
  }

  /**
   * Inserts RowToColumnarExecs and ColumnarToRowExecs where needed.
   */
  private def insertTransitions(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    if (outputsColumnar) {
      insertRowToColumnar(plan)
    } else if (plan.supportsColumnar && !plan.supportsRowBased) {
      // `outputsColumnar` is false but the plan only outputs columnar format, so add a
      // to-row transition here.
      ColumnarToRowExec(insertRowToColumnar(plan))
    } else if (plan.isInstanceOf[ColumnarToRowTransition]) {
      plan
    } else {
      val outputsColumnar = plan match {
        // With planned write, the write command invokes child plan's `executeWrite` which is
        // neither columnar nor row-based.
        case write: DataWritingCommandExec
            if write.cmd.isInstanceOf[V1WriteCommand] && conf.plannedWriteEnabled =>
          write.child.supportsColumnar
        case _ =>
          false
      }
      plan.withNewChildren(plan.children.map(insertTransitions(_, outputsColumnar)))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    var preInsertPlan: SparkPlan = plan
    columnarRules.foreach(r => preInsertPlan = r.preColumnarTransitions(preInsertPlan))
    var postInsertPlan = insertTransitions(preInsertPlan, outputsColumnar)
    columnarRules.reverse.foreach(r => postInsertPlan = r.postColumnarTransitions(postInsertPlan))
    postInsertPlan
  }
}

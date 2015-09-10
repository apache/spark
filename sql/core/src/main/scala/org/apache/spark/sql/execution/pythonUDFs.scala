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

import java.io.{BufferedInputStream, DataInputStream, DataOutputStream, BufferedOutputStream, OutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{List => JList, Map => JMap, Collections}

import org.apache.spark.api.python

import scala.collection.JavaConverters._

import net.razorvine.pickle._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging => SparkLogging, TaskContext, SparkEnv, Accumulator}

/**
 * A serialized version of a Python lambda function.  Suitable for use in a [[PythonRDD]].
 */
private[spark] case class PythonUDF(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[JList[Array[Byte]]],
    dataType: DataType,
    children: Seq[Expression]) extends Expression with Unevaluable with SparkLogging {

  override def toString: String = s"PythonUDF#$name(${children.mkString(",")})"

  override def nullable: Boolean = true
}

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
private[spark] object ExtractPythonUDFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // Skip EvaluatePython nodes.
    case plan: EvaluatePython => plan

    case plan: LogicalPlan if plan.resolved =>
      // Extract any PythonUDFs from the current operator.
      val udfs = plan.expressions.flatMap(_.collect { case udf: PythonUDF => udf })
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        plan
      } else {
        // Pick the UDF we are going to evaluate (TODO: Support evaluating multiple UDFs at a time)
        // If there is more than one, we will add another evaluation operator in a subsequent pass.
        udfs.find(_.resolved) match {
          case Some(udf) =>
            var evaluation: EvaluatePython = null

            // Rewrite the child that has the input required for the UDF
            val newChildren = plan.children.map { child =>
              // Check to make sure that the UDF can be evaluated with only the input of this child.
              // Other cases are disallowed as they are ambiguous or would require a cartesian
              // product.
              if (udf.references.subsetOf(child.outputSet)) {
                evaluation = EvaluatePython(udf, child)
                evaluation
              } else if (udf.references.intersect(child.outputSet).nonEmpty) {
                sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
              } else {
                child
              }
            }

            assert(evaluation != null, "Unable to evaluate PythonUDF.  Missing input attributes.")

            // Trim away the new UDF value if it was only used for filtering or something.
            logical.Project(
              plan.output,
              plan.transformExpressions {
                case p: PythonUDF if p.fastEquals(udf) => evaluation.resultAttribute
              }.withNewChildren(newChildren))

          case None =>
            // If there is no Python UDF that is resolved, skip this round.
            plan
        }
      }
  }
}

object EvaluatePython {
  def apply(udf: PythonUDF, child: LogicalPlan): EvaluatePython =
    new EvaluatePython(udf, child, AttributeReference("pythonUDF", udf.dataType)())

  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericInternalRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  /**
   * Converts `obj` to the type specified by the data type, or returns null if the type of obj is
   * unexpected. Because Python doesn't enforce the type.
   */
  def fromJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (c: Boolean, BooleanType) => c

    case (c: Int, ByteType) => c.toByte
    case (c: Long, ByteType) => c.toByte

    case (c: Int, ShortType) => c.toShort
    case (c: Long, ShortType) => c.toShort

    case (c: Int, IntegerType) => c
    case (c: Long, IntegerType) => c.toInt

    case (c: Int, LongType) => c.toLong
    case (c: Long, LongType) => c

    case (c: Double, FloatType) => c.toFloat

    case (c: Double, DoubleType) => c

    case (c: java.math.BigDecimal, dt: DecimalType) => Decimal(c, dt.precision, dt.scale)

    case (c: Int, DateType) => c

    case (c: Long, TimestampType) => c

    case (c: String, StringType) => UTF8String.fromString(c)
    case (c, StringType) =>
      // If we get here, c is not a string. Call toString on it.
      UTF8String.fromString(c.toString)

    case (c: String, BinaryType) => c.getBytes("utf-8")
    case (c, BinaryType) if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c

    case (c: java.util.List[_], ArrayType(elementType, _)) =>
      new GenericArrayData(c.asScala.map { e => fromJava(e, elementType)}.toArray)

    case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
      new GenericArrayData(c.asInstanceOf[Array[_]].map(e => fromJava(e, elementType)))

    case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) =>
      val keyValues = c.asScala.toSeq
      val keys = keyValues.map(kv => fromJava(kv._1, keyType)).toArray
      val values = keyValues.map(kv => fromJava(kv._2, valueType)).toArray
      ArrayBasedMapData(keys, values)

    case (c, StructType(fields)) if c.getClass.isArray =>
      new GenericInternalRow(c.asInstanceOf[Array[_]].zip(fields).map {
        case (e, f) => fromJava(e, f.dataType)
      })

    case (_, udt: UserDefinedType[_]) => fromJava(obj, udt.sqlType)

    // all other unexpected type should be null, or we will have runtime exception
    // TODO(davies): we could improve this by try to cast the object to expected type
    case (c, _) => null
  }


  private val module = "pyspark.sql.types"

  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((module + "\n" + "_parse_datatype_json_string" + "\n").getBytes("utf-8"))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  /**
   * Pickler for InternalRow
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericInternalRowWithSchema]

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + "_create_row_inbound_converter" + "\n").getBytes("utf-8"))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericInternalRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.size) {
          pickler.save(row.values(i))
          i += 1
        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

  private[this] var registered = false
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        SerDeUtil.initialize()
        new StructTypePickler().register()
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Evaluates a [[PythonUDF]], appending the result to the end of the input tuple.
 */
@DeveloperApi
case class EvaluatePython(
    udf: PythonUDF,
    child: LogicalPlan,
    resultAttribute: AttributeReference)
  extends logical.UnaryNode {

  def output: Seq[Attribute] = child.output :+ resultAttribute

  // References should not include the produced attribute.
  override def references: AttributeSet = udf.references
}

/**
 * :: DeveloperApi ::
 * Use a synchronous batched based system into order to calculate a [[PythonUDF]] without having to
 * risk deadlock.
 */
@DeveloperApi
case class BatchPythonEvaluation(udf: PythonUDF, output: Seq[Attribute], child: SparkPlan)
  extends SparkPlan {

  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val childResults = child.execute().map(_.copy())

    val bufferSize = childResults.context.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = childResults.context.conf.getBoolean("spark.python.worker.reuse", true)

    childResults.mapPartitionsWithIndex { case (partitionIndex, iter) =>
      val startTime = System.currentTimeMillis
      val env = SparkEnv.get

      val envVars = udf.envVars
      val localdir = env.blockManager.diskBlockManager.localDirs.map(
        f => f.getPath()).mkString(",")
      envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
      if (reuseWorker) {
        envVars.put("SPARK_REUSE_WORKER", "1")
      }

      val worker: Socket = env.createPythonWorker(udf.pythonExec, envVars.asScala.toMap)

      var released = new AtomicBoolean(false)

      TaskContext.get.addTaskCompletionListener { context =>
        if (!reuseWorker || !released.get()) {
          try {
            worker.close()
          } catch {
            case e: Exception =>
              logWarning("Failed to close worker socket", e)
          }
        }
      }

      val dataOut = new DataOutputStream(
        new BufferedOutputStream(worker.getOutputStream, bufferSize))
      val dataIn = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

      PythonRDD.writeHeaderToStream(
        dataOut,
        udf.pythonIncludes,
        udf.broadcastVars,
        worker,
        udf.pythonVer,
        partitionIndex, // partition number isn't used
        udf.command)

      // UDF mode
      dataOut.writeInt(PySparkMode.UDF)

      EvaluatePython.registerPicklers()

      val pickle = new Pickler
      val unpickle = new Unpickler

      val currentRow = newMutableProjection(udf.children, child.output)()
      val fields = udf.children.map(_.dataType)
      val schema = new StructType(fields.map(t => new StructField("", t, true)).toArray)

      val groupedIterator = iter.grouped(100)

      // Add a sentinel at the end so that we can know when to finish the pyspark protocol
      val groupedIteratorWithEnd = groupedIterator ++ Seq(BatchPythonEvaluation.SentinelEnd)

      groupedIteratorWithEnd.map { inputRows =>
        if (inputRows == BatchPythonEvaluation.SentinelEnd) {
          // Finish
          dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
          dataOut.writeInt(SpecialLengths.END_OF_STREAM)
          dataOut.flush()

          // This should return null and complete the cleanup procedure
          val shouldBeNull = PythonRDD.readPythonProcessSocket(
            dataIn,
            startTime,
            reuseWorker,
            udf.accumulator,
            udf.envVars,
            udf.pythonExec,
            worker,
            released)

          if (shouldBeNull != null) {
            throw new RuntimeException("Cleanup procedure failed")
          }

          // Return empty sequence, which will be flattened into nothing
          Seq()
        } else {
          val toBePickled = inputRows.map { row =>
            EvaluatePython.toJava(currentRow(row), schema)
          }.toArray
          val batchBytes = pickle.dumps(toBePickled)

          dataOut.writeInt(batchBytes.length)
          dataOut.write(batchBytes)
          dataOut.flush()

          val myObj = PythonRDD.readPythonProcessSocket(
            dataIn,
            startTime,
            reuseWorker,
            udf.accumulator,
            udf.envVars,
            udf.pythonExec,
            worker,
            released)

          val unpickledBatch = unpickle.loads(myObj)
          val unpickledBatchBuffer = unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala

          // TODO optimize performance

          val udfResults = unpickledBatchBuffer.map { result =>
            InternalRow.apply(EvaluatePython.fromJava(result, udf.dataType))
          }

          inputRows.zip(udfResults).map {
            case (row, udfResult) =>
              new JoinedRow(row, udfResult)
          }
        }
      }.flatten
    }
  }
}

private object BatchPythonEvaluation {
  val SentinelEnd = Seq(InternalRow("END"))
}

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

package org.apache.spark.sql.api.r

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.apache.spark.TaskContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.r.SerDe
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.CONFIG
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericRowWithSchema, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

private[sql] object SQLUtils extends Logging {
  SerDe.setSQLReadObject(readSqlObject).setSQLWriteObject(writeSqlObject)

  def getOrCreateSparkSession(
      jsc: JavaSparkContext,
      sparkConfigMap: JMap[Object, Object],
      enableHiveSupport: Boolean): SparkSession = {
    val spark =
      if (enableHiveSupport &&
          jsc.sc.conf.get(CATALOG_IMPLEMENTATION.key, "hive") == "hive" &&
          // Note that the order of conditions here are on purpose.
          // `SparkSession.hiveClassesArePresent` checks if Hive's `HiveConf` is loadable or not;
          // however, `HiveConf` itself has some static logic to check if Hadoop version is
          // supported or not, which throws an `IllegalArgumentException` if unsupported.
          // If this is checked first, there's no way to disable Hive support in the case above.
          // So, we intentionally check if Hive classes are loadable or not only when
          // Hive support is explicitly enabled by short-circuiting. See also SPARK-26422.
          SparkSession.hiveClassesArePresent) {
        SparkSession.builder().enableHiveSupport().sparkContext(jsc.sc).getOrCreate()
      } else {
        if (enableHiveSupport) {
          logWarning(log"SparkR: enableHiveSupport is requested for SparkSession but " +
            log"Spark is not built with Hive or ${MDC(CONFIG, CATALOG_IMPLEMENTATION.key)} " +
            log"is not set to 'hive', falling back to without Hive support.")
        }
        SparkSession.builder().sparkContext(jsc.sc).getOrCreate()
      }
    setSparkContextSessionConf(spark, sparkConfigMap)
    spark
  }

  def setSparkContextSessionConf(
      spark: SparkSession,
      sparkConfigMap: JMap[Object, Object]): Unit = {
    for ((name, value) <- sparkConfigMap.asScala) {
      spark.sessionState.conf.setConfString(name.toString, value.toString)
    }
    for ((name, value) <- sparkConfigMap.asScala) {
      spark.sparkContext.conf.set(name.toString, value.toString)
    }
  }

  def getSessionConf(spark: SparkSession): JMap[String, String] = {
    spark.conf.getAll.asJava
  }

  def getJavaSparkContext(spark: SparkSession): JavaSparkContext = {
    new JavaSparkContext(spark.sparkContext)
  }

  def createStructType(fields: Seq[StructField]): StructType = {
    StructType(fields)
  }

  // Support using regex in string interpolation
  private[this] implicit class RegexContext(sc: StringContext) {
    def r: Regex = new Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def createStructField(name: String, dataType: String, nullable: Boolean): StructField = {
    val dtObj = CatalystSqlParser.parseDataType(dataType)
    StructField(name, dtObj, nullable)
  }

  def createDF(rdd: RDD[Array[Byte]], schema: StructType, sparkSession: SparkSession): DataFrame = {
    val num = schema.fields.length
    val rowRDD = rdd.map(bytesToRow(_, schema))
    sparkSession.createDataFrame(rowRDD, schema)
  }

  def dfToRowRDD(df: DataFrame): JavaRDD[Array[Byte]] = {
    df.rdd.map(r => rowToRBytes(r))
  }

  private[this] def doConversion(data: Object, dataType: DataType): Object = {
    data match {
      case d: java.lang.Double if dataType == FloatType =>
        java.lang.Float.valueOf(d.toFloat)
      // Scala Map is the only allowed external type of map type in Row.
      case m: java.util.Map[_, _] => m.asScala
      case _ => data
    }
  }

  private[sql] def bytesToRow(bytes: Array[Byte], schema: StructType): Row = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)
    val num = SerDe.readInt(dis)
    Row.fromSeq((0 until num).map { i =>
      doConversion(SerDe.readObject(dis, jvmObjectTracker = null), schema.fields(i).dataType)
    })
  }

  private[sql] def rowToRBytes(row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val cols = (0 until row.length).map(row(_).asInstanceOf[Object]).toArray
    SerDe.writeObject(dos, cols, jvmObjectTracker = null)
    bos.toByteArray()
  }

  // Schema for DataFrame of serialized R data
  // TODO: introduce a user defined type for serialized R data.
  val SERIALIZED_R_DATA_SCHEMA = StructType(Array(StructField("R", BinaryType)))

  /**
   * The helper function for dapply() on R side.
   */
  def dapply(
      df: DataFrame,
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Object],
      schema: StructType): DataFrame = {
    val bv = broadcastVars.map(_.asInstanceOf[Broadcast[Object]])
    val realSchema = if (schema == null) SERIALIZED_R_DATA_SCHEMA else schema
    df.mapPartitionsInR(func, packageNames, bv, realSchema)
  }

  /**
   * The helper function for gapply() on R side.
   */
  def gapply(
      gd: RelationalGroupedDataset,
      func: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Object],
      schema: StructType): DataFrame = {
    val bv = broadcastVars.map(_.asInstanceOf[Broadcast[Object]])
    val realSchema = if (schema == null) SERIALIZED_R_DATA_SCHEMA else schema
    gd.flatMapGroupsInR(func, packageNames, bv, realSchema)
  }


  def dfToCols(df: DataFrame): Array[Array[Any]] = {
    val localDF: Array[Row] = df.collect()
    val numCols = df.columns.length
    val numRows = localDF.length

    val colArray = new Array[Array[Any]](numCols)
    for (colNo <- 0 until numCols) {
      colArray(colNo) = new Array[Any](numRows)
      for (rowNo <- 0 until numRows) {
        colArray(colNo)(rowNo) = localDF(rowNo)(colNo)
      }
    }
    colArray
  }

  def readSqlObject(dis: DataInputStream, dataType: Char): Object = {
    dataType match {
      case 's' =>
        // Read StructType for DataFrame
        val fields = SerDe.readList(dis, jvmObjectTracker = null)
        Row.fromSeq(fields.toImmutableArraySeq)
      case _ => null
    }
  }

  def writeSqlObject(dos: DataOutputStream, obj: Object): Boolean = {
    obj match {
      // Handle struct type in DataFrame
      case v: GenericRowWithSchema =>
        dos.writeByte('s')
        SerDe.writeObject(dos, v.schema.fieldNames, jvmObjectTracker = null)
        SerDe.writeObject(dos, v.values, jvmObjectTracker = null)
        true
      case _ =>
        false
    }
  }

  def getTableNames(sparkSession: SparkSession, databaseName: String): Array[String] = {
    val db = databaseName match {
      case _ if databaseName != null && databaseName.trim.nonEmpty =>
        databaseName
      case _ =>
        sparkSession.catalog.currentDatabase
    }
    sparkSession.catalog.listTables(db).collect().map(_.name)
  }

  def createArrayType(elementType: String): ArrayType = {
    ArrayType(ExprUtils.evalTypeExpr(Literal(elementType)), true)
  }

  /**
   * R callable function to read a file in Arrow stream format and create an `RDD`
   * using each serialized ArrowRecordBatch as a partition.
   */
  def readArrowStreamFromFile(
      sparkSession: SparkSession,
      filename: String): JavaRDD[Array[Byte]] = {
    // Parallelize the record batches to create an RDD
    val batches = ArrowConverters.readArrowStreamFromFile(filename).toImmutableArraySeq
    JavaRDD.fromRDD(sparkSession.sparkContext.parallelize(batches, batches.length))
  }

  /**
   * R callable function to create a `DataFrame` from a `JavaRDD` of serialized
   * ArrowRecordBatches.
   */
  def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schema: StructType,
      sparkSession: SparkSession): DataFrame = {
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val rdd = arrowBatchRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromBatchIterator(iter, schema, timeZoneId, true, context)
    }
    sparkSession.internalCreateDataFrame(rdd.setName("arrow"), schema)
  }
}

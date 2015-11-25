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

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.r.SerDe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, GroupedData, Row, SQLContext, SaveMode}

import scala.util.matching.Regex

private[r] object SQLUtils {
  SerDe.registerSqlSerDe((readSqlObject, writeSqlObject))

  def createSQLContext(jsc: JavaSparkContext): SQLContext = {
    new SQLContext(jsc)
  }

  def getJavaSparkContext(sqlCtx: SQLContext): JavaSparkContext = {
    new JavaSparkContext(sqlCtx.sparkContext)
  }

  def createStructType(fields : Seq[StructField]): StructType = {
    StructType(fields)
  }

  // Support using regex in string interpolation
  private[this] implicit class RegexContext(sc: StringContext) {
    def r: Regex = new Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def getSQLDataType(dataType: String): DataType = {
    dataType match {
      case "byte" => org.apache.spark.sql.types.ByteType
      case "integer" => org.apache.spark.sql.types.IntegerType
      case "float" => org.apache.spark.sql.types.FloatType
      case "double" => org.apache.spark.sql.types.DoubleType
      case "numeric" => org.apache.spark.sql.types.DoubleType
      case "character" => org.apache.spark.sql.types.StringType
      case "string" => org.apache.spark.sql.types.StringType
      case "binary" => org.apache.spark.sql.types.BinaryType
      case "raw" => org.apache.spark.sql.types.BinaryType
      case "logical" => org.apache.spark.sql.types.BooleanType
      case "boolean" => org.apache.spark.sql.types.BooleanType
      case "timestamp" => org.apache.spark.sql.types.TimestampType
      case "date" => org.apache.spark.sql.types.DateType
      case r"\Aarray<(.+)${elemType}>\Z" =>
        org.apache.spark.sql.types.ArrayType(getSQLDataType(elemType))
      case r"\Amap<(.+)${keyType},(.+)${valueType}>\Z" =>
        if (keyType != "string" && keyType != "character") {
          throw new IllegalArgumentException("Key type of a map must be string or character")
        }
        org.apache.spark.sql.types.MapType(getSQLDataType(keyType), getSQLDataType(valueType))
      case r"\Astruct<(.+)${fieldsStr}>\Z" =>
        if (fieldsStr(fieldsStr.length - 1) == ',') {
          throw new IllegalArgumentException(s"Invaid type $dataType")
        }
        val fields = fieldsStr.split(",")
        val structFields = fields.map { field =>
          field match {
            case r"\A(.+)${fieldName}:(.+)${fieldType}\Z" =>
              createStructField(fieldName, fieldType, true)

            case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
          }
        }
        createStructType(structFields)
      case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
    }
  }

  def createStructField(name: String, dataType: String, nullable: Boolean): StructField = {
    val dtObj = getSQLDataType(dataType)
    StructField(name, dtObj, nullable)
  }

  def createDF(rdd: RDD[Array[Byte]], schema: StructType, sqlContext: SQLContext): DataFrame = {
    val num = schema.fields.size
    val rowRDD = rdd.map(bytesToRow(_, schema))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def dfToRowRDD(df: DataFrame): JavaRDD[Array[Byte]] = {
    df.map(r => rowToRBytes(r))
  }

  private[this] def doConversion(data: Object, dataType: DataType): Object = {
    data match {
      case d: java.lang.Double if dataType == FloatType =>
        new java.lang.Float(d)
      case _ => data
    }
  }

  private[this] def bytesToRow(bytes: Array[Byte], schema: StructType): Row = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)
    val num = SerDe.readInt(dis)
    Row.fromSeq((0 until num).map { i =>
      doConversion(SerDe.readObject(dis), schema.fields(i).dataType)
    }.toSeq)
  }

  private[this] def rowToRBytes(row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val cols = (0 until row.length).map(row(_).asInstanceOf[Object]).toArray
    SerDe.writeObject(dos, cols)
    bos.toByteArray()
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

  def saveMode(mode: String): SaveMode = {
    mode match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "error" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }
  }

  def loadDF(
      sqlContext: SQLContext,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    sqlContext.read.format(source).options(options).load()
  }

  def loadDF(
      sqlContext: SQLContext,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    sqlContext.read.format(source).schema(schema).options(options).load()
  }

  def readSqlObject(dis: DataInputStream, dataType: Char): Object = {
    dataType match {
      case 's' =>
        // Read StructType for DataFrame
        val fields = SerDe.readList(dis).asInstanceOf[Array[Object]]
        Row.fromSeq(fields)
      case _ => null
    }
  }

  def writeSqlObject(dos: DataOutputStream, obj: Object): Boolean = {
    obj match {
      // Handle struct type in DataFrame
      case v: GenericRowWithSchema =>
        dos.writeByte('s')
        SerDe.writeObject(dos, v.schema.fieldNames)
        SerDe.writeObject(dos, v.values)
        true
      case _ =>
        false
    }
  }
}

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

package org.apache.spark.sql.hive.execution

import java.io.{BufferedReader, DataInputStream, DataOutputStream, EOFException, InputStreamReader}
import java.util.Properties

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.SerDeUtils
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.ql.exec.RecordReader
import org.apache.hadoop.hive.ql.exec.RecordWriter
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.parse.HiveParser
import org.apache.hadoop.hive.ql.plan.PlanUtils
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
private[hive]
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)(@transient sc: HiveContext)
  extends UnaryNode {

  override def otherCopyArgs: Seq[HiveContext] = sc :: Nil

  protected override def doExecute(): RDD[Row] = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val dataInputStream = new DataInputStream(inputStream)
 
      val outputTableDesc = ioschema.initOutputSerDe(output)
      val outputSerde = outputTableDesc.getDeserializerClass().newInstance()
      outputSerde.initialize(null, outputTableDesc.getProperties())
      val outputSoi: StructObjectInspector = ioschema.initOutputputSoi(outputSerde)

      val hConf = new HiveConf()

      val scriptOutputReader: RecordReader = ioschema.getRecordReader()
      scriptOutputReader.initialize(dataInputStream, hConf, outputTableDesc.getProperties())

      val iterator: Iterator[Row] = new Iterator[Row] with HiveInspectors {
        var cacheRow: Row = null
        var eof: Boolean = false
        val writable = scriptOutputReader.createRow()

        override def hasNext: Boolean = !eof

        def deserialize(): Row = {
          if (cacheRow != null) return cacheRow

          val mutableRow = new SpecificMutableRow(output.map(_.dataType))
          val bytes = scriptOutputReader.next(writable)

          if (bytes <= 0) {
            eof = true
            return null
          }

          val raw = outputSerde.deserialize(writable)
          val dataList = outputSoi.getStructFieldsDataAsList(raw)
          val fieldList = outputSoi.getAllStructFieldRefs()
            
          var i = 0
          dataList.foreach( element => {
            if (element == null) {
              mutableRow.setNullAt(i)
            } else {
              mutableRow(i) = unwrap(element, fieldList(i).getFieldObjectInspector)
            }
            i += 1
          })
          return mutableRow
        }

        override def next(): Row = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
 
          val ret = deserialize()
          if (!eof) {
            cacheRow = null
            cacheRow = deserialize()
          }
          ret
        }
      }

      val inputTableDesc = ioschema.initInputSerDe(input)
      val inputSerde: Serializer = inputTableDesc.getDeserializerClass()
        .newInstance().asInstanceOf[Serializer]
      inputSerde.initialize(hConf, inputTableDesc.getProperties())

      val inputSoi = ioschema.initInputSoi(input)

      val dataOutputStream = new DataOutputStream(outputStream)
      val outputProjection = new InterpretedProjection(input, child.output)

      val scriptOutWriter: RecordWriter = ioschema.getRecordWriter()
      scriptOutWriter.initialize(dataOutputStream, hConf)

      // Put the write(output to the pipeline) into a single thread
      // and keep the collector as remain in the main thread.
      // otherwise it will causes deadlock if the data size greater than
      // the pipeline / buffer capacity.
      new Thread(new Runnable() {
        override def run(): Unit = {
          iter
            .map(outputProjection)
            .foreach { row =>
            val writable = inputSerde.serialize(row.asInstanceOf[GenericRow].values, inputSoi)
            scriptOutWriter.write(prepareWritable(writable))
          }
          scriptOutWriter.close()
        }
      }).start()

      iterator
    }
  }
}

/**
 * The wrapper class of Hive input and output schema properties
 */
private[hive]
case class HiveScriptIOSchema (
    inputRowFormat: Seq[(String, String, String)],
    outputRowFormat: Seq[(String, String, String)],
    inputSerdeClass: String,
    outputSerdeClass: String,
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReader: String,
    recordWriter: String,
    schemaLess: Boolean) extends ScriptInputOutputSchema with HiveInspectors {

  @transient val hConf = new HiveConf()
  val defaultSerdeName: String = hConf.getVar(HiveConf.ConfVars.HIVESCRIPTSERDE)
  val defaultSerdeClass: Class[_ <: Deserializer] = Utils.classForName(defaultSerdeName)
    .asInstanceOf[Class[_ <: Deserializer]]

  val fieldSeparator: Int =
    if (HiveConf.getBoolVar(hConf, HiveConf.ConfVars.HIVESCRIPTESCAPE)) {
      Utilities.ctrlaCode
    } else {
      Utilities.tabCode
    }

  def initInputSerDe(input: Seq[Expression]): TableDesc = {
    val (columns, columnTypes) = parseAttrs(input)
    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")
    val inInfo: TableDesc =
      if (inputSerdeClass != "") {
        getTableDescFromSerDe(inputSerdeClass, columns, columnTypes, inputSerdeProps,
          inputRowFormat)
      } else {
        PlanUtils.getTableDesc(defaultSerdeClass, fieldSeparator.toString, columns.mkString(","),
          columnTypesNames, false, true)
      }
    inInfo
  }

  def initOutputSerDe(output: Seq[Attribute]): TableDesc = {
    val (columns, columnTypes) = parseAttrs(output)
    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")
    val outInfo: TableDesc =
      if (outputSerdeClass != "") {
        getTableDescFromSerDe(outputSerdeClass, columns, columnTypes, outputSerdeProps,
          outputRowFormat)
      } else {
        PlanUtils.getTableDesc(defaultSerdeClass, fieldSeparator.toString, columns.mkString(","),
          columnTypesNames, false)
      }
    outInfo
  }

  def parseAttrs(attrs: Seq[Expression]): (Seq[String], Seq[DataType]) = {
                                                
    val columns = attrs.map {
      case aref: AttributeReference => aref.name
      case e: NamedExpression => e.name
      case _ => null
    }
 
    val columnTypes = attrs.map {
      case aref: AttributeReference => aref.dataType
      case e: NamedExpression => e.dataType
      case _ =>  null
    }

    (columns, columnTypes)
  }

  def getTableDescFromSerDe(
      serdeClassName: String,
      columns: Seq[String],
      columnTypes: Seq[DataType],
      serdeProps: Seq[(String, String)],
      rowFormat: Seq[(String, String, String)]): TableDesc = {
    val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")
    if (serdeClassName != "") {
      val className = serdeClassName
      val serdeClass: Class[_ <: Deserializer] = Utils.classForName(className)
        .asInstanceOf[Class[_ <: Deserializer]]

      val tblDesc: TableDesc = PlanUtils.getTableDesc(serdeClass, Utilities.tabCode.toString,
        columns.mkString(","), columnTypesNames, false)
 
      var propsMap = serdeProps.map(kv => {
        (kv._1, kv._2)
      }).toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
      propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)
      propsMap.map(kv => tblDesc.getProperties().setProperty(kv._1, kv._2))

      return tblDesc
    } else if (rowFormat != Nil) {
      val tblDesc: TableDesc = PlanUtils.getDefaultTableDesc(Utilities.ctrlaCode.toString,
        columns.mkString(","), columnTypesNames, false)
      rowFormat.foreach { (tuple) =>
        tuple._1 match {
          case "TOK_TABLEROWFORMATFIELD" =>
            val fieldDelim: String = tuple._2
            tblDesc.getProperties().setProperty(serdeConstants.FIELD_DELIM, fieldDelim)
            tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_FORMAT,
              fieldDelim)
            if (tuple._3 != null)  {
              val fieldEscape: String = tuple._3
              tblDesc.getProperties().setProperty(serdeConstants.ESCAPE_CHAR, fieldEscape)
            }
          case "TOK_TABLEROWFORMATCOLLITEMS" =>
            tblDesc.getProperties().setProperty(serdeConstants.COLLECTION_DELIM, tuple._2)
          case "TOK_TABLEROWFORMATMAPKEYS" =>
            tblDesc.getProperties().setProperty(serdeConstants.MAPKEY_DELIM, tuple._2)
          case "TOK_TABLEROWFORMATLINES" =>
            val lineDelim: String = tuple._2
            tblDesc.getProperties().setProperty(serdeConstants.LINE_DELIM, lineDelim)
            if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
              sys.error("Lines terminated by non newline.")
            }
          case "TOK_TABLEROWFORMATNULL" =>
            val nullFormat: String = tuple._2
            tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT,
              nullFormat)
        }
      }
      return tblDesc
    }
    null
  }

  def getRecordReader(): RecordReader = {
    val hConf = new HiveConf()
    val readerName =
      if (recordReader == "") {
        hConf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER)
      } else {
        recordReader
      }
    Utils.classForName(readerName).newInstance.asInstanceOf[RecordReader]
  }

  def getRecordWriter(): RecordWriter = {
    val hConf = new HiveConf()
    val writerName =
      if (recordWriter == "") {
        hConf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDWRITER)
      } else {
        recordWriter
      }
    Utils.classForName(writerName).newInstance.asInstanceOf[RecordWriter]
  }
 
  def initInputSoi(input: Seq[Expression]): ObjectInspector = {
    val (columns, columnTypes) = parseAttrs(input)
    val fieldObjectInspectors = columnTypes.map(toInspector(_))
    ObjectInspectorFactory
      .getStandardStructObjectInspector(columns, fieldObjectInspectors)
      .asInstanceOf[ObjectInspector]
  }
 
  def initOutputputSoi(outputSerde: Deserializer): StructObjectInspector = {
    outputSerde.getObjectInspector().asInstanceOf[StructObjectInspector]
  }
}

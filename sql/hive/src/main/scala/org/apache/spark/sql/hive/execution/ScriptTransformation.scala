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

import java.io.{BufferedReader, InputStreamReader}
import java.io.{DataInputStream, DataOutputStream, EOFException}
import java.util.Properties

import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors}
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.util.Utils


/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
@DeveloperApi
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)(@transient sc: HiveContext)
  extends UnaryNode {

  override def otherCopyArgs = sc :: Nil

  def execute() = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))
 
      val (outputSerde, outputSoi) = ioschema.initOutputSerDe(output)

      val iterator: Iterator[Row] = new Iterator[Row] with HiveInspectors {
        var cacheRow: Row = null
        var curLine: String = null
        var eof: Boolean = false

        override def hasNext: Boolean = {
          if (outputSerde == null) {
            if (curLine == null) {
              curLine = reader.readLine()
              curLine != null
            } else {
              true
            }
          } else {
            !eof
          }
        }

        def deserialize(): Row = {
          if (cacheRow != null) return cacheRow

          val mutableRow = new SpecificMutableRow(output.map(_.dataType))
          try {
            val dataInputStream = new DataInputStream(inputStream)
            val writable = outputSerde.getSerializedClass().newInstance
            writable.readFields(dataInputStream)

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
          } catch {
            case e: EOFException =>
              eof = true
              return null
          }
        }

        override def next(): Row = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
 
          if (outputSerde == null) {
            val prevLine = curLine
            curLine = reader.readLine()
 
            if (!ioschema.schemaLess) {
              new GenericRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
                .asInstanceOf[Array[Any]])
            } else {
              new GenericRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
                .asInstanceOf[Array[Any]])
            }
          } else {
            val ret = deserialize()
            if (!eof) {
              cacheRow = null
              cacheRow = deserialize()
            }
            ret
          }
        }
      }

      val (inputSerde, inputSoi) = ioschema.initInputSerDe(input)
      val dataOutputStream = new DataOutputStream(outputStream)
      val outputProjection = new InterpretedProjection(input, child.output)

      iter
        .map(outputProjection)
        .foreach { row =>
          if (inputSerde == null) {
            val data = row.mkString("", ioschema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD"),
            ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")).getBytes("utf-8")
 
            outputStream.write(data)
          } else {
            val writable = inputSerde.serialize(row.asInstanceOf[GenericRow].values, inputSoi)
            prepareWritable(writable).write(dataOutputStream)
          }
        }
      outputStream.close()
      iterator
    }
  }
}

/**
 * The wrapper class of Hive input and output schema properties
 */
case class HiveScriptIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: String,
    outputSerdeClass: String,
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    schemaLess: Boolean) extends ScriptInputOutputSchema with HiveInspectors {

  val defaultFormat = Map(("TOK_TABLEROWFORMATFIELD", "\t"),
                          ("TOK_TABLEROWFORMATLINES", "\n"))

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))

  
  def initInputSerDe(input: Seq[Expression]): (AbstractSerDe, ObjectInspector) = {
    val (columns, columnTypes) = parseAttrs(input)
    val serde = initSerDe(inputSerdeClass, columns, columnTypes, inputSerdeProps)
    (serde, initInputSoi(serde, columns, columnTypes))
  }

  def initOutputSerDe(output: Seq[Attribute]): (AbstractSerDe, StructObjectInspector) = {
    val (columns, columnTypes) = parseAttrs(output)
    val serde = initSerDe(outputSerdeClass, columns, columnTypes, outputSerdeProps)
    (serde, initOutputputSoi(serde))
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
 
  def initSerDe(serdeClassName: String, columns: Seq[String],
    columnTypes: Seq[DataType], serdeProps: Seq[(String, String)]): AbstractSerDe = {

    val serde: AbstractSerDe = if (serdeClassName != "") {
      val trimed_class = serdeClassName.split("'")(1)
      Utils.classForName(trimed_class)
        .newInstance.asInstanceOf[AbstractSerDe]
    } else {
      null
    }

    if (serde != null) {
      val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")

      var propsMap = serdeProps.map(kv => {
        (kv._1.split("'")(1), kv._2.split("'")(1))
      }).toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
      propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)
    
      val properties = new Properties()
      properties.putAll(propsMap)
      serde.initialize(null, properties)
    }

    serde
  }

  def initInputSoi(inputSerde: AbstractSerDe, columns: Seq[String], columnTypes: Seq[DataType])
    : ObjectInspector = {

    if (inputSerde != null) {
      val fieldObjectInspectors = columnTypes.map(toInspector(_))
      ObjectInspectorFactory
        .getStandardStructObjectInspector(columns, fieldObjectInspectors)
        .asInstanceOf[ObjectInspector]
    } else {
      null
    }
  }
 
  def initOutputputSoi(outputSerde: AbstractSerDe): StructObjectInspector = {
    if (outputSerde != null) {
      outputSerde.getObjectInspector().asInstanceOf[StructObjectInspector]
    } else {
      null
    }
  }
}

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
import java.rmi.server.UID

import scala.reflect.runtime.universe.{runtimeMirror, newTermName}

import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable
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
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors, HiveShim}
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
    ioschema: ScriptInputOutputSchema)(@transient sc: HiveContext)
  extends UnaryNode with HiveInspectors {

  override def otherCopyArgs = sc :: Nil

  val defaultFormat = Map(("TOK_TABLEROWFORMATFIELD", "\t"),
                          ("TOK_TABLEROWFORMATLINES", "\n"))

  val inputRowFormatMap = ioschema.inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = ioschema.outputRowFormat.toMap.withDefault((k) => defaultFormat(k))

  def execute() = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))
 
      val outputSerde: AbstractSerDe = if (ioschema.outputSerdeClass != "") {
        val trimed_class = ioschema.outputSerdeClass.split("'")(1) 
        Utils.classForName(trimed_class)
          .newInstance.asInstanceOf[AbstractSerDe]
      } else {
        null
      }
 
      if (outputSerde != null) {
        val columns = output.map { case aref: AttributeReference => aref.name }
          .mkString(",")
        val columnTypes = output.map { case aref: AttributeReference =>
          aref.dataType.toTypeInfo.getTypeName()
        }.mkString(",")

        var propsMap = ioschema.outputSerdeProps.map(kv => {
          (kv._1.split("'")(1), kv._2.split("'")(1))
        }).toMap + (serdeConstants.LIST_COLUMNS -> columns)
        propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypes)

        val properties = new Properties()
        properties.putAll(propsMap)
 
        outputSerde.initialize(null, properties)
      }

      val outputSoi = if (outputSerde != null) {
        outputSerde.getObjectInspector().asInstanceOf[StructObjectInspector]
      } else {
        null
      }

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
                prevLine.split(outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
                .asInstanceOf[Array[Any]])
            } else {
              new GenericRow(
                prevLine.split(outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
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

      val inputSerde: AbstractSerDe = if (ioschema.inputSerdeClass != "") {
        val trimed_class = ioschema.inputSerdeClass.split("'")(1)
        Utils.classForName(trimed_class)
          .newInstance.asInstanceOf[AbstractSerDe]
      } else {
        null
      }
 
      if (inputSerde != null) {
        val columns = input.map { case e: NamedExpression => e.name }.mkString(",")
        val columnTypes = input.map { case e: NamedExpression =>
          e.dataType.toTypeInfo.getTypeName() 
        }.mkString(",")
 
        var propsMap = ioschema.inputSerdeProps.map(kv => {
          (kv._1.split("'")(1), kv._2.split("'")(1))
        }).toMap + (serdeConstants.LIST_COLUMNS -> columns)
        propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypes)

        val properties = new Properties()
        properties.putAll(propsMap)
        inputSerde.initialize(null, properties)
      }
      
      val inputSoi = if (inputSerde != null) {
        val fieldNames = input.map { case e: NamedExpression => e.name }
        val fieldObjectInspectors = input.map { case e: NamedExpression =>
          toInspector(e.dataType)
        }
        ObjectInspectorFactory
          .getStandardStructObjectInspector(fieldNames, fieldObjectInspectors)
          .asInstanceOf[ObjectInspector]
      } else {
        null
      }

      val dataOutputStream = new DataOutputStream(outputStream)
      val outputProjection = new InterpretedProjection(input, child.output)
      iter
        .map(outputProjection)
        .foreach { row =>
          if (inputSerde == null) {
            val data = row.mkString("", inputRowFormatMap("TOK_TABLEROWFORMATFIELD"),
            inputRowFormatMap("TOK_TABLEROWFORMATLINES")).getBytes("utf-8")
 
            outputStream.write(data)
          } else {
            val writable = inputSerde.serialize(row.asInstanceOf[GenericRow].values, inputSoi)
            if (writable.isInstanceOf[AvroGenericRecordWritable] && HiveShim.version == "0.13.1") {
              // setRecordReaderID only in 0.13.1, so use reflection API to call it
              val ref = runtimeMirror(getClass.getClassLoader).reflect(
                writable.asInstanceOf[AvroGenericRecordWritable])
              val method = ref.symbol.typeSignature.member(newTermName("setRecordReaderID"))
              ref.reflectMethod(method.asMethod)(new UID())
            }
            writable.write(dataOutputStream)
          }
        }
      outputStream.close()
      iterator
    }
  }
}

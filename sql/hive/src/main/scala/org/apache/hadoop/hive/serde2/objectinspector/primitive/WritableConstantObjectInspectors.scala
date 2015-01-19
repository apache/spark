/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.objectinspector.primitive

import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.{io => hadoopIo}
import org.apache.spark.sql.hive.HiveShim

/**
 * These methods have to be put in this package because we use package-private constructors of
 * writable constant object inspectors here to avoid adding them to the Hive shim layer.
 */
object WritableConstantObjectInspectors {
  def getStringWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantStringObjectInspector(getStringWritable(value))

  def getIntWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantIntObjectInspector(getIntWritable(value))

  def getDoubleWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantDoubleObjectInspector(getDoubleWritable(value))

  def getBooleanWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantBooleanObjectInspector(getBooleanWritable(value))

  def getLongWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantLongObjectInspector(getLongWritable(value))

  def getFloatWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantFloatObjectInspector(getFloatWritable(value))

  def getShortWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantShortObjectInspector(getShortWritable(value))

  def getByteWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantByteObjectInspector(getByteWritable(value))

  def getBinaryWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantBinaryObjectInspector(getBinaryWritable(value))

  def getDateWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantDateObjectInspector(getDateWritable(value))

  def getTimestampWritableConstantObjectInspector(value: Any): ObjectInspector =
    new WritableConstantTimestampObjectInspector(getTimestampWritable(value))

  def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    HiveShim.getDecimalWritableConstantObjectInspector(value)

  def getPrimitiveNullWritableConstantObjectInspector: ObjectInspector =
    new WritableVoidObjectInspector()

  def getStringWritable(value: Any): hadoopIo.Text =
    if (value == null) null else new hadoopIo.Text(value.asInstanceOf[String])

  def getIntWritable(value: Any): hadoopIo.IntWritable =
    if (value == null) null else new hadoopIo.IntWritable(value.asInstanceOf[Int])

  def getDoubleWritable(value: Any): hiveIo.DoubleWritable =
    if (value == null) null else new hiveIo.DoubleWritable(value.asInstanceOf[Double])

  def getBooleanWritable(value: Any): hadoopIo.BooleanWritable =
    if (value == null) null else new hadoopIo.BooleanWritable(value.asInstanceOf[Boolean])

  def getLongWritable(value: Any): hadoopIo.LongWritable =
    if (value == null) null else new hadoopIo.LongWritable(value.asInstanceOf[Long])

  def getFloatWritable(value: Any): hadoopIo.FloatWritable =
    if (value == null) null else new hadoopIo.FloatWritable(value.asInstanceOf[Float])

  def getShortWritable(value: Any): hiveIo.ShortWritable =
    if (value == null) null else new hiveIo.ShortWritable(value.asInstanceOf[Short])

  def getByteWritable(value: Any): hiveIo.ByteWritable =
    if (value == null) null else new hiveIo.ByteWritable(value.asInstanceOf[Byte])

  def getBinaryWritable(value: Any): hadoopIo.BytesWritable =
    if (value == null) null else new hadoopIo.BytesWritable(value.asInstanceOf[Array[Byte]])

  def getDateWritable(value: Any): hiveIo.DateWritable =
    if (value == null) null else new hiveIo.DateWritable(value.asInstanceOf[java.sql.Date])

  def getTimestampWritable(value: Any): hiveIo.TimestampWritable =
    if (value == null) null
    else new hiveIo.TimestampWritable(value.asInstanceOf[java.sql.Timestamp])

  def getDecimalWritable(value: Any): hiveIo.HiveDecimalWritable =
    HiveShim.getDecimalWritable(value)

  def getPrimitiveNullWritable: NullWritable = NullWritable.get()
}

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


package org.apache.spark.examples.pythonconverters

import org.apache.spark.api.python.Converter
import collection.JavaConversions._

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.Schema.Field
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

/* 
  Example usage in pyspark:  

    avroRdd = sc.newAPIHadoopFile("/tmp/data.avro", 
      "org.apache.avro.mapreduce.AvroKeyInputFormat", 
      "org.apache.avro.mapred.AvroKey", 
      "org.apache.hadoop.io.NullWritable",
      keyConverter="org.apache.spark.examples.pythonconverters.AvroGenericConverter")
*/
class AvroGenericConverter extends Converter[AvroKey[GenericRecord], java.util.Map[String, Any]] {
  override def convert(obj: AvroKey[GenericRecord]): java.util.Map[String, Any] = {

    def unpackRecord(record: GenericRecord): java.util.Map[String,Any] = {
      mapAsJavaMap(record.getSchema.getFields.map( f => (f.name, unpack(record.get(f.name), f.schema) ) ).toMap)
    }
    
    def unpackArray(value: Any, schema: Schema): java.util.List[Any] = {
      bufferAsJavaList(value.asInstanceOf[java.util.List[Any]].map( v => unpack(v, schema)))
    }

    def unpackUnion(value: Any): Any = value match {   
      case v:java.lang.Double => value.asInstanceOf[java.lang.Double]
      case v:java.lang.Float => value.asInstanceOf[java.lang.Float]  
      case v:java.lang.Integer => value.asInstanceOf[java.lang.Integer] 
      case v:java.lang.Long => value.asInstanceOf[java.lang.Long] 
      case v:java.lang.String => value.asInstanceOf[java.lang.String] 
      case _ => ""
    }

    def unpack(value: Any, schema: Schema): Any = schema.getType match {
      case ARRAY => unpackArray(value, schema.getElementType)       
      // case BOOLEAN            
      // case BYTES            
      case DOUBLE => value.asInstanceOf[java.lang.Double]
      case ENUM => value.toString
      // case FIXED           
      case FLOAT => value.asInstanceOf[java.lang.Float]
      case INT => value.asInstanceOf[java.lang.Integer]
      case LONG => value.asInstanceOf[java.lang.Long]
      // case MAP
      case RECORD => unpackRecord(value.asInstanceOf[GenericRecord])       
      case STRING => value.asInstanceOf[java.lang.String]     
      case UNION => unpackUnion(value)
      case _ => value.toString
    }  

    unpackRecord(obj.datum())
  }
}


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
package org.apache.spark.shuffle.parquet.avro

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificData.SchemaConstructable

/**
 * Helper class for wrapping two Avro objects inside a key-value object
 */
object AvroPair {
  private val PAIR: String = classOf[AvroPair[_, _]].getName
  private val KEY: String = "key"
  private val VALUE: String = "value"
  private val NULL_SCHEMA = Schema.create(Schema.Type.NULL)

  def checkIsPairSchema(schema: Schema): Boolean = PAIR == schema.getFullName

  /**
   * Creates a pair schema with the key and value fields being optional to
   * support null values
   * @param keySchema The Avro schema for the key
   * @param valueSchema The Avro schema for the value
   * @return The combined pair schema
   */
  def makePairSchema(keySchema: Schema, valueSchema: Schema): Schema = {
    val pair: Schema = Schema.createRecord(PAIR, null, null, false)
    pair.setFields(List(
      new Schema.Field(KEY, Schema.createUnion(List(NULL_SCHEMA, keySchema).asJava), "", null),
      new Schema.Field(VALUE, Schema.createUnion(List(NULL_SCHEMA, keySchema).asJava), "", null,
        Field.Order.IGNORE)).asJava)
    pair
  }
}

class AvroPair[K, V](var _1: K, var _2: V, schema: Schema)
  extends IndexedRecord with Product2[K, V] with SchemaConstructable {
  assert(AvroPair.checkIsPairSchema(schema),
    "AvroPair can only be created with a pair schema")

  // ctor for SchemaConstructable
  def this(schema: Schema) = this(null.asInstanceOf[K], null.asInstanceOf[V], schema)

  def update(key: K, value: V): AvroPair[K, V] = {
    this._1 = key
    this._2 = value
    this
  }

  override def get(i: Int): AnyRef = i match {
    case 0 => _1.asInstanceOf[AnyRef]
    case 1 => _2.asInstanceOf[AnyRef]
    case _ => new IndexOutOfBoundsException(i.toString)
  }

  override def put(i: Int, v: scala.Any): Unit = i match {
    case 0 => _1 = v.asInstanceOf[K]
    case 1 => _2 = v.asInstanceOf[V]
    case _ => new IndexOutOfBoundsException(i.toString)
  }

  override def getSchema: Schema = schema

  override def canEqual(that: Any): Boolean = that.isInstanceOf[AvroPair[_, _]]
}

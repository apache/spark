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

package org.apache.spark.sql.catalyst.joins

import java.util.{HashMap => JavaHashMap}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Projection, Row}
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.util.collection.CompactBuffer


/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[sql] sealed trait HashedRelation {
  def get(key: Row): CompactBuffer[Row]
}


/**
 * A general [[HashedRelation]] backed by a hash map that maps the key into a sequence of values.
 */
private[joins] final class GeneralHashedRelation(hashTable: JavaHashMap[Row, CompactBuffer[Row]])
  extends HashedRelation with Serializable {

  override def get(key: Row) = hashTable.get(key)
}


/**
 * A specialized [[HashedRelation]] that maps key into a single value. This implementation
 * assumes the key is unique.
 */
private[joins] final class UniqueKeyHashedRelation(hashTable: JavaHashMap[Row, Row])
  extends HashedRelation with Serializable {

  override def get(key: Row) = {
    val v = hashTable.get(key)
    if (v eq null) null else CompactBuffer(v)
  }

  def getValue(key: Row): Row = hashTable.get(key)
}

/**
 * A specialized [[HashedRelation]] that maps key into a single value. This implementation
 * assumes the key is unique.
 */
private[sql] final class UniqueIntKeyHashedRelation(
    hashTable: JavaHashMap[Int, Row])
  extends HashedRelation with Serializable {

  def get(key: Int): Row = {
    hashTable.get(key)
  }

  override def get(key: Row): CompactBuffer[Row] = {
    if (key.isNullAt(0)) return null
    val v = hashTable.get(key.getInt(0))
    if (v eq null) null else CompactBuffer(v)
  }

  def getValue(key: Row): Row = hashTable.get(key)
}

// TODO(rxin): a version of [[HashedRelation]] backed by arrays for consecutive integer keys.


private[sql] object HashedRelation {

  def apply(
      inputSchema: Seq[Attribute],
      input: Iterator[Row],
      buildKeys: Seq[Expression],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    // TODO: Use Spark's HashMap implementation.
    val hashTable = new JavaHashMap[Row, CompactBuffer[Row]](sizeEstimate)
    var currentRow: Row = null

    // Whether the join key is unique. If the key is unique, we can convert the underlying
    // hash map into one specialized for this.
    var keyIsUnique = true

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      currentRow = input.next()
      val rowKey = keyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[Row]()
          hashTable.put(rowKey, newMatchList)
          newMatchList
        } else {
          keyIsUnique = false
          existingMatchList
        }
        matchList += currentRow.copy()
      }
    }

    if (keyIsUnique && buildKeys.size == 1 && buildKeys.head.dataType == IntegerType) {
      val uniqHashTable = new JavaHashMap[Int, Row](hashTable.size)
      val iter = hashTable.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        uniqHashTable.put(entry.getKey.getInt(0), entry.getValue()(0))
      }
      new UniqueIntKeyHashedRelation(uniqHashTable)
    } else if (keyIsUnique) {
      val uniqHashTable = new JavaHashMap[Row, Row](hashTable.size)
      val iter = hashTable.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        uniqHashTable.put(entry.getKey, entry.getValue()(0))
      }
      new UniqueKeyHashedRelation(uniqHashTable)
    } else {
      new GeneralHashedRelation(hashTable)
    }
  }
}

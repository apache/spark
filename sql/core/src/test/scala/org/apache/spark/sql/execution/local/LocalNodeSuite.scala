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

package org.apache.spark.sql.execution.local

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.IntegerType

class LocalNodeSuite extends SparkFunSuite {
  private val data = (1 to 100).toArray

  test("basic open, next, fetch, close") {
    val node = new DummyLocalNode(data)
    assert(!node.isOpen)
    node.open()
    assert(node.isOpen)
    data.foreach { i =>
      assert(node.next())
      // fetch should be idempotent
      val fetched = node.fetch()
      assert(node.fetch() === fetched)
      assert(node.fetch() === fetched)
      assert(node.fetch().numFields === 1)
      assert(node.fetch().getInt(0) === i)
    }
    assert(!node.next())
    node.close()
    assert(!node.isOpen)
  }

  test("asIterator") {
    val node = new DummyLocalNode(data)
    val iter = node.asIterator
    node.open()
    data.foreach { i =>
      // hasNext should be idempotent
      assert(iter.hasNext)
      assert(iter.hasNext)
      val item = iter.next()
      assert(item.numFields === 1)
      assert(item.getInt(0) === i)
    }
    intercept[NoSuchElementException] {
      iter.next()
    }
    node.close()
  }

  test("collect") {
    val node = new DummyLocalNode(data)
    node.open()
    val collected = node.collect()
    assert(collected.size === data.size)
    assert(collected.forall(_.size === 1))
    assert(collected.map(_.getInt(0)) === data)
    node.close()
  }

}

/**
 * A dummy [[LocalNode]] that just returns one row per integer in the input.
 */
private case class DummyLocalNode(conf: SQLConf, input: Array[Int]) extends LocalNode(conf) {
  private var index = Int.MinValue

  def this(input: Array[Int]) {
    this(new SQLConf, input)
  }

  def isOpen: Boolean = {
    index != Int.MinValue
  }

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("something", IntegerType)())
  }

  override def children: Seq[LocalNode] = Seq.empty

  override def open(): Unit = {
    index = -1
  }

  override def next(): Boolean = {
    index += 1
    index < input.size
  }

  override def fetch(): InternalRow = {
    assert(index >= 0 && index < input.size)
    val values = Array(input(index).asInstanceOf[Any])
    new GenericInternalRow(values)
  }

  override def close(): Unit = {
    index = Int.MinValue
  }
}

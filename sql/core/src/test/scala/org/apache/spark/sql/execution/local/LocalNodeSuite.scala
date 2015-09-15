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


class LocalNodeSuite extends LocalNodeTest {
  private val data = (1 to 100).map { i => (i, i) }.toArray

  test("basic open, next, fetch, close") {
    val node = new DummyNode(kvIntAttributes, data)
    assert(!node.isOpen)
    node.open()
    assert(node.isOpen)
    data.foreach { case (k, v) =>
      assert(node.next())
      // fetch should be idempotent
      val fetched = node.fetch()
      assert(node.fetch() === fetched)
      assert(node.fetch() === fetched)
      assert(node.fetch().numFields === 2)
      assert(node.fetch().getInt(0) === k)
      assert(node.fetch().getInt(1) === v)
    }
    assert(!node.next())
    node.close()
    assert(!node.isOpen)
  }

  test("asIterator") {
    val node = new DummyNode(kvIntAttributes, data)
    val iter = node.asIterator
    node.open()
    data.foreach { case (k, v) =>
      // hasNext should be idempotent
      assert(iter.hasNext)
      assert(iter.hasNext)
      val item = iter.next()
      assert(item.numFields === 2)
      assert(item.getInt(0) === k)
      assert(item.getInt(1) === v)
    }
    intercept[NoSuchElementException] {
      iter.next()
    }
    node.close()
  }

  test("collect") {
    val node = new DummyNode(kvIntAttributes, data)
    node.open()
    val collected = node.collect()
    assert(collected.size === data.size)
    assert(collected.forall(_.size === 2))
    assert(collected.map { case row => (row.getInt(0), row.getInt(0)) } === data)
    node.close()
  }

}

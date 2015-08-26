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

import org.apache.spark.sql.execution.RowIterator

/**
 * A special RowIterator that has two extra methods: `open` and `close`, which we can use them to
 * manage resources in this Iterator.
 */
private[sql] abstract class OpenCloseRowIterator extends RowIterator {

  /**
   * Initializes the iterator state. Must be called before using this Iterator.
   *
   * Implementations of this must also call the `open()` function of its children.
   */
  def open(): Unit

  /**
   * Closes the iterator and releases all resources.
   *
   * Implementations of this must also call the `close()` function of its children.
   */
  def close(): Unit

}

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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(window_column) - Extract the time value from time/session window column which can be used for event time value of window.
      The extracted time is (window.end - 1) which reflects the fact that the the aggregating
      windows have exclusive upper bound - [start, end)
      See <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time">'Window Operations on Event Time'</a> in Structured Streaming guide doc for detailed explanation and examples.
  """,
  arguments = """
    Arguments:
      * window_column - The column representing time/session window.
  """,
  examples = """
    Examples:
      > SELECT a, window.start as start, window.end as end, _FUNC_(window), cnt FROM (SELECT a, window, count(*) as cnt FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', '2021-01-01 00:04:30'), ('A1', '2021-01-01 00:06:00'), ('A2', '2021-01-01 00:01:00') AS tab(a, b) GROUP by a, window(b, '5 minutes') ORDER BY a, window.start);
        A1	2021-01-01 00:00:00	2021-01-01 00:05:00	2021-01-01 00:04:59.999999	2
        A1	2021-01-01 00:05:00	2021-01-01 00:10:00	2021-01-01 00:09:59.999999	1
        A2	2021-01-01 00:00:00	2021-01-01 00:05:00	2021-01-01 00:04:59.999999	1
  """,
  group = "datetime_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit line.contains.tab
case class WindowTime(windowColumn: Expression)
  extends UnaryExpression
    with ImplicitCastInputTypes
    with Unevaluable
    with NonSQLExpression {

  override def child: Expression = windowColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(StructType)

  override def dataType: DataType = child.dataType.asInstanceOf[StructType].head.dataType

  override def prettyName: String = "window_time"

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  override protected def withNewChildInternal(newChild: Expression): WindowTime =
    copy(windowColumn = newChild)
}

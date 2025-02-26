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

package org.apache.spark.sql

import org.apache.spark.annotation.{DeveloperApi, Since}

// scalastyle:off line.size.limit
/**
 * Base trait for implementations used by [[SparkSessionExtensions]]
 *
 *
 * For example, now we have an external function named `Age` to register as an extension for SparkSession:
 *
 *
 * {{{
 *   package org.apache.spark.examples.extensions
 *
 *   import org.apache.spark.sql.catalyst.expressions.{CurrentDate, Expression, RuntimeReplaceable, SubtractDates}
 *
 *   case class Age(birthday: Expression, child: Expression) extends RuntimeReplaceable {
 *
 *     def this(birthday: Expression) = this(birthday, SubtractDates(CurrentDate(), birthday))
 *     override def exprsReplaced: Seq[Expression] = Seq(birthday)
 *     override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
 *   }
 * }}}
 *
 * We need to create our extension which inherits [[SparkSessionExtensionsProvider]]
 * Example:
 *
 * {{{
 *   package org.apache.spark.examples.extensions
 *
 *   import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
 *   import org.apache.spark.sql.catalyst.FunctionIdentifier
 *   import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
 *
 *   class MyExtensions extends SparkSessionExtensionsProvider {
 *     override def apply(v1: SparkSessionExtensions): Unit = {
 *       v1.injectFunction(
 *         (new FunctionIdentifier("age"),
 *           new ExpressionInfo(classOf[Age].getName, "age"),
 *           (children: Seq[Expression]) => new Age(children.head)))
 *     }
 *   }
 * }}}
 *
 * Then, we can inject `MyExtensions` in three ways,
 * <ul>
 *   <li>withExtensions of [[SparkSession.Builder]]</li>
 *   <li>Config - spark.sql.extensions</li>
 *   <li>[[java.util.ServiceLoader]] - Add to src/main/resources/META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider</li>
 * </ul>
 *
 * @see [[SparkSessionExtensions]]
 * @see [[SparkSession.Builder]]
 * @see [[java.util.ServiceLoader]]
 *
 * @since 3.2.0
 */
@DeveloperApi
@Since("3.2.0")
trait SparkSessionExtensionsProvider extends Function1[SparkSessionExtensions, Unit]
// scalastyle:on line.size.limit

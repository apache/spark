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

package org.apache.spark.sql.catalyst.expressions.codegen;

/**
 * A narrowing-soundness fixture for {@code CodeCompilerSuite}: a public class declaring a
 * public STATIC method. A Scala anonymous subclass can declare an INSTANCE method of the same
 * erased signature, because scalac does not treat a Java static as an inherited member, while
 * javac rejects the pair outright. That combination has to be written in Java to exist at all.
 *
 * Narrowing a reference to such a subclass is unsound: a static call is bound statically, so
 * {@code ((StaticClashBase) ref).value()} reaches this method, not the subclass's.
 */
public class StaticClashBase {
  public static int value() { return 1; }
}

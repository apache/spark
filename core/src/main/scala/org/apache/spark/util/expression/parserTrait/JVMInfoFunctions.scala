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
package org.apache.spark.util.expression.parserTrait

/**
 * Functions are evaluated at point of expansion
 */
private[spark] trait JVMInfoFunctions extends FunctionExpansion {
  protected abstract override def functions: Map[String, () => Double] = Map(
    "JVMTotalMemoryBytes".toLowerCase -> (() => Runtime.getRuntime.totalMemory() * 1.0),
    "JVMMaxMemoryBytes".toLowerCase -> (() => Runtime.getRuntime.maxMemory * 1.0),
    "JVMFreeMemoryBytes".toLowerCase-> (() => Runtime.getRuntime.freeMemory * 1.0),
    "JVMNumCores".toLowerCase -> (() => Runtime.getRuntime.availableProcessors * 1.0)
    ) ++ super.functions
}

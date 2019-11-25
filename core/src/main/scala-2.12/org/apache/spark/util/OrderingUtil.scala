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

package org.apache.spark.util

/**
 * This class only exists to bridge the difference between Scala 2.12 and Scala 2.13's
 * support for floating-point ordering. It is implemented separately for both as there
 * is no method that exists in both for comparison.
 *
 * It functions like Ordering.Double in Scala 2.12.
 */
private[spark] object OrderingUtil {
  
  def compareDouble(x: Double, y: Double): Int = Ordering.Double.compare(x, y)

  def compareFloat(x: Float, y: Float): Int = Ordering.Float.compare(x, y)

}

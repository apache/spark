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

/**
 * A bunch of functions use for testing udf serialization.
 */
object TestUDFs {
  type L = Long
  val udf0: () => Double = new Function0[Double] with Serializable {
    override def apply(): Double = Math.random()
  }

  val udf1: L => Tuple1[L] = new (L => Tuple1[L]) with Serializable {
    override def apply(i0: L): Tuple1[L] = Tuple1(i0)
  }

  val udf2: (L, L) => (L, L) = new ((L, L) => (L, L)) with Serializable {
    override def apply(i0: L, i1: L): (L, L) = (i0, i1)
  }

  val udf3: (L, L, L) => (L, L, L) = new ((L, L, L) => (L, L, L)) with Serializable {
    override def apply(i0: L, i1: L, i2: L): (L, L, L) = (i0, i1, i2)
  }

  val udf4: (L, L, L, L) => (L, L, L, L) = new ((L, L, L, L) => (L, L, L, L)) with Serializable {
    override def apply(i0: L, i1: L, i2: L, i3: L): (L, L, L, L) = (i0, i1, i2, i3)
  }
}

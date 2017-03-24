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

package org.apache.spark.util.collection

import scala.collection.mutable.HashMap

class ValueIncrementableHashMapAccumulator[T] extends HashMap[T,Long] with Serializable {
  
  override def += (kv : (T, Long)): this.type = {
    if(this.contains(kv._1)){
      // Key already exists, just increment the value for that Key
      this.put(kv._1, kv._2 + this.get(kv._1).get)
    }
    else{
        this.put(kv._1, kv._2)
    }
    this
  }
  
}

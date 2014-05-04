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

package org.apache.spark.sql.api.python

import net.razorvine.pickle.Pickler
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.api.java.JavaRDD
import java.util.{Map => JMap}


/**
 * :: DeveloperApi ::
 * The Java stubs necessary for the Python mllib bindings.
 *
 * See python/pyspark/mllib/_common.py for the mutually agreed upon data format.
 */
@DeveloperApi
class PythonSQLAPI extends Serializable {

  def javaToPython(rdd: SchemaRDD): JavaRDD[Array[Byte]] = {
    val fieldNames: Seq[String] = rdd.queryExecution.analyzed.output.map(_.name)
    rdd.mapPartitions {
      iter =>
        val pickle = new Pickler
        iter.map {
          row =>
            val map: JMap[String, Any] = new java.util.HashMap
            // TODO: We place the map in an ArrayList so that the object is pickled to a List[Dict].
            // Ideally we should be able to pickle an object directly into a Python collection so we
            // don't have to create an ArrayList every time.
            val arr: java.util.ArrayList[Any] = new java.util.ArrayList
            row.zip(fieldNames).foreach {
              case (obj, name) =>
                map.put(name, obj)
            }
            arr.add(map)
            pickle.dumps(arr)
        }
    }
  }
}

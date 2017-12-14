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

package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigEntry

object Common {
  case class Provenance(from: String, value: String) {
    override def toString: String = s"Provenance(from = $from, value = $value)"
  }

  object Provenance {
    def fromConf(sysPropName: String): Option[Provenance] = {
      sys.props.get(sysPropName).map(Provenance(s"Spark config $sysPropName", _))
    }

    def fromConf(sparkConf: SparkConf, conf: ConfigEntry[Option[String]]): Option[Provenance] = {
      sparkConf.get(conf).map(Provenance(s"Spark config ${conf.key}", _))
    }

    def fromConf(sparkConf: SparkConf, key: String): Option[Provenance] = {
      sparkConf.getOption(key).map(Provenance(s"Spark config $key", _))
    }

    def fromEnv(name: String): Option[Provenance] = {
      sys.env.get(name).map(Provenance(s"Environment variable $name", _))
    }
  }
}

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


package org.apache.hadoop.hive.thrift

import java.lang.reflect.{Field, Method}
import java.util.Map
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.security.SaslRpcServer
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Functions that bridge Thrift's SASL transports to Hadoop's SASL callback
 * handlers and authentication classes.
 *
 * This is a 0.23/2.x specific implementation
 */
class HadoopThriftAuthBridge23 extends HadoopThriftAuthBridge20S {

  /**
   * Read and return Hadoop SASL configuration which can be configured using
   * "hadoop.rpc.protection"
   *
   * @param conf
   * @return Hadoop SASL configuration
   */
  override def getHadoopSaslProperties(conf: Configuration): Map[String, String] =
  {
    if (!HadoopThriftAuthBridge23.latch.getAndSet(true)) {
      try {
        HadoopThriftAuthBridge23.SASL_PROPERTIES_RESOLVER_CLASS =
          Class.forName(HadoopThriftAuthBridge23.SASL_PROP_RES_CLASSNAME);
      } catch {
        case e =>
          throw new IllegalStateException("Error finding SaslPropertiesResolver", e);
      }

      if (HadoopThriftAuthBridge23.SASL_PROPERTIES_RESOLVER_CLASS != null) {
        // found the class, so this would be hadoop version 2.4 or newer (See
        // HADOOP-10221, HADOOP-10451)
        try {
          HadoopThriftAuthBridge23.RES_GET_INSTANCE_METHOD =
            HadoopThriftAuthBridge23.SASL_PROPERTIES_RESOLVER_CLASS
              .getMethod("getInstance", classOf[Configuration])
          HadoopThriftAuthBridge23.GET_DEFAULT_PROP_METHOD = HadoopThriftAuthBridge23
            .SASL_PROPERTIES_RESOLVER_CLASS.getMethod("getDefaultProperties")
        } catch {
          case e =>
            throw new IllegalStateException("Error finding method getDefaultProperties", e);
        }
      }

      if (HadoopThriftAuthBridge23.SASL_PROPERTIES_RESOLVER_CLASS == null
        || HadoopThriftAuthBridge23.GET_DEFAULT_PROP_METHOD == null) {
        // this must be a hadoop 2.4 version or earlier.
        // Resorting to the earlier method of getting the properties, which uses SASL_PROPS field
        try {
          HadoopThriftAuthBridge23.SASL_PROPS_FIELD = classOf[SaslRpcServer]
            .getField("SASL_PROPS");
        } catch {
          case e: NoSuchFieldException =>
            throw new IllegalStateException("Error finding hadoop SASL_PROPS field in "
              + classOf[SaslRpcServer].getSimpleName, e);
        }
      }
    }
    if (HadoopThriftAuthBridge23.SASL_PROPS_FIELD != null) {
      // hadoop 2.4 and earlier way of finding the sasl property settings
      // Initialize the SaslRpcServer to ensure QOP parameters are read from
      // conf
      SaslRpcServer.init(conf);
      try {
        return HadoopThriftAuthBridge23.SASL_PROPS_FIELD.get(null).asInstanceOf[Map[String, String]]
      } catch {
        case e =>
          throw new IllegalStateException("Error finding hadoop SASL properties", e);
      }
    }
    // 2.5 and later way of finding sasl property
    try {
      val saslPropertiesResolver: Configurable = HadoopThriftAuthBridge23
        .RES_GET_INSTANCE_METHOD.invoke(null, conf)
        .asInstanceOf[Configurable]
      saslPropertiesResolver.setConf(conf);
      return HadoopThriftAuthBridge23.GET_DEFAULT_PROP_METHOD.invoke(saslPropertiesResolver)
        .asInstanceOf[Map[String, String]]
    } catch {
      case e => throw new IllegalStateException("Error finding hadoop SASL properties", e);
    }
  }
}

object HadoopThriftAuthBridge23 {
  var SASL_PROPS_FIELD: Field = null
  var SASL_PROPERTIES_RESOLVER_CLASS: Class[_] = null
  var RES_GET_INSTANCE_METHOD: Method = _
  var GET_DEFAULT_PROP_METHOD: Method = _
  val SASL_PROP_RES_CLASSNAME = "org.apache.hadoop.security.SaslPropertiesResolver"
  var latch = new AtomicBoolean(false)
}

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

package org.apache.spark.internal.config

private[spark] object Kafka {

  val BOOTSTRAP_SERVERS =
    ConfigBuilder("spark.kafka.bootstrap.servers")
      .doc("A list of coma separated host/port pairs to use for establishing the initial " +
        "connection to the Kafka cluster. For further details please see kafka documentation. " +
        "Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val SECURITY_PROTOCOL =
    ConfigBuilder("spark.kafka.security.protocol")
      .doc("Protocol used to communicate with brokers. For further details please see kafka " +
        "documentation. Only used to obtain delegation token.")
      .stringConf
      .createWithDefault("SASL_SSL")

  val KERBEROS_SERVICE_NAME =
    ConfigBuilder("spark.kafka.sasl.kerberos.service.name")
      .doc("The Kerberos principal name that Kafka runs as. This can be defined either in " +
        "Kafka's JAAS config or in Kafka's config. For further details please see kafka " +
        "documentation. Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val TRUSTSTORE_LOCATION =
    ConfigBuilder("spark.kafka.ssl.truststore.location")
      .doc("The location of the trust store file. For further details please see kafka " +
        "documentation. Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val TRUSTSTORE_PASSWORD =
    ConfigBuilder("spark.kafka.ssl.truststore.password")
      .doc("The store password for the trust store file. This is optional for client and only " +
        "needed if ssl.truststore.location is configured. For further details please see kafka " +
        "documentation. Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val KEYSTORE_LOCATION =
    ConfigBuilder("spark.kafka.ssl.keystore.location")
      .doc("The location of the key store file. This is optional for client and can be used for " +
        "two-way authentication for client. For further details please see kafka documentation. " +
        "Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val KEYSTORE_PASSWORD =
    ConfigBuilder("spark.kafka.ssl.keystore.password")
      .doc("The store password for the key store file. This is optional for client and only " +
        "needed if ssl.keystore.location is configured. For further details please see kafka " +
        "documentation. Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val KEY_PASSWORD =
    ConfigBuilder("spark.kafka.ssl.key.password")
      .doc("The password of the private key in the key store file. This is optional for client. " +
        "For further details please see kafka documentation. Only used to obtain delegation token.")
      .stringConf
      .createOptional

  val TOKEN_SASL_MECHANISM =
    ConfigBuilder("spark.kafka.sasl.token.mechanism")
      .doc("SASL mechanism used for client connections with delegation token. Because SCRAM " +
        "login module used for authentication a compatible mechanism has to be set here. " +
        "For further details please see kafka documentation (sasl.mechanism). Only used to " +
        "authenticate against Kafka broker with delegation token.")
      .stringConf
      .createWithDefault("SCRAM-SHA-512")
}

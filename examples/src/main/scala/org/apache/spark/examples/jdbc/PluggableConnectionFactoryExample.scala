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
package org.apache.spark.examples.jdbc

import java.sql.Connection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc._

object RDS {
  def balancedUrl: String = "TBD"
}

/*
In real life the above would be something like the below but I did not want to introduce a
dependency on AWS-RDS:

object RDS extends Logging {
  lazy val config: Config = ConfigFactory.load(s"typesafe_configfile").getConfig("myconfig")

  lazy val clusterId = config.getString("rds.cluster")

  lazy val rds = AmazonRDSClientBuilder.defaultClient() // requires AWS_REGION environment variable
                                                        // as well as AWS credentials

  private var endpoints: Seq[String] = null

  def balancedUrl: String = this.synchronized {
    // initialize or rotate list of endpoints
    endpoints = if (endpoints == null) {
      rds.describeDBInstances().getDBInstances.asScala
        .filter(i => i.getDBClusterIdentifier == clusterId && i.getDBInstanceStatus == "available")
        .map(instance => s"${instance.getEndpoint.getAddress}:${instance.getEndpoint.getPort}")
    } else endpoints.drop(1) ++ endpoints.take(1)
    endpoints.mkString(s"jdbc:postgresql://",",","/dbname")
  }
}
*/

class RDSLoadBalancingConnectionFactory extends ConnectionFactoryProvider {
  override def createConnectionFactory(options: JDBCOptions): () => Connection =
    () => LoadDriver(options).connect(RDS.balancedUrl, options.asConnectionProperties)
}

object PluggableConnectionFactoryExample {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Load balance jdbc connections to RDS")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.read
      .format("jdbc")
      // this will enable load balancing against RDS cluster - use for reads only!
      .option(JDBCOptions.JDBC_CONNECTION_FACTORY_PROVIDER,
            "org.apache.spark.examples.jdbc.RDSLoadBalancingConnectionFactory")
  }
}


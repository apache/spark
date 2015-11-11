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

package org.apache.spark

import java.lang.{Byte => JByte}
import java.security.SecureRandom

import com.google.common.hash.HashCodes
import org.apache.hadoop.io.Text

import org.apache.spark.deploy.SparkHadoopUtil

/**
 * A password authenticator which obtains a user password from Spark configuration. Given a user
 * xxx wants to login, the authenticator will for the password at `spark.authenticate.secrets.xxx`.
 * If the password cannot be found, it returns the default secret which is defined at
 * `spark.authenticate.secret`. In Yarn mode, the default secret is generated in [[getYarnSecret()]]
 * method.
 */
private[spark] class DefaultPasswordAuthenticator(sparkConf: SparkConf)
    extends PasswordAuthenticator with Logging {

  import DefaultPasswordAuthenticator._

  override def getPassword(authId: String): String = {
    sparkConf.get(s"$SECRETS_CONF.$authId", defaultSecret)
  }

  private[this] val defaultSecret = generateDefaultSecretKey()

  /**
   * Generates or looks up the secret key.
   *
   * The way the key is stored depends on the Spark deployment mode. Yarn
   * uses the Hadoop UGI.
   *
   * For non-Yarn deployments, If the config variable is not set
   * we throw an exception.
   */
  private def generateDefaultSecretKey(): String = {
    if (SparkHadoopUtil.get.isYarnMode()) {
      getYarnSecret()
    } else {
      // user must have set spark.authenticate.secret config
      // For Master/Worker, auth secret is in conf; for Executors, it is in env variable
      sparkConf.getOption(SecurityManager.SPARK_AUTH_SECRET_CONF) match {
        case Some(value) => value
        case None =>
          throw new IllegalArgumentException(
            "Error: a secret key must be specified via the " +
                SecurityManager.SPARK_AUTH_SECRET_CONF + " config")
      }
    }
  }

  private def getYarnSecret(): String = {
    // In YARN mode, the secure cookie will be created by the driver and stashed in the
    // user's credentials, where executors can get it. The check for an array of size 0
    // is because of the test code in YarnSparkHadoopUtilSuite.
    val secretKey = SparkHadoopUtil.get.getSecretKeyFromUserCredentials(SECRET_LOOKUP_KEY)
    if (secretKey == null || secretKey.length == 0) {
      logDebug("generateSecretKey: yarn mode, secret key from credentials is null")
      val rnd = new SecureRandom()
      val length = sparkConf.getInt(SECRET_BIT_LENGTH_CONF, DEFAULT_SECRET_BIT_LENGTH) / JByte.SIZE
      val secret = new Array[Byte](length)
      rnd.nextBytes(secret)

      val cookie = HashCodes.fromBytes(secret).toString
      SparkHadoopUtil.get.addSecretKeyToUserCredentials(SECRET_LOOKUP_KEY, cookie)
      cookie
    } else {
      new Text(secretKey).toString
    }
  }

}

object DefaultPasswordAuthenticator {
  // key used to store the spark secret in the Hadoop UGI
  val SECRET_LOOKUP_KEY = "sparkCookie"

  val SECRET_BIT_LENGTH_CONF = "spark.authenticate.secretBitLength"

  val DEFAULT_SECRET_BIT_LENGTH = 256

  val SECRETS_CONF = "spark.authenticate.secrets"
}

package org.apache.spark.deploy.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf

/**
 * A credential provider for a service. User must implement this if they need to access a
 * secure service from Spark.
 */
trait ServiceCredentialProvider {

  /**
   * Name of the service to provide credentials. This name should unique, Spark internally will
   * use this name to differentiate credential provider.
   */
  def serviceName: String

  /**
   * To decide whether credential is required for this service. By default it based on whether
   * Hadoop security is enabled.
   */
  def credentialsRequired(hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  /**
   * Obtain credentials for this service and get the time of the next renewal.
   * @param hadoopConf Configuration of current Hadoop Compatible system.
   * @param sparkConf Spark configuration.
   * @param creds Credentials to add tokens and security keys to.
   * @return If this Credential is renewable and can be renewed, return the time of the next
   *         renewal, otherwise None should be returned.
   */
  def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long]
}

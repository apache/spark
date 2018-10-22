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

import java.net.{Authenticator, PasswordAuthentication}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.util.Utils

/**
 * Spark class responsible for security.
 *
 * In general this class should be instantiated by the SparkEnv and most components
 * should access it from that. There are some cases where the SparkEnv hasn't been
 * initialized yet and this class must be instantiated directly.
 *
 * This class implements all of the configuration related to security features described
 * in the "Security" document. Please refer to that document for specific features implemented
 * here.
 */
private[spark] class SecurityManager(
    sparkConf: SparkConf,
    val ioEncryptionKey: Option[Array[Byte]] = None)
  extends Logging with SecretKeyHolder {

  import SecurityManager._

  // allow all users/groups to have view/modify permissions
  private val WILDCARD_ACL = "*"

  private val authOn = sparkConf.get(NETWORK_AUTH_ENABLED)
  // keep spark.ui.acls.enable for backwards compatibility with 1.0
  private var aclsOn =
    sparkConf.getBoolean("spark.acls.enable", sparkConf.getBoolean("spark.ui.acls.enable", false))

  // admin acls should be set before view or modify acls
  private var adminAcls: Set[String] =
    stringToSet(sparkConf.get("spark.admin.acls", ""))

  // admin group acls should be set before view or modify group acls
  private var adminAclsGroups : Set[String] =
    stringToSet(sparkConf.get("spark.admin.acls.groups", ""))

  private var viewAcls: Set[String] = _

  private var viewAclsGroups: Set[String] = _

  // list of users who have permission to modify the application. This should
  // apply to both UI and CLI for things like killing the application.
  private var modifyAcls: Set[String] = _

  private var modifyAclsGroups: Set[String] = _

  // always add the current user and SPARK_USER to the viewAcls
  private val defaultAclUsers = Set[String](System.getProperty("user.name", ""),
    Utils.getCurrentUserName())

  setViewAcls(defaultAclUsers, sparkConf.get("spark.ui.view.acls", ""))
  setModifyAcls(defaultAclUsers, sparkConf.get("spark.modify.acls", ""))

  setViewAclsGroups(sparkConf.get("spark.ui.view.acls.groups", ""));
  setModifyAclsGroups(sparkConf.get("spark.modify.acls.groups", ""));

  private var secretKey: String = _
  logInfo("SecurityManager: authentication " + (if (authOn) "enabled" else "disabled") +
    "; ui acls " + (if (aclsOn) "enabled" else "disabled") +
    "; users  with view permissions: " + viewAcls.toString() +
    "; groups with view permissions: " + viewAclsGroups.toString() +
    "; users  with modify permissions: " + modifyAcls.toString() +
    "; groups with modify permissions: " + modifyAclsGroups.toString())

  // Set our own authenticator to properly negotiate user/password for HTTP connections.
  // This is needed by the HTTP client fetching from the HttpServer. Put here so its
  // only set once.
  if (authOn) {
    Authenticator.setDefault(
      new Authenticator() {
        override def getPasswordAuthentication(): PasswordAuthentication = {
          var passAuth: PasswordAuthentication = null
          val userInfo = getRequestingURL().getUserInfo()
          if (userInfo != null) {
            val  parts = userInfo.split(":", 2)
            passAuth = new PasswordAuthentication(parts(0), parts(1).toCharArray())
          }
          return passAuth
        }
      }
    )
  }

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
  // the default SSL configuration - it will be used by all communication layers unless overwritten
  private val defaultSSLOptions =
    SSLOptions.parse(sparkConf, hadoopConf, "spark.ssl", defaults = None)

  def getSSLOptions(module: String): SSLOptions = {
    val opts =
      SSLOptions.parse(sparkConf, hadoopConf, s"spark.ssl.$module", Some(defaultSSLOptions))
    logDebug(s"Created SSL options for $module: $opts")
    opts
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a Set of strings
   */
  private def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setViewAcls(defaultUsers: Set[String], allowedUsers: String) {
    viewAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
  }

  def setViewAcls(defaultUser: String, allowedUsers: String) {
    setViewAcls(Set[String](defaultUser), allowedUsers)
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setViewAclsGroups(allowedUserGroups: String) {
    viewAclsGroups = (adminAclsGroups ++ stringToSet(allowedUserGroups));
    logInfo("Changing view acls groups to: " + viewAclsGroups.mkString(","))
  }

  /**
   * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
   */
  def getViewAcls: String = {
    if (viewAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAcls.mkString(",")
    }
  }

  def getViewAclsGroups: String = {
    if (viewAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAclsGroups.mkString(",")
    }
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setModifyAcls(defaultUsers: Set[String], allowedUsers: String) {
    modifyAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    logInfo("Changing modify acls to: " + modifyAcls.mkString(","))
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setModifyAclsGroups(allowedUserGroups: String) {
    modifyAclsGroups = (adminAclsGroups ++ stringToSet(allowedUserGroups));
    logInfo("Changing modify acls groups to: " + modifyAclsGroups.mkString(","))
  }

  /**
   * Checking the existence of "*" is necessary as YARN can't recognize the "*" in "defaultuser,*"
   */
  def getModifyAcls: String = {
    if (modifyAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAcls.mkString(",")
    }
  }

  def getModifyAclsGroups: String = {
    if (modifyAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAclsGroups.mkString(",")
    }
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setAdminAcls(adminUsers: String) {
    adminAcls = stringToSet(adminUsers)
    logInfo("Changing admin acls to: " + adminAcls.mkString(","))
  }

  /**
   * Admin acls groups should be set before the view or modify acls groups. If you modify the admin
   * acls groups you should also set the view and modify acls groups again to pick up the changes.
   */
  def setAdminAclsGroups(adminUserGroups: String) {
    adminAclsGroups = stringToSet(adminUserGroups)
    logInfo("Changing admin acls groups to: " + adminAclsGroups.mkString(","))
  }

  def setAcls(aclSetting: Boolean) {
    aclsOn = aclSetting
    logInfo("Changing acls enabled to: " + aclsOn)
  }

  def getIOEncryptionKey(): Option[Array[Byte]] = ioEncryptionKey

  /**
   * Check to see if Acls for the UI are enabled
   * @return true if UI authentication is enabled, otherwise false
   */
  def aclsEnabled(): Boolean = aclsOn

  /**
   * Checks the given user against the view acl and groups list to see if they have
   * authorization to view the UI. If the UI acls are disabled
   * via spark.acls.enable, all users have view access. If the user is null
   * it is assumed authentication is off and all users have access. Also if any one of the
   * UI acls or groups specify the WILDCARD(*) then all users have view access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkUIViewPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " viewAcls=" +
      viewAcls.mkString(",") + " viewAclsGroups=" + viewAclsGroups.mkString(","))
    if (!aclsEnabled || user == null || viewAcls.contains(user) ||
        viewAcls.contains(WILDCARD_ACL) || viewAclsGroups.contains(WILDCARD_ACL)) {
      return true
    }
    val currentUserGroups = Utils.getCurrentUserGroups(sparkConf, user)
    logDebug("userGroups=" + currentUserGroups.mkString(","))
    viewAclsGroups.exists(currentUserGroups.contains(_))
  }

  /**
   * Checks the given user against the modify acl and groups list to see if they have
   * authorization to modify the application. If the modify acls are disabled
   * via spark.acls.enable, all users have modify access. If the user is null
   * it is assumed authentication isn't turned on and all users have access. Also if any one
   * of the modify acls or groups specify the WILDCARD(*) then all users have modify access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkModifyPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " modifyAcls=" +
      modifyAcls.mkString(",") + " modifyAclsGroups=" + modifyAclsGroups.mkString(","))
    if (!aclsEnabled || user == null || modifyAcls.contains(user) ||
        modifyAcls.contains(WILDCARD_ACL) || modifyAclsGroups.contains(WILDCARD_ACL)) {
      return true
    }
    val currentUserGroups = Utils.getCurrentUserGroups(sparkConf, user)
    logDebug("userGroups=" + currentUserGroups)
    modifyAclsGroups.exists(currentUserGroups.contains(_))
  }

  /**
   * Check to see if authentication for the Spark communication protocols is enabled
   * @return true if authentication is enabled, otherwise false
   */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
   * Checks whether network encryption should be enabled.
   * @return Whether to enable encryption when connecting to services that support it.
   */
  def isEncryptionEnabled(): Boolean = {
    sparkConf.get(NETWORK_ENCRYPTION_ENABLED) || sparkConf.get(SASL_ENCRYPTION_ENABLED)
  }

  /**
   * Gets the user used for authenticating HTTP connections.
   * For now use a single hardcoded user.
   * @return the HTTP user as a String
   */
  def getHttpUser(): String = "sparkHttpUser"

  /**
   * Gets the user used for authenticating SASL connections.
   * For now use a single hardcoded user.
   * @return the SASL user as a String
   */
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key.
   * @return the secret key as a String if authentication is enabled, otherwise returns null
   */
  def getSecretKey(): String = {
    if (isAuthenticationEnabled) {
      val creds = UserGroupInformation.getCurrentUser().getCredentials()
      Option(creds.getSecretKey(SECRET_LOOKUP_KEY))
        .map { bytes => new String(bytes, UTF_8) }
        // Secret key may not be found in current UGI's credentials.
        // This happens when UGI is refreshed in the driver side by UGI's loginFromKeytab but not
        // copy secret key from original UGI to the new one. This exists in ThriftServer's Hive
        // logic. So as a workaround, storing secret key in a local variable to make it visible
        // in different context.
        .orElse(Option(secretKey))
        .orElse(Option(sparkConf.getenv(ENV_AUTH_SECRET)))
        .orElse(sparkConf.getOption(SPARK_AUTH_SECRET_CONF))
        .getOrElse {
          throw new IllegalArgumentException(
            s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config")
        }
    } else {
      null
    }
  }

  /**
   * Initialize the authentication secret.
   *
   * If authentication is disabled, do nothing.
   *
   * In YARN and local mode, generate a new secret and store it in the current user's credentials.
   *
   * In other modes, assert that the auth secret is set in the configuration.
   */
  def initializeAuth(): Unit = {
    import SparkMasterRegex._

    if (!sparkConf.get(NETWORK_AUTH_ENABLED)) {
      return
    }

    val master = sparkConf.get(SparkLauncher.SPARK_MASTER, "")
    master match {
      case "yarn" | "local" | LOCAL_N_REGEX(_) | LOCAL_N_FAILURES_REGEX(_, _) =>
        // Secret generation allowed here
      case _ =>
        require(sparkConf.contains(SPARK_AUTH_SECRET_CONF),
          s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config.")
        return
    }

    secretKey = Utils.createSecret(sparkConf)
    val creds = new Credentials()
    creds.addSecretKey(SECRET_LOOKUP_KEY, secretKey.getBytes(UTF_8))
    UserGroupInformation.getCurrentUser().addCredentials(creds)
  }

  // Default SecurityManager only has a single secret key, so ignore appId.
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()
}

private[spark] object SecurityManager {

  val SPARK_AUTH_CONF = NETWORK_AUTH_ENABLED.key
  val SPARK_AUTH_SECRET_CONF = "spark.authenticate.secret"
  // This is used to set auth secret to an executor's env variable. It should have the same
  // value as SPARK_AUTH_SECRET_CONF set in SparkConf
  val ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"

  // key used to store the spark secret in the Hadoop UGI
  val SECRET_LOOKUP_KEY = new Text("sparkCookie")
}

package org.apache.hadoop.hdfsproxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import java.io.IOException;
import java.util.Arrays;

/**
 * This filter is required for hdfsproxies connecting to HDFS
 * with kerberos authentication. Keytab file and principal to
 * use for proxy user is retrieved from a configuration file.
 * If user attribute in ldap doesn't kerberos realm, the 
 * default realm is picked up from configuration. 
 */
public class KerberosAuthorizationFilter extends AuthorizationFilter {

  private String defaultRealm;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    super.init(filterConfig);
    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    conf.addResource("hdfsproxy-site.xml");
    initializeUGI(conf);
    initDefaultRealm(conf);
  }

  private void initializeUGI(Configuration conf) {
    try {
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");

      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(
          conf.get("hdfsproxy.kerberos.principal"),
          conf.get("hdfsproxy.kerberos.keytab"));

      LOG.info(contextPath + " :: Logged in user: " +
          UserGroupInformation.getLoginUser().getUserName() +
          ", Current User: " + UserGroupInformation.getCurrentUser().getUserName());

    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize credentials", e);
    }
  }

  private void initDefaultRealm(Configuration conf) {
    defaultRealm = conf.get("hdfsproxy.kerberos.default.realm","");
  }

  @Override
  /** If the userid does not have realm, add the default realm */
  protected String getUserId(ServletRequest request) {
    String userId =  super.getUserId(request);
    return userId +
        (userId.indexOf('@') > 0 ? "" : defaultRealm);
  }

  @Override
  protected String getGroups(ServletRequest request) {
    UserGroupInformation ugi = UserGroupInformation.
        createRemoteUser(getUserId(request));
    return Arrays.toString(ugi.getGroupNames());
  }
}

package org.apache.hadoop.mapred;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRPC.TestImpl;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapred.AuditLogger.Keys;
import org.apache.hadoop.net.NetUtils;

import junit.framework.TestCase;

/**
 * Tests {@link AuditLogger}.
 */
public class TestAuditLogger extends TestCase {
  private static final String USER = "test";
  private static final String OPERATION = "oper";
  private static final String TARGET = "tgt";
  private static final String PERM = "admin group";
  private static final String DESC = "description of an audit log";
  
  /**
   * Test the AuditLog format with key-val pair.
   */
  public void testKeyValLogFormat() {
    StringBuilder actLog = new StringBuilder();
    StringBuilder expLog = new StringBuilder();
    // add the first k=v pair and check
    AuditLogger.start(Keys.USER, USER, actLog);
    expLog.append("USER=test");
    assertEquals(expLog.toString(), actLog.toString());
    
    // append another k1=v1 pair to already added k=v and test
    AuditLogger.add(Keys.OPERATION, OPERATION, actLog);
    expLog.append("\tOPERATION=oper");
    assertEquals(expLog.toString(), actLog.toString());
    
    // append another k1=null pair and test
    AuditLogger.add(Keys.PERMISSIONS, (String)null, actLog);
    expLog.append("\tPERMISSIONS=null");
    assertEquals(expLog.toString(), actLog.toString());
    
    // now add the target and check of the final string
    AuditLogger.add(Keys.TARGET, TARGET, actLog);
    expLog.append("\tTARGET=tgt");
    assertEquals(expLog.toString(), actLog.toString());
  }
  
  /**
   * Test the AuditLog format for successful events.
   */
  private void testSuccessLogFormat(boolean checkIP) {
    // check without the IP
    String sLog = AuditLogger.createSuccessLog(USER, OPERATION, TARGET);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");
    assertEquals(expLog.toString(), sLog);
    
  }
  
  /**
   * Test the AuditLog format for failure events.
   */
  private void testFailureLogFormat(boolean checkIP, String perm) {
    String fLog = 
      AuditLogger.createFailureLog(USER, OPERATION, perm, TARGET, DESC);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
    expLog.append("DESCRIPTION=description of an audit log\t");
    expLog.append("PERMISSIONS=" + perm);
    assertEquals(expLog.toString(), fLog);
  }
  
  /**
   * Test the AuditLog format for failure events.
   */
  private void testFailureLogFormat(boolean checkIP) {
    testFailureLogFormat(checkIP, PERM);
    testFailureLogFormat(checkIP, null);
  }
  
  /**
   * Test {@link AuditLogger} without IP set.
   */
  public void testAuditLoggerWithoutIP() throws Exception {
    // test without ip
    testSuccessLogFormat(false);
    testFailureLogFormat(false);
  }
  
  /**
   * A special extension of {@link TestImpl} RPC server with 
   * {@link TestImpl#ping()} testing the audit logs.
   */
  private class MyTestRPCServer extends TestImpl {
    @Override
    public void ping() {
      // test with ip set
      testSuccessLogFormat(true);
      testFailureLogFormat(true);
    }
  }
  
  /**
   * Test {@link AuditLogger} with IP set.
   */
  public void testAuditLoggerWithIP() throws Exception {
    Configuration conf = new Configuration();
    // start the IPC server
    Server server = RPC.getServer(new MyTestRPCServer(), "0.0.0.0", 0, conf);
    server.start();
    
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    
    // Make a client connection and test the audit log
    TestProtocol proxy = (TestProtocol)RPC.getProxy(TestProtocol.class, 
                           TestProtocol.versionID, addr, conf);
    // Start the testcase
    proxy.ping();

    server.stop();
  }
}

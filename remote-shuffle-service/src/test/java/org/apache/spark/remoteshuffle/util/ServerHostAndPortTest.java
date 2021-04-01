package org.apache.spark.remoteshuffle.util;

import org.apache.spark.remoteshuffle.exceptions.RssInvalidDataException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServerHostAndPortTest {
  @Test
  public void constructor() {
    ServerHostAndPort hostAndPort = new ServerHostAndPort("server1", 123);
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 123);
    Assert.assertEquals(hostAndPort.toString(), "server1:123");
  }

  @Test
  public void parseString() {
    ServerHostAndPort hostAndPort = new ServerHostAndPort(null);
    Assert.assertEquals(hostAndPort.getHost(), null);
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "null:0");

    hostAndPort = new ServerHostAndPort("");
    Assert.assertEquals(hostAndPort.getHost(), "");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), ":0");

    hostAndPort = new ServerHostAndPort(" ");
    Assert.assertEquals(hostAndPort.getHost(), " ");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), " :0");

    hostAndPort = new ServerHostAndPort("server1");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "server1:0");

    hostAndPort = new ServerHostAndPort("server1:");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "server1:0");

    hostAndPort = new ServerHostAndPort("server1:123");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 123);
    Assert.assertEquals(hostAndPort.toString(), "server1:123");
  }

  @Test(expectedExceptions = RssInvalidDataException.class)
  public void invalidPort() {
    new ServerHostAndPort("server1:a");
  }

  @Test(expectedExceptions = RssInvalidDataException.class)
  public void multiPortValues() {
    new ServerHostAndPort("server1:a:b");
  }
}

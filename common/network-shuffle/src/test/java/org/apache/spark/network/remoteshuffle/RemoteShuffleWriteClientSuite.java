package org.apache.spark.network.remoteshuffle;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleSecuritySuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class RemoteShuffleWriteClientSuite {
  private RemoteShuffleServer server;

  @Before
  public void beforeEach() {
    server = new RemoteShuffleServer(new HashMap<>());
    server.start();
  }

  @After
  public void afterEach() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testConnect() throws IOException, InterruptedException {
    int port = server.getBoundPort();
    long timeoutMs = 30000;
    ShuffleFqid shuffleFqid = new ShuffleFqid("app1", "1", 2);
    try (RemoteShuffleWriteClient client = new RemoteShuffleWriteClient("localhost", port, timeoutMs, shuffleFqid, new HashMap<>())) {
      client.connect();
    }
  }
}

package org.apache.spark.network.shuffle.protocol.mesos;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * A heartbeat sent from the driver to the MesosExternalShuffleService.
 */
public class ShuffleServiceHeartbeat extends BlockTransferMessage {
  private final String appId;

  public ShuffleServiceHeartbeat(String appId) {
    this.appId = appId;
  }

  public String getAppId() { return appId; }

  @Override
  protected Type type() { return Type.HEARTBEAT; }

  @Override
  public int encodedLength() { return Encoders.Strings.encodedLength(appId); }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
  }

  public static ShuffleServiceHeartbeat decode(ByteBuf buf) {
    return new ShuffleServiceHeartbeat(Encoders.Strings.decode(buf));
  }
}

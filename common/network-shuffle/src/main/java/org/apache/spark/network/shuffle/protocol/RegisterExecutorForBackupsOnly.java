package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

public class RegisterExecutorForBackupsOnly extends BlockTransferMessage {

  public final String appId;
  public final String execId;
  public final String shuffleManager;

  public RegisterExecutorForBackupsOnly(
      String appId, String execId, String shuffleManager) {
    this.appId = appId;
    this.execId = execId;
    this.shuffleManager = shuffleManager;
  }

  @Override
  protected Type type() {
    return Type.REGISTER_EXECUTOR_FOR_BACKUPS;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
        + Encoders.Strings.encodedLength(execId)
        + Encoders.Strings.encodedLength(shuffleManager);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    Encoders.Strings.encode(buf, shuffleManager);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RegisterExecutorForBackupsOnly) {
      RegisterExecutorForBackupsOnly o = (RegisterExecutorForBackupsOnly) other;
      return Objects.equal(appId, o.appId)
          && Objects.equal(execId, o.execId)
          && Objects.equal(shuffleManager, o.shuffleManager);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, execId, shuffleManager);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(RegisterExecutorForBackupsOnly.class)
        .add("appId", appId)
        .add("execId", execId)
        .add("shuffleManager", shuffleManager)
        .toString();
  }

  public static RegisterExecutorForBackupsOnly decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    String shuffleManager = Encoders.Strings.decode(buf);
    return new RegisterExecutorForBackupsOnly(appId, execId, shuffleManager);
  }
}

package org.apache.spark.network.shuffle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.network.client.StreamCallbackWithID;

final class FileWriterStreamCallback implements StreamCallbackWithID {

  private static final Logger logger = LoggerFactory.getLogger(FileWriterStreamCallback.class);

  public enum BackupFileType {
    DATA("shuffle-data"),
    INDEX("shuffle-index");

    private final String typeString;

    BackupFileType(String typeString) {
      this.typeString = typeString;
    }

    @Override
    public String toString() {
      return typeString;
    }
  }
  private final ExternalShuffleBlockResolver.AppExecId fullExecId;
  private final int shuffleId;
  private final int mapId;
  private final File file;
  private final BackupFileType fileType;
  private WritableByteChannel fileOutputChannel = null;

  FileWriterStreamCallback(
      ExternalShuffleBlockResolver.AppExecId fullExecId,
      int shuffleId,
      int mapId,
      File file,
      BackupFileType fileType) {
    this.fullExecId = fullExecId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.file = file;
    this.fileType = fileType;
  }

  public void open() {
    logger.info(
        "Opening {} for backup writing. File type: {}", file.getAbsolutePath(), fileType);
    if (fileOutputChannel != null) {
      throw new IllegalStateException(
          String.format(
              "File %s for is already open for writing (type: %s).",
              file.getAbsolutePath(),
              fileType));
    }
    if (!file.exists()) {
      try {
        if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  "Failed to create shuffle file directory at"
                      + file.getParentFile().getAbsolutePath() + "(type: %s).", fileType));
        }

        if (!file.createNewFile()) {
          throw new IOException(
              String.format(
                  "Failed to create shuffle file (type: %s).", fileType));
        }
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Failed to create shuffle file at %s for backup (type: %s).",
                file.getAbsolutePath(),
                fileType),
            e);
      }
    }
    try {
      // TODO encryption
      fileOutputChannel = Channels.newChannel(new FileOutputStream(file));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Failed to find file for writing at %s (type: %s).",
              file.getAbsolutePath(),
              fileType),
          e);
    }
  }

  @Override
  public String getID() {
    return String.format("%s-%s-%d-%d-%s",
        fullExecId.appId,
        fullExecId.execId,
        shuffleId,
        mapId,
        fileType);
  }

  @Override
  public void onData(String streamId, ByteBuffer buf) throws IOException {
    verifyShuffleFileOpenForWriting();
    while (buf.hasRemaining()) {
      fileOutputChannel.write(buf);
    }
  }

  @Override
  public void onComplete(String streamId) throws IOException {
    fileOutputChannel.close();
  }

  @Override
  public void onFailure(String streamId, Throwable cause) throws IOException {
    logger.warn("Failed to back up shuffle file at {} (type: %s).",
        file.getAbsolutePath(),
        fileType,
        cause);
    fileOutputChannel.close();
    // TODO delete parent dirs too
    if (!file.delete()) {
      logger.warn(
          "Failed to delete incomplete backup shuffle file at %s (type: %s)",
          file.getAbsolutePath(),
          fileType);
    }
  }

  private void verifyShuffleFileOpenForWriting() {
    if (fileOutputChannel == null) {
      throw new RuntimeException(
          String.format(
              "Shuffle file at %s not open for writing (type: %s).",
              file.getAbsolutePath(),
              fileType));
    }
  }
}